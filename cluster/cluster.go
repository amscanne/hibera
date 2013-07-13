package cluster

import (
    "bytes"
    "encoding/json"
    "hibera/client"
    "hibera/core"
    "hibera/storage"
    "hibera/utils"
    "net"
    "sync"
    "time"
)

var DefaultKeys = uint(128)
var DefaultN = uint(1)
var RootKey = core.Key("")

type Cluster struct {
    // Our built-in auth token.
    auth string

    // The cluster revision.
    // When the cluster has not yet been activated (i.e.
    // we've just started and not yet seen any active nodes
    // or participated in a paxos round) then this number
    // will be zero.
    rev core.Revision

    // Our replication factor.
    N int

    // Our local datastore.
    // Some of these functions are exported and used directly
    // by the calling servers (HTTPServer).
    // NOTE: Not exported so that it is not serialized.
    *core.Data

    // Our node map.
    // This represents the consensus of what the cluster looks
    // like and is computed by the changing versions above.
    // Similarly, some functions are exported and used directly
    // by the calling servers (GossipServer).
    *core.Nodes

    // Our token list.
    // This represents the available access tokens.
    *core.Access

    // Our ring.
    // This routes requests, etc.
    // This is computed each round based on the node map above.
    // NOTE: Not exported so that it is not serialized.
    *ring

    // Cache of connections to other servers.
    clients map[string]*client.HiberaAPI

    sync.Mutex
}

func (c *Cluster) doEncode(next bool) ([]byte, error) {
    info := core.NewInfo()

    // Encode our nodes.
    nodes_changed, err := c.Nodes.Encode(c.rev, next, info.Nodes, c.N)
    if err != nil {
        utils.Print("CLUSTER", "ENCODING-NODES-ERROR %s", err)
        return nil, err
    }

    // Encode our tokens.
    access_changed, err := c.Access.Encode(next, info.Access)
    if err != nil {
        utils.Print("CLUSTER", "ENCODING-ACCESS-ERROR %s", err)
        return nil, err
    }

    // Check if we're encoding anything interesting.
    if next && !nodes_changed && !access_changed {
        utils.Print("CLUSTER", "NOTHING-DOING")
        return nil, nil
    }

    // Encode our info object as JSON.
    buf := new(bytes.Buffer)
    enc := json.NewEncoder(buf)
    err = enc.Encode(&info)

    return buf.Bytes(), err
}

func (c *Cluster) doDecode(data []byte) (bool, error) {
    info := core.NewInfo()

    // Decode our nodes and tokens.
    buf := bytes.NewBuffer(data)
    dec := json.NewDecoder(buf)
    err := dec.Decode(&info)
    if err != nil {
        return false, err
    }

    // Do the decode from nodes.
    force, err := c.Nodes.Decode(info.Nodes)
    if err != nil {
        return force, err
    }

    // Do the token decode.
    err = c.Access.Decode(info.Access)
    return force, err
}

func (c *Cluster) Active() bool {
    return c.rev != core.Revision(0)
}

func (c *Cluster) doActivate() error {
    // If we're already activated, ignore.
    if c.Active() {
        utils.Print("CLUSTER", "ALREADY-ACTIVATED")
        return nil
    }

    // Activate our node.
    c.Nodes.Reset()
    if !c.Nodes.Activate(c.Nodes.Self().Id(), core.Revision(1)) {
        utils.Print("CLUSTER", "ACTIVATE-ERROR")
        return nil
    }

    // Do the data set.
    bytes, err := c.doEncode(false)
    if bytes == nil || err != nil {
        return err
    }
    rev, err := c.Data.DataSet(RootKey, core.Revision(1), bytes)
    if err != nil || rev != core.Revision(1) {
        return err
    }

    // Save the revision.
    c.changeRevision(core.Revision(1), true)
    return nil
}

func (c *Cluster) doDeactivate() error {
    // Reset our revision (hard).
    c.rev = core.Revision(0)
    c.Data.DataRemove(RootKey, 0)

    // Reset the nodes state.
    c.Nodes.Reset()
    c.ring = NewRing(c.Nodes)

    // Reset authentication tokens.
    c.Access = core.NewAccess(c.auth)

    return nil
}

func (c *Cluster) doSync(addr *net.UDPAddr, from core.Revision) {
    // Pull the remote info from the node.
    cl := c.getClient(utils.AsURL(addr))
    data, rev, err := cl.Info(uint64(from))
    if err != nil {
        utils.Print("CLUSTER", "SYNC-CLIENT-ERROR %s", err.Error())
        return
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Update our nodemap.
    force, err := c.doDecode(data)
    if err != nil {
        utils.Print("CLUSTER", "SYNC-DATA-ERROR %s", err.Error())
    }

    // Update our revision.
    c.changeRevision(core.Revision(rev), force)
    utils.Print("CLUSTER", "SYNC-SUCCESS")
}

func (c *Cluster) GossipUpdate(addr *net.UDPAddr, id string, rev core.Revision, dead []string) {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Mark dead nodes as dead.
    for _, id := range dead {
        c.Nodes.NoHeartbeat(id)
    }

    // Update our nodes.
    if !c.Nodes.Heartbeat(id, rev) || rev > c.rev {
        // We don't know about this node.
        // Refresh addr will go get info
        // from this node by address.
        if rev > c.rev {
            go c.doSync(addr, c.rev)
        } else {
            go c.doSync(addr, 0)
        }
    }
}

func (c *Cluster) syncData(ring *ring, key core.Key, quorum bool) {
    // Pull in the quorum value (from the old ring).
    value, rev, err := c.quorumGet(ring, key)
    if err != nil {
        // It's possible that the quorumGet failed because we were in the middle
        // of some other quorumSet(). That's fine, whatever other quroumSet() will
        // complete and ultimately represent the correct quorumValue.
        utils.Print("CLUSTER", "SYNC-GET-ERROR key=%s rev=%d %s", string(key), uint64(rev), err.Error())
        return
    }

    // Set the full cluster.
    rev, err = c.quorumSet(ring, key, rev, value)
    if err != nil {
        // It's possible that the cluster has changed again by the time this
        // set has actually had a chance to run. That's okay, at some point
        // the most recent quorumSet will work (setting the most recent ring).
        utils.Print("CLUSTER", "SYNC-QUORUM-ERROR key=%s rev=%d %s", string(key), uint64(rev), err.Error())
        return
    }

    utils.Print("CLUSTER", "SYNC-OKAY key=%s rev=%d", string(key), uint64(rev))
}

func (c *Cluster) changeRevision(rev core.Revision, force bool) {
    // We only update revisions unless force is set.
    // Force will be set when we are joining some other
    // cluster (with a potentially conflicting history)
    // so we may end up with a lower revision.
    if rev <= c.rev && !force {
        return
    }

    // Do a synchronize of all of our keys across this new
    // revision. This will happen before we bump our version
    // in order to ensure that everyone we send to other hosts
    // has the correct version.
    old_ring := c.ring
    c.ring = NewRing(c.Nodes)
    items, err := c.Data.DataList()
    if err != nil {
        utils.Print("CLUSTER", "REVISION-DATA-ERROR rev=%d force=%t", uint64(rev), force)
        return
    }

    // Schedule updates for every key.
    for _, key := range items {

        old_master := old_ring.IsMaster(key)
        new_master := c.ring.IsMaster(key)
        new_slave := c.ring.IsSlave(key, 2 * c.N + 1)

        if !old_master && new_master {
            // We're the new master for this key.
            // We unconditionally do a sync for this key
            // in order to ensure that all the slaves have
            // the correct data in place.
            if key != RootKey {
                go c.syncData(c.ring, key, true)
            }

        } else if c.ring.IsFailover(key, c.N + 1) &&
            !old_ring.IsNode(key, c.ring.MasterFor(key), 2 * c.N + 1) {
            // Ensure that the master is aware of this key.
            // This happens if the new master was not in the
            // ring previously, but is now in the ring.
            if key != RootKey {
                go c.syncData(c.ring, key, true)
            }
        }

        // We don't have anything to do with this key.
        // Scrub it from our local datastore.
        // NOTE: This is done unconditionally because
        // for this ring, we aren't expected to have any
        // data related to this key. If we change rings 
        // again, the new data will be populated.
        if !new_slave && !new_master {
            c.Data.DataRemove(key, 0)
        }

        // We aren't the master for this key.
        // We ensure that all the synchronization
        // groups have fired events (and they will
        // be appropriately redirected to the true
        // master).
        if !new_master {
            c.Data.EventFire(key, 0)
        }
    }

    // Update the revision.
    c.rev = rev
    c.Nodes.Heartbeat(c.Nodes.Self().Id(), c.rev)
}

func (c *Cluster) healthcheck() {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // If we're not active skip it.
    if !c.Active() {
        return
    }

    for {
        // If we have unknowns, let's propose a change.
        target_rev := c.rev + 1
        master := c.ring.MasterFor(RootKey)
        is_master := (master == nil || master == c.Nodes.Self())
        is_failover := (master != nil && !master.Alive() && c.ring.IsFailover(RootKey, 2 * c.N + 1))

        if is_master {
            utils.Print("CLUSTER", "HEALTHCHECK-MASTER")
        } else if is_failover {
            utils.Print("CLUSTER", "HEALTHCHECK-FAILOVER")
        }

        if is_master || is_failover {
            // Encode a cluster delta.
            bytes, err := c.doEncode(true)

            // Check if everything is okay.
            if bytes == nil || err != nil {
                break
            }

            // Try to do the data set.
            target_rev, err = c.lockedClusterDataSet(bytes, target_rev)
            if err == nil {
                // Updated successfully.
                break
            }

        } else {
            // We're no longer the master.
            break
        }
    }
}

func (c *Cluster) lockedClusterDataSet(bytes []byte, target_rev core.Revision) (core.Revision, error) {
    // Update the node configuration and bump our cluster
    // version number. Note that we must first successfully
    // contact a quorum of nodes to ensure that we're in the
    // majority still.
    utils.Print("CLUSTER", "SET-CLUSTER")
    rev, err := c.quorumSet(c.ring, RootKey, target_rev, bytes)
    if err != nil {
        c.Mutex.Lock()
        return rev, err
    }

    force, err := c.doDecode(bytes)
    if err != nil {
        utils.Print("CLUSTER", "DECODE-ERROR %s", err.Error())
        return c.rev, err
    }
    c.changeRevision(rev, force)
    return c.rev, nil
}

func (c *Cluster) timedHealthcheck() {
    c.healthcheck()

    // Restart the healthcheck.
    f := func() { c.timedHealthcheck() }
    time.AfterFunc(time.Duration(1)*time.Second, f)
}

func NewCluster(N uint, backend *storage.Backend, addr string, auth string, domain string, ids []string) *Cluster {
    c := new(Cluster)
    c.N = int(N)
    c.auth = auth
    c.Nodes = core.NewNodes(addr, ids, domain)
    c.Access = core.NewAccess(auth)
    c.Data = core.NewData(backend)
    c.ring = NewRing(c.Nodes)
    c.clients = make(map[string]*client.HiberaAPI)

    // Read cluster data.
    data, rev, err := c.Data.DataGet(RootKey)
    if err == nil && rev > 0 {
        utils.Print("CLUSTER", "START-FOUND rev=%d", uint64(rev))

        // Load the existing cluster data.
        force, err := c.doDecode(data)
        if err == nil {
            utils.Print("CLUSTER", "START-DECODED rev=%d", uint64(rev))
            c.changeRevision(rev, force)
        } else {
            c.Data.DataRemove(RootKey, 0)
        }
    }

    // Start running our healthcheck.
    go c.timedHealthcheck()

    return c
}
