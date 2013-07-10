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
var RootKey = core.Key("")

type Cluster struct {
    // Our cluster id.
    id  string

    // Our built-in auth token.
    auth string

    // The cluster revision.
    // When the cluster has not yet been activated (i.e.
    // we've just started and not yet seen any active nodes
    // or participated in a paxos round) then this number
    // will be zero.
    rev core.Revision

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

    sync.Mutex
}

func (c *Cluster) doEncode(rev core.Revision, next bool) ([]byte, error) {
    info := core.NewInfo()

    // Encode our nodes.
    nodes_changed, err := c.Nodes.Encode(rev, next, info.Nodes)
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
    return c.id != "" && c.rev != core.Revision(0)
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

    // Generate and save the cluster id.
    uuid, err := utils.Uuid()
    if err != nil {
        utils.Print("CLUSTER", "ID-ERROR")
        return err
    }
    c.id = uuid
    c.Data.SaveClusterId(c.id)
    c.changeRevision(core.Revision(1), true)
    utils.Print("CLUSTER", "ACTIVATED")

    // Do the quorum set.
    bytes, err := c.doEncode(core.Revision(0), false)
    if bytes == nil || err != nil {
        return err
    }
    rev, err := c.lockedClusterDataSet(bytes, c.rev)
    if err != nil {
        utils.Print("CLUSTER", "WRITE-ERROR %s", err.Error())
        return err
    }

    utils.Print("CLUSTER", "ACTIVATE-OKAY rev=%d", c.rev)
    c.changeRevision(rev, true)

    return nil
}

func (c *Cluster) doDeactivate() error {
    // Reset our id.
    c.id = ""
    c.Data.SaveClusterId(c.id)

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
    cl := client.NewHiberaAPI(utils.AsURLs(addr), c.auth, c.Id(), 0)
    id, data, rev, err := cl.Info(uint64(from))
    if err != nil {
        utils.Print("CLUSTER", "SYNC-CLIENT-ERROR id=%s %s", id, err.Error())
        return
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Take on the identity if we have none.
    if c.id == "" && rev > 0 {
        c.id = id
        c.Access.Reset()
        c.Data.SaveClusterId(c.id)
        utils.Print("CLUSTER", "SYNC-ACTIVATE id=%s", id)
    }

    // Check that the sync is legit.
    if c.id != id {
        return
    }

    // Update our nodemap.
    force, err := c.doDecode(data)
    if err != nil {
        utils.Print("CLUSTER", "SYNC-DATA-ERROR id=%s %s", id, err.Error())
    }

    // Update our revision.
    c.changeRevision(core.Revision(rev), force)
    utils.Print("CLUSTER", "SYNC-SUCCESS id=%s", id)
}

func (c *Cluster) Heartbeat(id string, addr *net.UDPAddr, rev core.Revision, nodes uint64, dead []string) {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Mark dead nodes as dead.
    for _, id := range dead {
        c.Nodes.NoHeartbeat(id)
    }

    // Update our nodes.
    if !c.Nodes.Heartbeat(id) ||
        (rev > c.rev || (rev == c.rev && nodes > uint64(c.Count()))) {
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

    if quorum {
        // Set the full cluster.
        rev, err = c.quorumSet(ring, key, rev, value)
        if err != nil {
            // It's possible that the cluster has changed again by the time this
            // set has actually had a chance to run. That's okay, at some point
            // the most recent quorumSet will work (setting the most recent ring).
            utils.Print("CLUSTER", "SYNC-QUORUM-ERROR key=%s rev=%d %s", string(key), uint64(rev), err.Error())
            return
        }
    } else {
        // Set the local data.
        rev, err = c.Data.DataSet(key, rev, value)
        if err != nil {
            // Same as per above.
            utils.Print("CLUSTER", "SYNC-LOCAL-ERROR key=%s rev=%d %s", string(key), uint64(rev), err.Error())
            return
        }
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

    // Update the revision.
    c.rev = rev

    // Schedule updates for every key.
    for _, key := range items {

        old_master := old_ring.IsMaster(key)
        new_master := c.ring.IsMaster(key)
        new_slave := c.ring.IsSlave(key)

        // We're the new master for this key.
        if !old_master && new_master {
            if key != RootKey {
                go c.syncData(c.ring, key, true)
            }

            // Ensure that the master is aware of this key.
            // This happens if the new master was not in the
            // ring previously, but is now in the ring.
        } else if c.ring.IsFailover(key) &&
            !old_ring.IsNode(key, c.ring.MasterFor(key)) {
            if key != RootKey {
                go c.syncData(c.ring, key, true)
            }

            // We're a slave for this key, make sure we're
            // synchronized (this may generate some extra
            // traffic, but whatever).
        } else if new_slave {
            go c.syncData(c.ring, key, false)
        }

        // We don't have anything to do with this key.
        if !new_slave && !new_master {
            c.Data.DataRemove(key, 0)
        }

        // We aren't the master for this key.
        if !new_master {
            c.Data.EventFire(key, 0)
        }
    }
}

func (c *Cluster) Healthcheck() {
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
        is_failover := (master != nil && !master.Alive() && c.ring.IsFailover(RootKey))

        if is_master {
            utils.Print("CLUSTER", "HEALTHCHECK-MASTER")
        } else if is_failover {
            utils.Print("CLUSTER", "HEALTHCHECK-FAILOVER")
        }

        if is_master || is_failover {
            // Encode a cluster delta.
            bytes, err := c.doEncode(c.rev+1, true)

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
    orig_rev := c.rev
    ring := c.ring
    c.Mutex.Unlock()

    // Update the node configuration and bump our cluster
    // version number. Note that we must first successfully
    // contact a quorum of nodes to ensure that we're in the
    // majority still.
    utils.Print("CLUSTER", "SET-CLUSTER")
    rev, err := c.quorumSet(ring, RootKey, target_rev, bytes)
    if err != nil {
        c.Mutex.Lock()
        return rev, err
    }

    c.Mutex.Lock()
    if c.rev != orig_rev {
        return c.rev, nil
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
    c.Healthcheck()

    // Restart the healthcheck.
    f := func() { c.timedHealthcheck() }
    time.AfterFunc(time.Duration(1)*time.Second, f)
}

func NewCluster(backend *storage.Backend, addr string, auth string, domain string, ids []string) *Cluster {
    c := new(Cluster)
    c.Nodes = core.NewNodes(addr, ids, domains(domain))
    c.Access = core.NewAccess(auth)
    c.Data = core.NewData(backend)
    c.ring = NewRing(c.Nodes)
    c.auth = auth

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

    // Load a cluster id (only if active).
    if c.rev != 0 {
        utils.Print("CLUSTER", "LOADING-ID...")
        c.id, err = c.Data.LoadClusterId()
        if err != nil {
            // Crap.
            utils.Print("CLUSTER", "ID-LOAD-ERROR %s", err.Error())
            return nil
        }
    }

    // Start running our healthcheck.
    go c.timedHealthcheck()

    return c
}
