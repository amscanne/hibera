package core

import (
    "net"
    "time"
    "sync"
    "os"
    "syscall"
    "bytes"
    "os/signal"
    "encoding/json"
    "hibera/client"
    "hibera/storage"
    "hibera/utils"
)

type Key string
type Revision uint64

var DefaultKeys = uint(128)
var HiberaKey = Key("hibera")
var TokensKey = Key("tokens")

type Cluster struct {
    // Our cluster id.
    id string

    // Our authorization key.
    auth string

    // Our connection hub.
    *Hub

    // The cluster revision.
    // When the cluster has not yet been activated (i.e.
    // we've just started and not yet seen any active nodes
    // or participated in a paxos round) then this number
    // will be zero.
    rev Revision

    // Our local datastore.
    // Some of these functions are exported and used directly
    // by the calling servers (HTTPServer).
    // NOTE: Not exported so that it is not serialized.
    *data

    // Our node map.
    // This represents the consensus of what the cluster looks
    // like and is computed by the changing versions above.
    // Similarly, some functions are exported and used directly
    // by the calling servers (GossipServer).
    *Nodes

    // Our token list.
    // This represents the available access tokens.
    *Tokens

    // Our ring.
    // This routes requests, etc.
    // This is computed each round based on the node map above.
    // NOTE: Not exported so that it is not serialized.
    *ring

    sync.Mutex
}

type Info struct {
    Nodes map[string]*Node
    Tokens map[string]*Token
}

func (c *Cluster) doEncode(rev Revision, next bool) ([]byte, error) {
    info := new(Info)
    info.Nodes = make(map[string]*Node)
    info.Tokens = make(map[string]*Token)

    // Encode our nodes.
    err := c.Nodes.Encode(rev, next, info.Nodes)
    if err != nil {
        return nil, err
    }

    // Encode our tokens.
    err = c.Tokens.Encode(rev, info.Tokens)
    if err != nil {
        return nil, err
    }

    // Check if we're encoding anything interesting.
    if len(info.Nodes) == 0 && len(info.Tokens) == 0 {
        return nil, nil
    }

    // Encode our info object as JSON.
    buf := new(bytes.Buffer)
    enc := json.NewEncoder(buf)
    err = enc.Encode(&info)

    return buf.Bytes(), err
}

func (c *Cluster) doDecode(data []byte) (bool, error) {
    info := new(Info)
    info.Nodes = make(map[string]*Node)
    info.Tokens = make(map[string]*Token)

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
    err = c.Tokens.Decode(info.Tokens)
    return force, err
}

func (c *Cluster) doActivate() error {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // If we're already activated, ignore.
    if c.id != "" {
        return nil
    }

    // Activate our node.
    if !c.Nodes.Activate(c.Nodes.Self().Id(), c.rev) {
        utils.Print("CLUSTER", "ACTIVATE-ERROR")
        return nil
    }

    // Generate and save the cluster id.
    uuid, err := utils.Uuid()
    if err != nil {
        return err
    }
    c.id = uuid
    c.data.saveClusterId(c.id)

    // Write the cluster data.
    bytes, err := c.doEncode(0, false)
    c.rev, err = c.data.DataSet(HiberaKey, c.rev, bytes)
    if err != nil {
        utils.Print("CLUSTER", "WRITE-ERROR %s", err.Error())
        return err
    }

    utils.Print("CLUSTER", "ACTIVATE-OKAY rev=%d", c.rev)
    c.changeRevision(c.rev, true)
    return nil
}

func (c *Cluster) doDeactivate() error {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Reset our id.
    c.id = ""
    c.data.saveClusterId(c.id)

    // Reset our revision (hard).
    c.rev = Revision(0)

    // Reset the nodes state.
    c.Nodes.Reset()

    return nil
}

func (c *Cluster) doSync(id string, addr *net.UDPAddr, from Revision) {
    // Pull the remote info from the node.
    cl := client.NewHiberaAPI(utils.AsURLs(addr), c.auth, c.Nodes.Self().Id(), 0)
    id, data, rev, err := cl.Info(uint64(from))
    if err != nil {
        utils.Print("CLUSTER", "SYNC-CLIENT-ERROR id=%s %s",
            id, err.Error())
        return
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Take on the identity if we have none.
    if c.id == "" {
        c.id = id
    }

    // Check that the sync is legit.
    if c.id != id {
        return
    }

    // Update our nodemap.
    force, err := c.doDecode(data)
    c.Nodes.Update(id, addr)
    if err != nil {
        utils.Print("CLUSTER", "SYNC-DATA-ERROR id=%s %s",
            id, err.Error())
    }

    // Update our revision.
    c.changeRevision(Revision(rev), force)
    utils.Print("CLUSTER", "SYNC-SUCCESS id=%s", id)
}

func (c *Cluster) Heartbeat(id string, addr *net.UDPAddr, rev Revision, nodes uint64, dead []string) {
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
            go c.doSync(id, addr, c.rev)
        } else {
            go c.doSync(id, addr, 0)
        }
    }
}

func (c *Cluster) syncData(ring *ring, key Key, quorum bool) {
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
        rev, err = c.data.DataSet(key, rev, value)
        if err != nil {
            // Same as per above.
            utils.Print("CLUSTER", "SYNC-LOCAL-ERROR key=%s rev=%d %s", string(key), uint64(rev), err.Error())
            return
        }
    }

    utils.Print("CLUSTER", "SYNC-OKAY key=%s rev=%d", string(key), uint64(rev))
}

func (c *Cluster) dumpCluster() {
    // Output all nodes.
    utils.Print("CLUSTER", "CLUSTER (rev=%d,master=%t,failover=%t)",
        c.rev,
        c.ring.IsMaster(HiberaKey),
        c.ring.IsFailover(HiberaKey))
    if c.ring.MasterFor(HiberaKey) != nil {
        utils.Print("CLUSTER", "MASTER %s", c.ring.MasterFor(HiberaKey).Id())
    }
    c.Nodes.dumpNodes()
}

func (c *Cluster) changeRevision(rev Revision, force bool) {
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
    items, err := c.data.DataList()
    if err != nil {
        utils.Print("CLUSTER", "REVISION-DATA-ERROR rev=%d force=%t", uint64(rev), force)
        return
    }

    // Update the revision.
    c.rev = rev
    c.dumpCluster()

    // Schedule updates for every key.
    for _, key := range items {

        old_master := old_ring.IsMaster(key)
        new_master := c.ring.IsMaster(key)
        new_slave := c.ring.IsSlave(key)

        // We're the new master for this key.
        if !old_master && new_master {
            if key != HiberaKey {
                go c.syncData(c.ring, key, true)
            }

        // Ensure that the master is aware of this key.
        // This happens if the new master was not in the
        // ring previously, but is now in the ring.
        } else if c.ring.IsFailover(key) &&
            !old_ring.IsNode(key, c.ring.MasterFor(key)) {
            if key != HiberaKey {
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
            c.data.DataRemove(key, 0)
        }

        // We aren't the master for this key.
        if !new_master {
            c.data.EventFire(key, 0)
        }
    }
}

func (c *Cluster) Healthcheck() {
    c.Mutex.Lock()

    // If we have unknowns, let's propose a change.
    master := c.ring.MasterFor(HiberaKey)
    is_master := (master == nil || master == c.Nodes.Self())
    is_failover := (master != nil && !master.Alive() && c.ring.IsFailover(HiberaKey))

    if is_master {
        utils.Print("CLUSTER", "HEALTHCHECK-MASTER")
    } else if is_failover {
        utils.Print("CLUSTER", "HEALTHCHECK-FAILOVER")
    } else {
        c.Mutex.Unlock()
        return
    }

    if is_master || is_failover {
        // Encode a cluster delta.
        delta, err := c.doEncode(c.rev+1, true)

        // Check if everything is okay.
        if delta == nil || err != nil {
            c.Mutex.Unlock()
            return
        }

        // Try to do the data set.
        c.lockedClusterDataSet(0, delta)

    } else {
        // Nothing to be done anyways, wait for either
        // the master or the failover master to adjust.
        c.Mutex.Unlock()
    }
}

func (c *Cluster) lockedClusterDataSet(set_rev Revision, delta []byte) (Revision, error) {
    // Encode a full cluster dump.
    bytes, err := c.doEncode(0, false)
    if err != nil {
        return c.rev, err
    }

    orig_rev := c.rev
    var target_rev Revision
    if set_rev == 0 {
        target_rev = c.rev + 1
    } else {
        target_rev = set_rev
    }
    ring := c.ring
    c.Mutex.Unlock()

    for {
        // Update the node configuration and bump our cluster
        // version number. Note that we must first successfully
        // contact a quorum of nodes to ensure that we're in the
        // majority still.
        utils.Print("CLUSTER", "SET-CLUSTER %d", uint64(target_rev))
        rev, err := c.quorumSet(ring, HiberaKey, target_rev, bytes)
        if err != nil {
            if rev > 0 && (set_rev == 0) {
                target_rev = rev + 1
                continue
            } else {
                utils.Print("CLUSTER", "WRITE-ERROR %s", err.Error())
                return rev, err
            }
        }

        c.Mutex.Lock()
        if c.rev != orig_rev {
            defer c.Mutex.Unlock()
            return c.rev, nil
        }
        force, err := c.doDecode(delta)
        if err != nil {
            utils.Print("CLUSTER", "DECODE-ERROR %s", err.Error())
            defer c.Mutex.Unlock()
            return c.rev, err
        }
        c.changeRevision(target_rev, force)
        break
    }

    defer c.Mutex.Unlock()
    return c.rev, nil
}

func (c *Cluster) timedHealthcheck() {
    c.Healthcheck()

    // Restart the healthcheck.
    f := func() { c.timedHealthcheck() }
    time.AfterFunc(time.Duration(1)*time.Second, f)
}

func NewCluster(backend *storage.Backend, auth string, domain string, ids []string) *Cluster {
    c := new(Cluster)
    c.auth = auth
    c.Nodes = NewNodes(ids, domains(domain))
    c.Tokens = NewTokens()
    c.data = NewData(backend)
    c.ring = NewRing(c.Nodes)
    c.Hub = NewHub(c)

    // Load a cluster id.
    var err error
    c.id, err = c.data.loadClusterId()
    if err != nil {
        // Crap.
        utils.Print("CLUSTER", "ID-LOAD-ERROR %s", err.Error())
        return nil
    }

    // Read cluster data.
    data, rev, err := c.data.DataGet(HiberaKey)
    if err != nil {
        // Load the existing cluster data.
        force, err := c.doDecode(data)
        if err != nil {
            c.changeRevision(rev, force)
        }
    }

    // Start running our healthcheck.
    go c.timedHealthcheck()

    // Setup a debug handler.
    sigchan := make(chan os.Signal)
    handler := func() {
        for {
            sig := <-sigchan
            if sig == syscall.SIGUSR1 {
                c.dumpCluster()
                c.data.dumpData()
                c.Hub.dumpHub()
                items, err := c.data.DataList()
                if err == nil {
                    c.ring.dumpRing(items)
                }
            }
        }
    }
    signal.Notify(sigchan, syscall.SIGUSR1)
    go handler()

    return c
}
