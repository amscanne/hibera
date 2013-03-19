package core

import (
    "net"
    "time"
    "sync"
    "os"
    "syscall"
    "os/signal"
    "hibera/client"
    "hibera/storage"
    "hibera/utils"
)

type Key string
type Revision uint64

var DefaultKeys = uint(128)
var HiberaKey = Key("hibera")

type Cluster struct {
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

    // Our ring.
    // This routes requests, etc.
    // This is computed each round based on the node map above.
    // NOTE: Not exported so that it is not serialized.
    *ring

    sync.Mutex
}

func (c *Cluster) Activate(rev Revision) {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Activate our node.
    if !c.Nodes.Activate(c.Id(), rev) {
        // No information has been fetched on this node before.
        // We can't activate until refreshAddr() has completed.
        utils.Print("CLUSTER", "ACTIVATE-ERROR rev=%d", rev)
        return
    }

    // Write the cluster data.
    bytes, err := c.Nodes.Encode(0, false)
    rev, err = c.data.DataSet(HiberaKey, rev, bytes)
    if err != nil {
        utils.Print("CLUSTER", "WRITE-ERROR %s", err.Error())
        return
    }

    utils.Print("CLUSTER", "ACTIVATE-OKAY rev=%d", rev)
    c.changeRevision(rev, true)
}

func (c *Cluster) doSync(id string, addr *net.UDPAddr, from Revision) {
    // Pull the remote info from the node.
    cl := client.NewHiberaAPI(utils.AsURLs(addr), c.Id(), 0)
    data, rev, err := cl.Info(uint64(from))
    if err != nil {
        utils.Print("CLUSTER", "SYNC-CLIENT-ERROR id=%s %s",
            id, err.Error())
        return
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Update our nodemap.
    force, err := c.Nodes.Decode(data)
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
        delta, err := c.Nodes.Encode(c.rev+1, true)

        // Check if everything is okay.
        if delta == nil || err != nil {
            c.Mutex.Unlock()
            return
        }

        // Encode a full cluster dump.
        bytes, err := c.Nodes.Encode(0, false)

        orig_rev := c.rev
        target_rev := c.rev + 1
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
                if rev > 0 {
                    target_rev = rev + 1
                    continue
                } else {
                    utils.Print("CLUSTER", "WRITE-ERROR %s", err.Error())
                    return
                }
            }

            c.Mutex.Lock()
            if c.rev != orig_rev {
                c.Mutex.Unlock()
                return
            }
            force, err := c.Nodes.Decode(delta)
            if err != nil {
                utils.Print("CLUSTER", "DECODE-ERROR %s", err.Error())
                return
            }
            c.changeRevision(target_rev, force)
            break
        }

    } else {
        // Nothing to be done anyways, wait for either
        // the master or the failover master to adjust.
    }

    c.Mutex.Unlock()
}

func (c *Cluster) timedHealthcheck() {
    c.Healthcheck()

    // Restart the healthcheck.
    f := func() { c.timedHealthcheck() }
    time.AfterFunc(time.Duration(1)*time.Second, f)
}

func NewCluster(backend *storage.Backend, domain string, ids []string) *Cluster {
    c := new(Cluster)
    c.Nodes = NewNodes(ids, domains(domain))
    c.data = NewData(backend)
    c.ring = NewRing(c.Nodes)
    c.Hub = NewHub(c)

    activated := false
    data, rev, err := c.data.DataGet(HiberaKey)
    if err != nil {
        // Load the existing cluster data.
        force, err := c.Nodes.Decode(data)
        if err != nil {
            activated = true
            c.changeRevision(rev, force)
        }
    }
    if !activated {
        // Activate a cluster of one.
        c.Activate(Revision(0))
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
