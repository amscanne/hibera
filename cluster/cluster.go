package cluster

import (
    "bytes"
    "encoding/json"
    "github.com/amscanne/hibera/core"
    "github.com/amscanne/hibera/storage"
    "github.com/amscanne/hibera/utils"
    "net"
    "sync"
    "time"
)

var RootNamespace = core.Namespace("")
var RootKey = core.Key("")

type Cluster struct {
    // Our built-in auth token.
    root core.Token

    // Whether we are forcing an update.
    force bool

    // Our replication factor.
    N     uint
    N_new uint
    N_mod core.Revision

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

    // Whether or not we are executing a quorum function on the given key.
    quorumInProgress map[core.Key]bool
    quorumWaiters    map[core.Key]uint
    quorumLock       *sync.Cond

    // Whether or not a synchronization is currently active.
    // This is guarded by syncLock, which is also held during
    // data synchronization (i.e. changing to a new revision).
    syncActive bool
    syncLock   sync.Mutex
    syncLimit  uint

    // The core lock (protecting revision, etc.)
    sync.Mutex
}

func (c *Cluster) lockedEncode(next bool, rev core.Revision) ([]byte, error) {
    info := core.NewInfo()

    if next && c.N != c.N_new {
        // Encode the new replication factor.
        info.N = c.N_new
        info.N_mod = rev
    } else {
        // Ensure our current replication factor.
        info.N = c.N
        info.N_mod = c.N_mod
    }

    // Encode our nodes.
    nodes_changed, err := c.Nodes.Encode(next, info.Nodes, info.N)
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
    if next && !c.force && (info.N == c.N) && !nodes_changed && !access_changed {
        utils.Print("CLUSTER", "NOTHING-DOING")
        return nil, nil
    }

    // Encode our info object as JSON.
    buf := new(bytes.Buffer)
    enc := json.NewEncoder(buf)
    err = enc.Encode(&info)

    return buf.Bytes(), err
}

func (c *Cluster) lockedDecodeInfo(info *core.Info) (bool, error) {

    // Do the decode from nodes.
    force, err := c.Nodes.Decode(info.Nodes)
    if err != nil {
        return force, err
    }

    // Do the token decode.
    // Note: there is no turning back at this point.
    // We've already successfully decoded node updates,
    // so even if we have errors decoding the access
    // updates we will continue to press on.
    err = c.Access.Decode(info.Access)

    // Reset our N value.
    if info.N_mod.GreaterThan(c.N_mod) {
        c.N = info.N
        c.N_new = info.N
        c.N_mod = info.N_mod
        force = true
    }

    return force, err
}

func (c *Cluster) lockedDecodeBytes(data []byte) (bool, error) {
    info := core.NewInfo()

    // Decode our nodes and tokens.
    buf := bytes.NewBuffer(data)
    dec := json.NewDecoder(buf)
    err := dec.Decode(&info)
    if err != nil {
        return false, err
    }

    return c.lockedDecodeInfo(info)
}

func (c *Cluster) Revision() core.Revision {
    return c.Nodes.Self().Current
}

func (c *Cluster) Active() bool {
    return !c.Nodes.Self().Current.IsZero()
}

func (c *Cluster) URL() string {
    return c.Nodes.Self().URL
}

func (c *Cluster) Id() string {
    return c.Nodes.Self().Id()
}

func (c *Cluster) doActivate(N uint) (core.Revision, error) {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Always force an encode.
    c.force = true

    // Always take this as the proposed replication.
    c.N_new = N

    // If we're already activated, we simply
    // take on the latest replication factor.
    if c.Active() {
        return c.Revision(), nil
    }

    // Activate our node.
    rev_one := core.NoRevision.Next()
    utils.Print("CLUSTER", "ACTIVATE WITH rev=%s", rev_one.String())

    c.Nodes.Reset()
    err := c.Nodes.Activate(c.Id(), rev_one)
    if err != nil {
        utils.Print("CLUSTER", "NODE-ACTIVATE-ERROR %s", err.Error())
        return c.Revision(), err
    }

    // Do an encoding of this cluster.
    data, err := c.lockedEncode(false, rev_one)
    if err != nil {
        c.Nodes.Reset()
        utils.Print("CLUSTER", "ENCODE-ACTIVATE-ERROR %s", err.Error())
        return c.Revision(), err
    }

    // Write out the new data.
    _, err = c.Data.DataSet(RootNamespace, RootKey, rev_one, data)
    if err != nil {
        c.Nodes.Reset()
        utils.Print("CLUSTER", "DATA-SET-ERROR %s", err.Error())
        return core.NoRevision, err
    }

    // Change our revision (manually).
    return c.lockedChangeRevision(rev_one, false)
}

func (c *Cluster) lockedDeactivate() error {
    // Reset our revision (hard).
    c.Nodes.Self().Current = core.NoRevision
    c.Data.DataRemove(RootNamespace, RootKey, core.NoRevision)

    // Reset the nodes state.
    c.Nodes.Reset()
    c.ring.Recompute()

    // Reset authentication tokens.
    c.Access = core.NewAccess(c.root)

    return nil
}

func (c *Cluster) startSync() bool {
    c.syncLock.Lock()
    defer c.syncLock.Unlock()
    if c.syncActive {
        return false
    }
    c.syncActive = true
    return true
}

func (c *Cluster) doneSync() {
    c.syncLock.Lock()
    defer c.syncLock.Unlock()
    c.syncActive = false
}

func (c *Cluster) doSync(url string) error {
    // Only do one sync at a time.
    if !c.startSync() {
        return nil
    }

    // Pull the remote info from the node.
    cl := c.getClient(url)
    defer cl.Close()

    info, rev, err := cl.Info(false)
    if err != nil {
        c.doneSync()
        utils.Print("CLUSTER", "SYNC-CLIENT-ERROR %s", err.Error())
        return err
    }
    c.Mutex.Lock()

    // Update our nodemap.
    force, err := c.lockedDecodeInfo(info)
    if err != nil {
        utils.Print("CLUSTER", "SYNC-DATA-ERROR %s", err.Error())
        c.Mutex.Unlock()
        c.doneSync()
        return err
    }

    // Update our revision.
    _, err = c.lockedChangeRevision(rev, force)
    c.Mutex.Unlock()
    c.doneSync()
    return err
}

func (c *Cluster) SetSyncLimit(active uint) {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    c.syncLimit = active
}

func (c *Cluster) GossipUpdate(addr *net.UDPAddr, id string, url string, rev core.Revision, dead []string) *core.Node {
    // Mark dead nodes as dead.
    for _, id := range dead {
        c.Nodes.NoHeartbeat(id)
    }

    // Update our nodes.
    // NOTE: This will read the rev unlocked (so there's a
    // change this line might be a bit racey). But there's
    // no real downside to generating an extra sync().
    if !c.Nodes.Heartbeat(id, rev) || rev.GreaterThan(c.Revision()) {
        // We don't know about this node.
        // Refresh addr will go get info
        // from this node by address.
        c.doSync(core.APIFromURL(url, addr.String()))
        master := c.ring.MasterFor(RootKey)
        if master != c.Nodes.Self() {
            return master
        }
    }

    return nil
}

func (c *Cluster) syncData(old_ring *ring, ns core.Namespace, key core.Key, syncLimit chan bool, notify chan bool) {
    // Wait for a slot.
    <-syncLimit

    utils.Print("CLUSTER", "SYNC-START ns=%s key=%s", ns, key)

    // Pull in our local value.
    value, local_rev, err := c.Data.DataGet(ns, key, true)
    if err != nil {
        value = nil
        local_rev = core.NoRevision
    }

    // Pull in the quorum revision.
    rev, err := c.quorumInfo(old_ring, ns, key)
    if err != nil || !rev.Equals(local_rev) {
        // Pull in the quorum value (from the old ring).
        value, rev, err = c.quorumGet(old_ring, ns, key)
        if err != nil {
            // It's possible that the quorumGet failed because we were in the middle
            // of some other quorumSet(). That's fine, whatever other quroumSet() will
            // complete and ultimately represent the correct quorumValue.
            syncLimit <- true
            utils.Print("CLUSTER", "SYNC-GET-ERROR key=%s rev=%s %s", key, rev.String(), err.Error())
            notify <- false
            return
        }
    }

    // Set the full cluster.
    rev, err = c.quorumSet(c.ring, ns, key, rev, value)
    if err != nil && err != core.RevConflict {
        // It's possible that the cluster has changed again by the time this
        // set has actually had a chance to run. That's okay, at some point
        // the most recent quorumSet will work (setting the most recent ring).
        syncLimit <- true
        utils.Print("CLUSTER", "SYNC-QUORUM-ERROR key=%s rev=%s %s", key, rev.String(), err.Error())
        notify <- false
        return
    }

    syncLimit <- true
    utils.Print("CLUSTER", "SYNC-OKAY key=%s rev=%s", key, rev.String())
    notify <- true
}

func (c *Cluster) purgeData(ns core.Namespace, key core.Key, notify chan bool) {
    utils.Print("CLUSTER", "PURGE ns=%s key=%s", ns, key)
    c.Data.DataRemove(ns, key, core.NoRevision)
    notify <- true
}

func (c *Cluster) lockedChangeRevision(rev core.Revision, force bool) (core.Revision, error) {
    // We only update revisions unless force is set.
    // Force will be set when we are joining some other
    // cluster (with a potentially conflicting history)
    // so we may end up with a lower revision.
    if c.Revision().GreaterThan(rev) && !force {
        utils.Print("CLUSTER", "Cowarding refusing to downgrade revision.")
        return c.Revision(), nil
    }

    utils.Print("CLUSTER", "Revision %s has %d active nodes.",
        rev.String(), len(c.Nodes.Inuse()))

    // Do a synchronize of all of our keys across this new
    // revision. This will happen before we bump our version
    // in order to ensure that everyone we send to other hosts
    // has the correct version.
    old_ring := c.ring
    c.ring = NewRing(2*c.N+1, c.Nodes)
    if c.Revision().IsZero() {
        // This is a cold start -- we don't really have an
        // old ring that we can use to compute. In this case,
        // we simply start from the new ring and rely on the
        // fact that we must be one of the N-1 failures.
        old_ring = c.ring
    }

    // List all available namespaces.
    namespaces, err := c.Data.DataNamespaces()
    if err != nil {
        utils.Print("CLUSTER", "NAMESPACE-ERROR rev=%s", rev.String())
        return c.Revision(), err
    }

    done := 0
    scheduled := 0
    notify := make(chan bool)
    syncLimit := make(chan bool, c.syncLimit)
    for i := 0; i < int(c.syncLimit); i += 1 {
        syncLimit <- true
    }

    // Update every namespace.
    for _, ns := range namespaces {

        // Read the items for this namespace.
        items, err := c.Data.DataList(ns)
        if err != nil {
            utils.Print("CLUSTER", "REVISION-DATA-ERROR rev=%s", rev.String())
            continue
        }

        // Schedule updates for every key.
        for key, _ := range items {

            _, rev, err := c.Data.DataGet(ns, key, false)
            if rev.IsZero() || err != nil {
                // Something is wrong with this key.
                // We purge this guy.
                go c.purgeData(ns, key, notify)
                scheduled += 1

            } else if c.Access.Has(ns) {
                new_master := c.ring.IsMaster(key)
                new_slave := c.ring.IsSlave(key)

                if new_master {
                    // We're the new master for this key.
                    // We unconditionally do a sync for this key
                    // in order to ensure that all the slaves have
                    // the correct data in place.
                    go c.syncData(old_ring, ns, key, syncLimit, notify)
                    scheduled += 1

                } else if c.ring.IsFailover(key) &&
                    !old_ring.IsNode(key, c.ring.MasterFor(key)) {
                    // Ensure that the master is aware of this key.
                    // This happens if the new master was not in the
                    // ring previously, but is now in the ring.
                    go c.syncData(old_ring, ns, key, syncLimit, notify)
                    scheduled += 1
                }

                // We don't have anything to do with this key.
                // Scrub it from our local datastore.
                // NOTE: This is done unconditionally because
                // for this ring, we aren't expected to have any
                // data related to this key. If we change rings
                // again, the new data will be populated.
                if !new_slave && !new_master {
                    go c.purgeData(ns, key, notify)
                    scheduled += 1
                }

                // We aren't the master for this key.
                // We ensure that all the synchronization
                // groups have fired events (and they will
                // be appropriately redirected to the true
                // master).
                if !new_master {
                    c.Data.EventFire(ns, key, core.NoRevision)
                }
            } else {
                // This namespace doesn't exist, purge.
                go c.purgeData(ns, key, notify)
                scheduled += 1
            }
        }
    }

    // Unlock and allow data access to proceeed
    // during updates (individual revs are still
    // monotonic, so updates should be "okay).
    // But I haven't really thought about this.
    c.Mutex.Unlock()

    // Wait until we've updated all keys.
    for done < scheduled {
        <-notify
        done += 1
    }

    // Relock before the update.
    c.Mutex.Lock()

    // Update the revision.
    c.Nodes.Self().Current = rev
    c.force = false

    return c.Revision(), nil
}

func (c *Cluster) healthcheck() {
    // If we're not active skip it.
    if !c.Active() {
        utils.Print("CLUSTER", "NOT-ACTIVE")
        return
    }

    // Our next revision.
    target_rev := c.Revision().Next()

    for {
        c.Mutex.Lock()

        // If we have unknowns, let's propose a change.
        master := c.ring.MasterFor(RootKey)
        is_master := (master == nil || master == c.Nodes.Self())
        is_failover := c.ring.IsFailover(RootKey)

        if is_master {
            utils.Print("CLUSTER", "HEALTHCHECK-MASTER")
        } else if is_failover {
            utils.Print("CLUSTER", "HEALTHCHECK-FAILOVER")
        } else {
            utils.Print("CLUSTER", "HEALTHCHECK-SLAVE")
        }

        if is_master || (is_failover && !master.Alive()) {
            // Encode a cluster delta.
            bytes, err := c.lockedEncode(true, target_rev)

            // Check if everything is okay.
            if bytes == nil || err != nil {
                c.Mutex.Unlock()
                break
            }

            // Try to do the data set.
            ok, new_rev, err := c.lockedClusterDataSet(bytes, target_rev)
            c.Mutex.Unlock()
            if err != nil {
                break
            }
            if ok {
                // We've done it! We've changed the revision.
                break
            }

            target_rev = new_rev.Next()
            continue

        } else {
            // We're not the master.
            c.Mutex.Unlock()
            break
        }
    }
}

func (c *Cluster) lockedClusterDataSet(bytes []byte, target_rev core.Revision) (bool, core.Revision, error) {
    // Release the lock, otherwise we could end up with
    // a distributed deadlock in the cluster. This will
    // require us to sanity-check our result below a bit
    // more carefully.
    orig_rev := c.Revision()
    c.Mutex.Unlock()

    // Update the node configuration and bump our cluster
    // version number. Note that we must first successfully
    // contact a quorum of nodes to ensure that we're in the
    // majority still.
    rev, err := c.quorumSet(
        c.ring, RootNamespace, RootKey, target_rev, bytes)
    c.Mutex.Lock()

    ok := true
    if err == core.RevConflict {
        ok = false
        err = nil
    }
    if err != nil {
        utils.Print("CLUSTER", "SET-CLUSTER-ERROR %s", err.Error())
        return ok, rev, err
    }

    // Yikes, something has happened between the quorum set
    // above and the lock being reacquired. Let it be handled
    // on the next healthcheck and bail.
    if !rev.Equals(target_rev) || !c.Revision().Equals(orig_rev) {
        utils.Print("CLUSTER", "SET-CLUSTER-REV-CHANGED")
        return ok, rev, nil
    }

    // Return the decode error.
    force, err := c.lockedDecodeBytes(bytes)
    if err != nil {
        return ok, c.Revision(), err
    }

    // Update our revision.
    rev, err = c.lockedChangeRevision(rev, force)
    return ok, rev, err
}

func (c *Cluster) timedHealthcheck() {
    c.healthcheck()

    // Restart the healthcheck.
    f := func() { c.timedHealthcheck() }
    time.AfterFunc(time.Duration(1)*time.Second, f)
}

func NewCluster(store *storage.Store, addr string, url string, root core.Token, domain string, ids []string) (*Cluster, error) {

    c := new(Cluster)
    c.N = uint(0)
    c.N_new = c.N
    c.N_mod = core.NoRevision
    c.root = root
    c.Nodes = core.NewNodes(addr, url, ids, domain)
    c.Access = core.NewAccess(root)
    c.Data = core.NewData(store)
    c.ring = NewRing(2*c.N+1, c.Nodes)

    // Initialize synchronization structures.
    c.quorumLock = sync.NewCond(&sync.Mutex{})
    c.quorumInProgress = make(map[core.Key]bool)
    c.quorumWaiters = make(map[core.Key]uint)

    // Initialize our syncLimit.
    c.syncLimit = 1

    // Read cluster data.
    data, rev, err := c.Data.DataGet(RootNamespace, RootKey, true)
    if err == nil && !rev.IsZero() {
        utils.Print("CLUSTER", "START-FOUND rev=%s", rev.String())

        // Load the existing cluster data.
        c.Mutex.Lock()
        force, err := c.lockedDecodeBytes(data)
        if err == nil {
            rev, err = c.lockedChangeRevision(rev, force)
            c.Mutex.Unlock()
            if err != nil {
                utils.Print("CLUSTER", "START-ERROR %s", err.Error())
            } else {
                utils.Print("CLUSTER", "START-OKAY rev=%s", rev.String())
            }
        } else {
            c.Mutex.Unlock()
            c.Data.DataRemove(RootNamespace, RootKey, core.NoRevision)
        }
    } else {
        utils.Print("CLUSTER", "NO-START-FOUND")
    }

    // Start running our healthcheck.
    go c.timedHealthcheck()

    return c, nil
}
