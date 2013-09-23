package core

import (
    "errors"
    "fmt"
    "hibera/storage"
    "hibera/utils"
    "strings"
    "sync"
    "time"
)

// The ephemeral id is used to identify and remove
// keys associated with individual connections. This
// will generally be some sort of ClientId from the
// higher-level but it is kept abstract for here.

type RevisionMap map[Key]Revision
type DataMap map[string]Revision
type EphemeralSet map[EphemId]DataMap

var NotFound = errors.New("Key not found or inconsistent")
var RevConflict = errors.New("Revision is not sufficient")

type Lock struct {
    sem     chan bool
    waiters []chan bool
}

func (l *Lock) lock() {
    <-l.sem
}

func (l *Lock) unlock() {
    l.sem <- true
}

func (l *Lock) wait(timeout bool, duration time.Duration, notifier <-chan bool) bool {
    // Setup some basic channels for doing our wait
    // on this lock. We keep waiting as long as there
    // is someone on the other end of the connection.
    timeoutchan := make(chan bool, 1)
    wakeupchan := make(chan bool, 1)

    // Start the timeout channel.
    go func() {
        if timeout {
            time.Sleep(duration)
            timeoutchan <- true
        }
    }()

    // We release this lock at this point.
    // NOTE that we've added our unique channel
    // to the list of waiters, so when a notification
    // happens we are guaranteed that we will get
    // a notification.
    l.waiters = append(l.waiters, wakeupchan)
    l.sem <- true

    rval := true

    select {
    case alive := <-wakeupchan:
        rval = alive
        break

    case <-timeoutchan:
    case <-notifier:
        rval = false
        break
    }

    // Reaquire the lock before returning.
    <-l.sem

    return rval
}

func (l *Lock) notify(alive bool) {
    // Send all application notifications.
    for _, wakechan := range l.waiters {
        wakechan <- alive
    }

    // Reset our waiter list.
    l.waiters = make([]chan bool, 0)
}

func NewLock() *Lock {
    lock := new(Lock)
    lock.sem = make(chan bool, 1)
    lock.waiters = make([]chan bool, 0)
    lock.sem <- true
    return lock
}

type Data struct {
    // Synchronization.
    // The global.Cond.L protects access to
    // the map of all locks. We could easily
    // split this into a more scalable structure
    // if it becomes a clear bottleneck.
    locks map[Namespace]map[Key]*Lock
    revs  map[Namespace]RevisionMap
    *sync.Cond

    // Whether or not this has been flushed.
    // No waits proceed if it has been flushed.
    flushed bool

    // In-memory ephemera.
    // (Kept synchronized by other modules).
    sync map[Namespace]map[Key]*EphemeralSet

    // Our backend.
    // This is used to store data keys.
    store *storage.Store
}

func (d *Data) lock(ns Namespace, key Key) *Lock {
    d.Cond.L.Lock()
    ns_locks, has_ns := d.locks[ns]
    if !has_ns {
        ns_locks = make(map[Key]*Lock)
        d.revs[ns] = make(RevisionMap)
        d.sync[ns] = make(map[Key]*EphemeralSet)
        d.locks[ns] = ns_locks
    }
    lock := ns_locks[key]
    if lock == nil {
        lock = NewLock()
        ns_locks[key] = lock
    }
    d.Cond.L.Unlock()
    lock.lock()
    return lock
}

func (d *Data) getSyncRev(ns Namespace, key Key) Revision {
    return d.revs[ns][key]
}

func (d *Data) setSyncRev(ns Namespace, key Key, rev Revision) {
    d.revs[ns][key] = rev
}

func (d *Data) delSyncRev(ns Namespace, key Key) {
    delete(d.revs[ns], key)
}

func (d *Data) getSyncMap(ns Namespace, key Key) *EphemeralSet {
    return d.sync[ns][key]
}

func (d *Data) setSyncMap(ns Namespace, key Key, revmap *EphemeralSet) {
    d.sync[ns][key] = revmap
}

func (d *Data) delSyncMap(ns Namespace, key Key) {
    delete(d.sync[ns], key)
}

func toStoreKey(ns Namespace, key Key) string {
    return fmt.Sprintf("%s:%s", string(ns), string(key))
}

func fromStoreKey(data string) (Namespace, Key) {
    parts := strings.SplitN(data, ":", 2)
    if len(parts) == 1 {
        return Namespace(""), Key(parts[0])
    }
    return Namespace(parts[0]), Key(parts[1])
}

func (d *Data) DataNamespaces() ([]Namespace, error) {
    items, err := d.store.List()
    if err != nil {
        return nil, err
    }
    namespaces_map := make(map[Namespace]bool)
    for _, item := range items {
        ns, _ := fromStoreKey(item)
        namespaces_map[ns] = true
    }
    namespaces := make([]Namespace, 0, 0)
    for namespace, _ := range namespaces_map {
        namespaces = append(namespaces, namespace)
    }

    return namespaces, nil
}

func (d *Data) DataList(ns Namespace) (map[Key]uint, error) {
    items, err := d.store.List()
    if err != nil {
        return nil, err
    }
    keys := make(map[Key]uint)
    utils.Print("DATA", "LIST namespace=%s", ns)
    for _, item := range items {
        item_ns, item_key := fromStoreKey(item)
        if item_ns == ns {
            keys[item_key] = 1
            utils.Print("DATA", "    %s", item_key)
        }
    }

    return keys, nil
}

func (d *Data) SyncMembers(ns Namespace, key Key, data string, limit uint) (SyncInfo, Revision, error) {
    lock := d.lock(ns, key)
    defer lock.unlock()

    // Lookup the key.
    revmap := d.getSyncMap(ns, key)
    if revmap == nil {
        // This is valid, it's an empty key.
        return NoSyncInfo, d.getSyncRev(ns, key), nil
    }

    // Return the members and rev.
    return d.computeIndex(revmap, data, limit), d.getSyncRev(ns, key), nil
}

func (d *Data) computeIndex(revmap *EphemeralSet, data string, limit uint) SyncInfo {
    var info SyncInfo

    // Aggregate all members across clients.
    allmap := make(map[string]Revision, 0)
    for _, set := range *revmap {
        for member, revjoined := range set {
            allmap[member] = revjoined
        }
    }

    // Pull out the correct number from allmap.
    if limit == 0 || len(allmap) < int(limit) {
        limit = uint(len(allmap))
    }
    current := uint(0)
    info.Members = make([]string, limit, limit)
    for candidate, revjoined := range allmap {
        if current < limit {
            info.Members[current] = candidate
            current += 1
        } else {
            for i, current := range info.Members {
                if allmap[current].GreaterThan(revjoined) {
                    info.Members[i] = candidate
                    break
                }
            }
        }
    }

    // Do a simple insertion sort on the set.
    // This is efficient given the small size.
    for i, membermin1 := range info.Members {
        for j, membermin2 := range info.Members {
            if i < j &&
                allmap[membermin1].GreaterThan(allmap[membermin2]) {
                // Swap, we're not the right order.
                info.Members[i] = membermin2
                info.Members[j] = membermin1
            }
        }
    }

    // Save the index if it's available.
    info.Index = -1
    for i, current := range info.Members {
        if current == data {
            info.Index = i
        }
    }

    return info
}

func (d *Data) SyncJoin(id EphemId, ns Namespace, key Key, data string, limit uint, timeout uint, notifier <-chan bool, valid func() bool) (int, Revision, error) {

    lock := d.lock(ns, key)
    defer lock.unlock()

    // Go through in a safe way.
    index := -1
    start := time.Now()
    end := start.Add(time.Duration(timeout) * time.Millisecond)
    var revmap *EphemeralSet
    for {
        // Lookup the key.
        revmap = d.getSyncMap(ns, key)
        if revmap == nil {
            newmap := make(EphemeralSet)
            revmap = &newmap
            d.setSyncMap(ns, key, revmap)
        }

        // Lookup the key and see if we're a member.
        _, present := (*revmap)[id]
        if !present {
            (*revmap)[id] = make(DataMap)
        }

        // Check that we're still not there already.
        if !(*revmap)[id][data].IsZero() {
            info := d.computeIndex(revmap, data, limit)
            return info.Index, d.getSyncRev(ns, key), nil
        }

        // Count the number of holders.
        members := uint(0)
        for _, names := range *revmap {
            members += uint(len(names))
        }
        if limit == 0 || members < limit {
            index = int(members)
            break
        }

        // Wait for a change.
        now := time.Now()
        if timeout > 0 && now.After(end) {
            info := d.computeIndex(revmap, data, limit)
            return info.Index, d.getSyncRev(ns, key), nil
        }
        if !lock.wait(timeout > 0, end.Sub(now), notifier) || !valid() {
            info := d.computeIndex(revmap, data, limit)
            return info.Index, d.getSyncRev(ns, key), nil
        }
    }

    // Join and fire.
    rev, err := d.doFire(ns, key, NoRevision, lock)
    (*revmap)[id][data] = rev
    utils.Print("DATA", "JOIN key=%s index=%d id=%d",
        key, index, uint64(id))
    return index, rev, err
}

func (d *Data) SyncLeave(id EphemId, ns Namespace, key Key, data string) (Revision, error) {
    lock := d.lock(ns, key)
    defer lock.unlock()

    // Lookup the key and see if we're a member.
    revmap := d.getSyncMap(ns, key)
    if revmap == nil {
        return NoRevision, NotFound
    }

    // Leave the key.
    changed := false
    _, present := (*revmap)[id]
    if present {
        delete((*revmap)[id], data)
        if len((*revmap)[id]) == 0 {
            delete((*revmap), id)
        }
        changed = true
    }
    if len(*revmap) == 0 {
        d.delSyncMap(ns, key)
    }

    utils.Print("DATA", "LEAVE key=%s id=%d",
        key, uint64(id))
    if changed {
        return d.doFire(ns, key, NoRevision, lock)
    }
    return d.getSyncRev(ns, key), NotFound
}

func (d *Data) DataGet(ns Namespace, key Key, with_data bool) ([]byte, Revision, error) {

    var data []byte
    var metadata []byte
    var err error

    if with_data {
        // Read the local value available.
        metadata, data, err = d.store.Read(toStoreKey(ns, key))
    } else {
        // Read the header only.
        metadata, err = d.store.Info(toStoreKey(ns, key))
    }

    if err != nil {
        return nil, NoRevision, err
    }
    return data, RevisionFromBytes(metadata), err
}

func (d *Data) DataWatch(
    id EphemId,
    ns Namespace,
    key Key,
    rev Revision, timeout uint,
    notifier <-chan bool, valid func() bool,
    with_data bool) ([]byte, Revision, error) {

    lock := d.lock(ns, key)
    defer lock.unlock()

    if id > 0 {
        getrev := func() (Revision, error) {
            metadata, _, _, done, err := d.store.ReadFile(toStoreKey(ns, key), nil, nil)
            done() // Release all locks, we only care about revision.
            return RevisionFromBytes(metadata), err
        }
        rev, err := d.doWait(id, ns, key, lock, rev, timeout, getrev, notifier, valid)
        if err != nil {
            return nil, rev, err
        }
    }

    // Read the local store.
    return d.DataGet(ns, key, with_data)
}

func (d *Data) DataModify(ns Namespace, key Key, rev Revision, mod func(Revision) error) (Revision, error) {

    lock := d.lock(ns, key)
    defer func() {
        lock.notify(true)
        lock.unlock()
    }()

    // Read the existing data.
    metadata, _, err := d.store.Read(toStoreKey(ns, key))
    if err != nil {
        return NoRevision, err
    }
    metadata_rev := RevisionFromBytes(metadata)
    if rev.IsZero() {
        rev = metadata_rev.Next()
    }
    if !rev.GreaterThan(metadata_rev) {
        return metadata_rev, RevConflict
    }

    // Do the operation.
    // NOTE: When we leave this function,
    // we always fire a notification event
    // on the lock for this key. That will
    // wake any data watchers.
    return rev, mod(rev)
}

func (d *Data) DataSet(ns Namespace, key Key, rev Revision, value []byte) (Revision, error) {

    return d.DataModify(ns, key, rev, func(rev Revision) error {
        return d.store.Write(toStoreKey(ns, key), rev.Bytes(), value)
    })
}

func (d *Data) DataRemove(ns Namespace, key Key, rev Revision) (Revision, error) {

    return d.DataModify(ns, key, rev, func(rev Revision) error {
        return d.store.Delete(toStoreKey(ns, key))
    })
}

func (d *Data) doWait(id EphemId, ns Namespace, key Key, lock *Lock, rev Revision, timeout uint, getrev func() (Revision, error), notifier <-chan bool, valid func() bool) (Revision, error) {

    start := time.Now()
    end := start.Add(time.Duration(timeout) * time.Millisecond)

    var currev Revision
    var err error

    for {
        // Get the current revision.
        currev, err = getrev()
        if err != nil {
            return currev, err
        }

        // Wait until we are no longer on the given rev.
        if !currev.Equals(rev) {
            break
        }

        // Check that this is a valid wait.
        if !valid() || d.flushed {
            break
        }

        // Wait for a change.
        now := time.Now()
        if timeout > 0 && now.After(end) {
            break
        }
        if !lock.wait(timeout > 0, end.Sub(now), notifier) || !valid() || d.flushed {
            break
        }
    }

    // Return the new rev.
    return currev, nil
}

func (d *Data) EventWait(id EphemId, ns Namespace, key Key, rev Revision, timeout uint, notifier <-chan bool, valid func() bool) (Revision, error) {

    lock := d.lock(ns, key)
    defer lock.unlock()

    getrev := func() (Revision, error) {
        return d.getSyncRev(ns, key), nil
    }
    // If the rev is 0, then we start waiting on the current rev.
    if rev.IsZero() {
        rev, _ = getrev()
    }
    return d.doWait(id, ns, key, lock, rev, timeout, getrev, notifier, valid)
}

func (d *Data) doFire(ns Namespace, key Key, rev Revision, lock *Lock) (Revision, error) {

    // Check our condition.
    if !rev.IsZero() && !rev.GreaterThan(d.getSyncRev(ns, key)) {
        return d.getSyncRev(ns, key), NotFound
    }

    // Update our rev.
    if rev.IsZero() {
        rev = d.getSyncRev(ns, key).Next()
    }
    d.setSyncRev(ns, key, rev)

    // Broadcast.
    lock.notify(true)

    utils.Print("DATA", "FIRED key=%s rev=%s", key, rev.String())
    return rev, nil
}

func (d *Data) EventFire(ns Namespace, key Key, rev Revision) (Revision, error) {
    lock := d.lock(ns, key)
    defer lock.unlock()

    // Fire on a synchronization item.
    return d.doFire(ns, key, rev, lock)
}

func (d *Data) Purge(id EphemId) {
    utils.Print("DATA", "PURGE id=%d", uint64(id))
    to_purge := make(map[Namespace][]Key)

    // Kill off all ephemeral nodes.
    d.Cond.L.Lock()
    for namespace, revmap := range d.sync {
        added := false
        for key, members := range revmap {
            if len((*members)[id]) > 0 {
                if !added {
                    to_purge[namespace] = make([]Key, 0)
                    added = true
                }
                to_purge[namespace] = append(to_purge[namespace], key)
            }
            delete(*members, id)
        }
    }
    d.Cond.L.Unlock()

    // Fire watches.
    for namespace, keys := range to_purge {
        for _, key := range keys {
            d.EventFire(namespace, key, NoRevision)
        }
    }
}

func (d *Data) Flush() {
    d.Cond.L.Lock()
    defer d.Cond.L.Unlock()

    // Disable any additional waits.
    d.flushed = true

    // Fire events on all locks.
    for _, locks := range d.locks {
        for _, lock := range locks {
            lock.notify(false)
        }
    }
}

func NewData(store *storage.Store) *Data {
    d := new(Data)
    d.Cond = sync.NewCond(new(sync.Mutex))
    d.store = store
    d.locks = make(map[Namespace]map[Key]*Lock)
    d.revs = make(map[Namespace]RevisionMap)
    d.sync = make(map[Namespace]map[Key]*EphemeralSet)
    d.flushed = false
    return d
}
