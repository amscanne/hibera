package core

import (
    "errors"
    "hibera/storage"
    "hibera/utils"
    "sync"
    "time"
)

// The ephemeral id is used to identify and remove
// keys associated with individual connections. This
// will generally be some sort of ClientId from the
// higher-level but it is kept abstract for here.

type RevisionMap map[Key]Revision
type NameMap map[string]Revision
type EphemeralSet map[EphemId]NameMap

var NotFound = errors.New("Key not found or inconsistent")

type Lock struct {
    sem     chan bool
    waiters []chan bool
}

func (l *Lock) lock(key Key) {
    utils.Print("DATA", "key=%s acquiring", string(key))
    <-l.sem
    utils.Print("DATA", "key=%s acquired", string(key))
}

func (l *Lock) unlock(key Key) {
    utils.Print("DATA", "key=%s released", string(key))
    l.sem <- true
}

func (l *Lock) wait(key Key, timeout bool, duration time.Duration, notifier <-chan bool) bool {
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
    utils.Print("DATA", "key=%s waiting", string(key))
    l.waiters = append(l.waiters, wakeupchan)
    l.sem <- true

    rval := true

    select {
    case <-wakeupchan:
    case <-timeoutchan:
        rval = true
        break

    case <-notifier:
        rval = false
        break
    }

    // Reaquire the lock before returning.
    <-l.sem

    return rval
}

func (l *Lock) notify(key Key) {
    utils.Print("DATA", "key=%s notifying", string(key))

    // Send all application notifications.
    for _, wakechan := range l.waiters {
        wakechan <- true
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
    sync map[Key]*Lock
    revs RevisionMap
    *sync.Cond

    // In-memory ephemera.
    // (Kept synchronized by other modules).
    keys map[Key]*EphemeralSet

    // Our backend.
    // This is used to store data keys.
    store *storage.Backend
}

func (d *Data) lock(key Key) *Lock {
    d.Cond.L.Lock()
    lock := d.sync[key]
    if lock == nil {
        lock = NewLock()
        d.sync[key] = lock
    }
    d.Cond.L.Unlock()
    lock.lock(key)
    return lock
}

func (d *Data) DataList() ([]Key, error) {
    items, err := d.store.List()
    if err != nil {
        return nil, err
    }
    keys := make([]Key, len(items), len(items))
    for i, item := range items {
        keys[i] = Key(item)
    }

    return keys, nil
}

func (d *Data) SyncMembers(key Key, name string, limit uint) (int, []string, Revision, error) {
    lock := d.lock(key)
    defer lock.unlock(key)

    // Lookup the key.
    index := -1
    revmap := d.keys[key]
    if revmap == nil {
        // This is valid, it's an empty key.
        return index, make([]string, 0), d.revs[key], nil
    }

    // Return the members and rev.
    index, members := d.computeIndex(revmap, name, limit)
    return index, members, d.revs[key], nil
}

func (d *Data) computeIndex(revmap *EphemeralSet, name string, limit uint) (int, []string) {
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
    members := make([]string, limit, limit)
    for candidate, revjoined := range allmap {
        if current < limit {
            members[current] = candidate
            current += 1
        } else {
            for i, current := range members {
                if revjoined <= allmap[current] {
                    members[i] = candidate
                    break
                }
            }
        }
    }

    // Do a simple insertion sort on the set.
    // This is efficient given the small size.
    for i, membermin1 := range members {
        for j, membermin2 := range members {
            if i < j &&
                allmap[membermin2] < allmap[membermin1] {
                // Swap, we're not the right order.
                members[i] = membermin2
                members[j] = membermin1
            }
        }
    }

    // Save the index if it's available.
    index := -1
    for i, current := range members {
        if current == name {
            index = i
        }
    }

    return index, members
}

func (d *Data) SyncJoin(
    id EphemId, key Key, name string, limit uint, timeout uint,
    notifier <-chan bool,
    valid func() bool) (int, Revision, error) {

    lock := d.lock(key)
    defer lock.unlock(key)

    // Go through in a safe way.
    index := -1
    start := time.Now()
    end := start.Add(time.Duration(timeout) * time.Millisecond)
    var revmap *EphemeralSet
    for {
        // Lookup the key.
        revmap = d.keys[key]
        if revmap == nil {
            newmap := make(EphemeralSet)
            revmap = &newmap
            d.keys[key] = revmap
        }

        // Lookup the key and see if we're a member.
        _, present := (*revmap)[id]
        if !present {
            (*revmap)[id] = make(NameMap)
        }

        // Check that we're still not there already.
        if (*revmap)[id][name] > 0 {
            index, _ := d.computeIndex(revmap, name, limit)
            return index, d.revs[key], nil
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
            index, _ := d.computeIndex(revmap, name, limit)
            return index, d.revs[key], nil
        }
        if !lock.wait(key, timeout > 0, end.Sub(now), notifier) || !valid() {
            index, _ := d.computeIndex(revmap, name, limit)
            return index, d.revs[key], nil
        }
    }

    // Join and fire.
    rev, err := d.doFire(key, 0, lock)
    (*revmap)[id][name] = rev
    utils.Print("DATA", "JOIN key=%s index=%d id=%d", string(key), index, uint64(id))
    return index, rev, err
}

func (d *Data) SyncLeave(id EphemId, key Key, name string) (Revision, error) {
    lock := d.lock(key)
    defer lock.unlock(key)

    // Lookup the key and see if we're a member.
    revmap := d.keys[key]
    if revmap == nil {
        return 0, NotFound
    }

    // Leave the key.
    _, present := (*revmap)[id]
    if present {
        delete((*revmap)[id], name)
        if len((*revmap)[id]) == 0 {
            delete((*revmap), id)
        }
    }
    if len(*revmap) == 0 {
        delete(d.keys, key)
    }

    utils.Print("DATA", "LEAVE key=%s id=%d", string(key), uint64(id))
    return d.doFire(key, 0, lock)
}

func (d *Data) DataGet(key Key) ([]byte, Revision, error) {
    // Read the local value available.
    value, rev, err := d.store.Read(string(key))
    return value, Revision(rev), err
}

func (d *Data) DataWatch(
    id EphemId, key Key, rev Revision, timeout uint,
    notifier <-chan bool,
    valid func() bool) ([]byte, Revision, error) {

    lock := d.lock(key)
    defer lock.unlock(key)

    if id > 0 {
        getrev := func() (Revision, error) {
            _, lrev, err := d.store.Read(string(key))
            return Revision(lrev), err
        }
        rev, err := d.doWait(id, key, lock, rev, timeout, getrev, notifier, valid)
        if err != nil {
            return nil, rev, err
        }
    }

    // Read the local store.
    return d.DataGet(key)
}

func (d *Data) DataModify(key Key, rev Revision, mod func(Revision) error) (Revision, error) {
    lock := d.lock(key)
    defer func() {
        lock.notify(key)
        lock.unlock(key)
    }()

    // Read the existing data.
    _, orev, err := d.store.Read(string(key))
    if err != nil {
        return Revision(orev), err
    }
    if rev == 0 {
        rev = Revision(orev) + 1
    }
    if rev <= Revision(orev) {
        return Revision(orev), err
    }

    // Do the operation.
    return rev, mod(rev)
}

func (d *Data) DataSet(key Key, rev Revision, value []byte) (Revision, error) {
    return d.DataModify(key, rev, func(rev Revision) error {
        return d.store.Write(string(key), value, uint64(rev))
    })
}

func (d *Data) DataPromise(key Key, rev Revision) (Revision, error) {
    return d.DataModify(key, rev, func(rev Revision) error {
        return d.store.Promise(string(key), uint64(rev))
    })
}

func (d *Data) DataRemove(key Key, rev Revision) (Revision, error) {
    return d.DataModify(key, rev, func(rev Revision) error {
        err := d.store.Delete(string(key))
        if err == nil {
            delete(d.revs, key)
        }
        return err
    })
}

func (d *Data) doWait(
    id EphemId, key Key, lock *Lock, rev Revision, timeout uint,
    getrev func() (Revision, error),
    notifier <-chan bool,
    valid func() bool) (Revision, error) {

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
        if currev != rev {
            break
        }

        // Wait for a change.
        now := time.Now()
        if timeout > 0 && now.After(end) {
            return currev, nil
        }
        if !lock.wait(key, timeout > 0, end.Sub(now), notifier) || !valid() {
            return currev, nil
        }
    }

    // Return the new rev.
    return currev, nil
}

func (d *Data) EventWait(
    id EphemId, key Key, rev Revision, timeout uint,
    notifier <-chan bool,
    valid func() bool) (Revision, error) {

    lock := d.lock(key)
    defer lock.unlock(key)

    getrev := func() (Revision, error) {
        return d.revs[key], nil
    }
    // If the rev is 0, then we start waiting on the current rev.
    if rev == 0 {
        rev, _ = getrev()
    }
    return d.doWait(id, key, lock, rev, timeout, getrev, notifier, valid)
}

func (d *Data) doFire(key Key, rev Revision, lock *Lock) (Revision, error) {
    // Check our condition.
    if rev != 0 && rev <= d.revs[key] {
        return d.revs[key], NotFound
    }

    // Update our rev.
    if rev == 0 {
        rev = d.revs[key] + 1
    }
    d.revs[key] = rev

    // Broadcast.
    lock.notify(key)

    return rev, nil
}

func (d *Data) EventFire(key Key, rev Revision) (Revision, error) {
    lock := d.lock(key)
    defer lock.unlock(key)

    // Fire on a synchronization item.
    return d.doFire(key, rev, lock)
}

func (d *Data) Purge(id EphemId) {
    utils.Print("DATA", "PURGE id=%d", uint64(id))
    paths := make([]Key, 0)

    // Kill off all ephemeral nodes.
    d.Cond.L.Lock()
    for key, members := range d.keys {
        if len((*members)[id]) > 0 {
            paths = append(paths, key)
        }
        delete(*members, id)
    }
    d.Cond.L.Unlock()

    // Fire watches.
    for _, path := range paths {
        d.EventFire(path, 0)
    }
}

func (d *Data) SaveClusterId(id string) error {
    return d.store.SetCluster(id)
}

func (d *Data) LoadClusterId() (string, error) {
    return d.store.GetCluster()
}

func NewData(store *storage.Backend) *Data {
    d := new(Data)
    d.Cond = sync.NewCond(new(sync.Mutex))
    d.store = store
    d.sync = make(map[Key]*Lock)
    d.revs = make(RevisionMap)
    d.keys = make(map[Key]*EphemeralSet)
    return d
}
