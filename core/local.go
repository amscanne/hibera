package core

import (
	"sync"
	"time"
	"hibera/storage"
)

// The ephemeral id is used to identify and remove
// keys associated with individual connections. This
// will generally be some sort of ClientId from the
// higher-level but it is kept abstract for here.

type RevisionMap map[Key]Revision
type NameMap map[SubName]Revision
type EphemeralSet map[EphemId]NameMap

type Lock struct {
	*sync.Cond
}

func (l *Lock) lock() {
	l.Cond.L.Lock()
}

func (l *Lock) unlock() {
	l.Cond.L.Unlock()
}

func (l *Lock) timeout(duration time.Duration) {
	time.Sleep(duration)
	l.Cond.Broadcast()
}

func (l *Lock) wait(timeout bool, duration time.Duration) {
	if timeout {
		go l.timeout(duration)
	}
	l.Cond.Wait()
}

func (l *Lock) notify() {
	l.Cond.Broadcast()
}

func NewLock() *Lock {
	lock := new(Lock)
	lock.Cond = sync.NewCond(new(sync.Mutex))
	return lock
}

type Local struct {
	// Our backend.
	data *storage.Backend

	// Synchronization.
	// The global Mutex protects access to
	// the map of all locks. We could easily
	// split this into a more scalable structure
	// if it becomes a clear bottleneck.
	sync map[Key]*Lock
	revs RevisionMap
	sync.Mutex

	// In-memory data.
	// (Kept synchronized by other modules).
	groups map[Key]*EphemeralSet
	locks  map[Key]*EphemeralSet
}

func (l *Local) lock(key Key) *Lock {
	l.Mutex.Lock()
	lock := l.sync[key]
	if lock == nil {
		lock = NewLock()
		l.sync[key] = lock
	}
	l.Mutex.Unlock()
	lock.lock()
	return lock
}

func (l *Local) Info() (Info, error) {
	return Info{}, nil
}

func (l *Local) DataList() (*[]Key, error) {
	items, err := l.data.List()
        if err != nil {
            return nil, err
        }
        keys := make([]Key, len(items), len(items))
        for i, item := range items {
            keys[i] = Key(item)
        }
	return &keys, nil
}

func (l *Local) DataClear() error {
	l.Mutex.Lock()
	items, err := l.data.List()
	if err != nil {
		l.Mutex.Unlock()
		return err
	}
	err = l.data.Clear()
	l.Mutex.Unlock()
	if err != nil {
		return err
	}

	// Fire watches.
	for _, path := range items {
		l.WatchFire(Key(path), 0)
	}

	return nil
}

func (l *Local) LockOwners(key Key, name SubName) ([]SubName, Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Get the lock.
	revmap := l.locks[key]
	if revmap == nil {
		// This is valid, it's an unheld lock.
		return make([]SubName, 0), l.revs[key], nil
	}

	// Lookup all the owners.
	owners := make([]SubName, 0)
	for _, set := range *revmap {
		for ownername := range set {
			owners = append(owners, ownername)
		}
	}

	// Return the info.
	return owners, l.revs[key], nil
}

func (l *Local) LockAcquire(id EphemId, key Key, timeout uint64, name SubName, limit uint64) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Get the lock.
	revmap := l.locks[key]
	if revmap == nil {
		newmap := make(EphemeralSet)
		revmap = &newmap
		l.locks[key] = revmap
	}

	// Already held?
	_, present := (*revmap)[id]
	if !present {
		(*revmap)[id] = make(NameMap)
	}
	if (*revmap)[id][name] != 0 {
		return 0, Busy
	}

	start := time.Now()
	end := start.Add(time.Duration(timeout) * time.Millisecond)

	for {
		now := time.Now()
		if timeout > 0 && now.After(end) {
			return 0, Busy
		}

		// Count the number of holders.
		holders := uint64(0)
		for _, names := range *revmap {
			holders += uint64(len(names))
		}
		if holders < limit {
			break
		}

		// Wait for a change.
		lock.wait(timeout > 0, time.Duration(0))
	}

	// Acquire it.
	rev, err := l.doWatchFire(key, 0, lock)
	(*revmap)[id][name] = rev
	return rev, err
}

func (l *Local) LockRelease(id EphemId, key Key, name SubName) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Get the lock.
	revmap := l.locks[key]
	if revmap == nil || (*revmap)[id][name] == 0 {
		return 0, NotFound
	}

	// Release it.
	_, present := (*revmap)[id]
	if !present {
		(*revmap)[id] = make(NameMap)
	}
	delete((*revmap)[id], name)
	if len(*revmap) == 0 {
		delete(l.locks, key)
	}

	return l.doWatchFire(key, 0, lock)
}

func (l *Local) GroupMembers(group Key, name SubName, limit uint64) ([]SubName, Revision, error) {
	lock := l.lock(group)
	defer lock.unlock()

	// Lookup the group.
	revmap := l.groups[group]
	if revmap == nil {
		// This is valid, it's an empty group.
		return make([]SubName, 0), l.revs[group], nil
	}

	// Aggregate all members across clients.
	allmap := make(map[SubName]Revision, 0)
	for _, set := range *revmap {
		for member, revjoined := range set {
			allmap[member] = revjoined
		}
	}

	// Pull out the correct number from allmap.
	if len(allmap) < int(limit) {
		limit = uint64(len(allmap))
	}
	members := make([]SubName, limit, limit)
	for candidate, revjoined := range allmap {
		placed := false
		for i, current := range members {
			if current == "" {
				members[i] = candidate
				placed = true
				break
			}
		}
		if placed {
			continue
		}
		for i, current := range members {
			if revjoined <= allmap[current] {
				members[i] = candidate
				break
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

	// Return the members and rev.
	return members, l.revs[group], nil
}

func (l *Local) GroupJoin(id EphemId, group Key, name SubName) (Revision, error) {
	lock := l.lock(group)
	defer lock.unlock()

	// Lookup the group.
	revmap := l.groups[group]
	if revmap == nil {
		newmap := make(EphemeralSet)
		revmap = &newmap
		l.groups[group] = revmap
	}

	// Lookup the group and see if we're a member.
	_, present := (*revmap)[id]
	if !present {
		(*revmap)[id] = make(NameMap)
	}
	if (*revmap)[id][name] != 0 {
		return 0, Busy
	}

	// Join and fire.
	rev, err := l.doWatchFire(group, 0, lock)
	(*revmap)[id][name] = rev
	return rev, err
}

func (l *Local) GroupLeave(id EphemId, group Key, name SubName) (Revision, error) {
	lock := l.lock(group)
	defer lock.unlock()

	// Lookup the group and see if we're a member.
	revmap := l.groups[group]
	if revmap == nil || (*revmap)[id][name] == 0 {
		return 0, NotFound
	}

	// Leave the group.
	_, present := (*revmap)[id]
	if !present {
		(*revmap)[id] = make(NameMap)
	}
	delete((*revmap)[id], name)
	if len(*revmap) == 0 {
		delete(l.groups, group)
	}

	return l.doWatchFire(group, 0, lock)
}

func (l *Local) DataGet(key Key) ([]byte, Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	value, rev, err := l.data.Read(string(key))
        return value, Revision(rev), err
}

func (l *Local) DataSet(key Key, value []byte, rev Revision) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Check the revisions.
	rev, err := l.doWatchFire(key, rev, lock)
	if err != nil {
		return rev, err
	}

        err = l.data.Write(string(key), value, uint64(rev))
	return Revision(rev), err
}

func (l *Local) DataRemove(key Key, rev Revision) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Check the revisions.
	rev, err := l.doWatchFire(key, rev, lock)
	if err != nil {
		return rev, err
	}

	// Delete and set the revision.
	return 0, l.data.Delete(string(key))
}

func (l *Local) WatchWait(id EphemId, key Key, rev Revision, timeout uint64) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	start := time.Now()
	end := start.Add(time.Duration(timeout) * time.Millisecond)

	for {
		now := time.Now()
		if timeout > 0 && now.After(end) {
			return l.revs[key], Busy
		}

		// Wait until we are no longer on the given rev.
		if l.revs[key] != rev {
			break
		}

		// Wait for a change.
		lock.wait(timeout > 0, end.Sub(now))
	}

	// Return the new rev.
	return l.revs[key], nil
}

func (l *Local) doWatchFire(key Key, rev Revision, lock *Lock) (Revision, error) {
	// Check our condition.
	if rev != 0 && rev != (l.revs[key]+1) {
		return 0, NotFound
	}

	// Update our rev.
	rev = l.revs[key] + 1
	l.revs[key] = rev

	// Broadcast.
	lock.notify()

	return rev, nil
}

func (l *Local) WatchFire(key Key, rev Revision) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	return l.doWatchFire(key, rev, lock)
}

func (l *Local) Purge(id EphemId) {
	paths := make([]Key, 0)

	// Kill off all ephemeral nodes.
	l.Mutex.Lock()
	for group, members := range l.groups {
		if len((*members)[id]) > 0 {
			paths = append(paths, group)
		}
		delete(*members, id)
	}
	for lock, owners := range l.locks {
		if len((*owners)[id]) > 0 {
			paths = append(paths, lock)
		}
		delete(*owners, id)
	}
	l.Mutex.Unlock()

	// Fire watches.
	for _, path := range paths {
		l.WatchFire(path, 0)
	}
}

func (l *Local) loadRevs() error {
	l.Mutex.Lock()
	defer l.Mutex.Unlock()

	items, err := l.data.List()
	if err != nil {
		return err
	}

	for _, item := range items {
		// Save the revision into our map.
		_, rev, err := l.data.Read(item)
		if err != nil {
			return err
		}
		l.revs[Key(item)] = Revision(rev)
	}

	return nil
}

func NewLocal(backend *storage.Backend) *Local {
	local := new(Local)
	local.data = backend
	err := local.init()
	if err != nil {
		return nil
	}
	return local
}

func (l *Local) init() error {
	l.sync = make(map[Key]*Lock)
	l.revs = make(RevisionMap)
	l.groups = make(map[Key]*EphemeralSet)
	l.locks = make(map[Key]*EphemeralSet)
	return l.loadRevs()
}
