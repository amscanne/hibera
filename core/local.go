package core

import (
	"errors"
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

var NotFound = errors.New("")
var Busy = errors.New("")

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

type local struct {
	// Synchronization.
	// The global Mutex protects access to
	// the map of all locks. We could easily
	// split this into a more scalable structure
	// if it becomes a clear bottleneck.
	sync map[Key]*Lock
	revs RevisionMap
	sync.Mutex

	// In-memory ephemeral data.
	// (Kept synchronized by other modules).
	keys map[Key]*EphemeralSet

	// Our backend.
	// This is used to store data keys.
	data *storage.Backend
}

func (l *local) lock(key Key) *Lock {
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

func (l *local) DataList() (*[]Key, error) {
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

func (l *local) DataClear() error {
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
		l.EventFire(Key(path), 0)
	}

	return nil
}

func (l *local) SyncMembers(key Key, name SubName, limit uint64) (int, []SubName, Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Lookup the key.
	index := -1
	revmap := l.keys[key]
	if revmap == nil {
		// This is valid, it's an empty key.
		return index, make([]SubName, 0), l.revs[key], nil
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
	current := uint64(0)
	members := make([]SubName, limit, limit)
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
	for i, current := range members {
		if current == name {
			index = i
		}
	}

	// Return the members and rev.
	return index, members, l.revs[key], nil
}

func (l *local) SyncJoin(id EphemId, key Key, name SubName, limit uint, timeout uint) (int, Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Go through in a safe way.
	index := -1
	start := time.Now()
	end := start.Add(time.Duration(timeout) * time.Millisecond)
	var revmap *EphemeralSet
	for {
		now := time.Now()
		if timeout > 0 && now.After(end) {
			return index, 0, Busy
		}

		// Lookup the key.
		revmap = l.keys[key]
		if revmap == nil {
			newmap := make(EphemeralSet)
			revmap = &newmap
			l.keys[key] = revmap
		}

		// Lookup the key and see if we're a member.
		_, present := (*revmap)[id]
		if !present {
			(*revmap)[id] = make(NameMap)
		}

		// Check that we're still not there already.
		if (*revmap)[id][name] != 0 {
			return index, 0, Busy
		}

		// Count the number of holders.
		members := uint(0)
		for _, names := range *revmap {
			members += uint(len(names))
		}
		if limit == 0 || members < limit {
			// We will be the Nth member.
			index = int(members)
			break
		}

		// Wait for a change.
		lock.wait(timeout > 0, time.Duration(0))
	}

	// Join and fire.
	rev, err := l.doEventFire(key, 0, lock)
	(*revmap)[id][name] = rev
	return index, rev, err
}

func (l *local) SyncLeave(id EphemId, key Key, name SubName) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Lookup the key and see if we're a member.
	revmap := l.keys[key]
	if revmap == nil || (*revmap)[id][name] == 0 {
		return 0, NotFound
	}

	// Leave the key.
	_, present := (*revmap)[id]
	if !present {
		(*revmap)[id] = make(NameMap)
	}
	delete((*revmap)[id], name)
	if len(*revmap) == 0 {
		delete(l.keys, key)
	}

	return l.doEventFire(key, 0, lock)
}

func (l *local) DataGet(key Key) ([]byte, Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	value, rev, err := l.data.Read(string(key))
	return value, Revision(rev), err
}

func (l *local) DataSet(key Key, value []byte, rev Revision) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Check the revisions.
	rev, err := l.doEventFire(key, rev, lock)
	if err != nil {
		return rev, err
	}

	err = l.data.Write(string(key), value, uint64(rev))
	return Revision(rev), err
}

func (l *local) DataRemove(key Key, rev Revision) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Check the revisions.
	rev, err := l.doEventFire(key, rev, lock)
	if err != nil {
		return rev, err
	}

	// Delete and set the revision.
	return 0, l.data.Delete(string(key))
}

func (l *local) EventWait(id EphemId, key Key, rev Revision, timeout uint64) (Revision, error) {
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

func (l *local) doEventFire(key Key, rev Revision, lock *Lock) (Revision, error) {
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

func (l *local) EventFire(key Key, rev Revision) (Revision, error) {
	lock := l.lock(key)
	defer lock.unlock()

	return l.doEventFire(key, rev, lock)
}

func (l *local) Purge(id EphemId) {
	paths := make([]Key, 0)

	// Kill off all ephemeral nodes.
	l.Mutex.Lock()
	for key, members := range l.keys {
		if len((*members)[id]) > 0 {
			paths = append(paths, key)
		}
		delete(*members, id)
	}
	l.Mutex.Unlock()

	// Fire watches.
	for _, path := range paths {
		l.EventFire(path, 0)
	}
}

func (l *local) loadRevs() error {
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

func NewLocal(backend *storage.Backend) *local {
	local := new(local)
	local.data = backend
	err := local.init()
	if err != nil {
		return nil
	}
	return local
}

func (l *local) init() error {
	l.sync = make(map[Key]*Lock)
	l.revs = make(RevisionMap)
	l.keys = make(map[Key]*EphemeralSet)
	return l.loadRevs()
}
