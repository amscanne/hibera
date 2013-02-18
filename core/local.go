package core

import (
	"sync"
	"time"
	"hibera/storage"
)

type RevSet map[string]uint64
type ClientMap map[ClientId]RevSet

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
	sync map[string]*Lock
	revs map[string]uint64
	sync.Mutex

	// In-memory data.
        // (Kept synchronized by other modules).
	groups map[string]*ClientMap
	locks map[string]*ClientMap
}

func (l *Local) lock(key string) *Lock {
	l.Mutex.Lock()
	defer l.Mutex.Unlock()
	lock := l.sync[key]
	if lock == nil {
		lock = NewLock()
		l.sync[key] = lock
	}
	lock.lock()
	return lock
}

func (l *Local) Info() (Info, error) {
	return Info{}, nil
}

func (l *Local) DataList() ([]string, error) {
	return l.data.List()
}

func (l *Local) DataClear() error {
	return l.data.Clear()
}

func (l *Local) LockOwners(key string, name string) ([]string, uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Get the lock.
	revmap := l.locks[key]
	if revmap == nil {
                // This is valid, it's an unheld lock.
		return make([]string, 0), l.revs[key], nil
	}

	// Lookup all the owners.
	owners := make([]string, 0)
	for _, set := range *revmap {
		for ownername := range set {
			if ownername == name {
				owners = append(owners, "*")
			} else {
				owners = append(owners, ownername)
			}
		}
	}

	// Return the info.
	return owners, l.revs[key], nil
}

func (l *Local) LockAcquire(client *Client, key string, timeout uint64, name string, limit uint64) (uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Get the lock.
	revmap := l.locks[key]
	if revmap == nil {
		newmap := make(ClientMap)
		revmap = &newmap
		l.locks[key] = revmap
	}

	// Already held?
	_, present := (*revmap)[client.ClientId]
	if !present {
		(*revmap)[client.ClientId] = make(RevSet)
	}
	if (*revmap)[client.ClientId][name] != 0 {
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
	(*revmap)[client.ClientId][name] = rev
        return rev, err
}

func (l *Local) LockRelease(client *Client, key string) (uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Get the lock.
	revmap := l.locks[key]
	if revmap == nil || (*revmap)[client.ClientId][key] == 0 {
		return 0, NotFound
	}

	// Release it.
	_, present := (*revmap)[client.ClientId]
	if !present {
		(*revmap)[client.ClientId] = make(RevSet)
	}
	delete((*revmap)[client.ClientId], key)
	if len(*revmap) == 0 {
		delete(l.locks, key)
	}

	return l.doWatchFire(key, 0, lock)
}

func (l *Local) GroupMembers(group string, name string, limit uint64) ([]string, uint64, error) {
	lock := l.lock(group)
	defer lock.unlock()

	// Lookup the group.
	revmap := l.groups[group]
	if revmap == nil {
                // This is valid, it's an empty group.
		return make([]string, 0), l.revs[group], nil
	}

	// Assembly a list of members.
        indices := make([]uint64, 0)
	members := make(map[uint64]string, 0)
	for _, set := range *revmap {
		for membername, revjoined := range set {
                        indices = append(indices, revjoined)
			if membername == name {
				members[revjoined] = "*"
			} else {
				members[revjoined] = membername
			}
		}
	}

        if len(indices) > int(limit) {
            indices = indices[0:limit]
        }
        results := make([]string, 0, len(indices))
        for _, index := range indices {
            results = append(results, members[uint64(index)])
        }

	// Return the members and rev.
	return results, l.revs[group], nil
}

func (l *Local) GroupJoin(client *Client, group string, name string) (uint64, error) {
	lock := l.lock(group)
	defer lock.unlock()

	// Lookup the group.
	revmap := l.groups[group]
	if revmap == nil {
		newmap := make(ClientMap)
		revmap = &newmap
		l.groups[group] = revmap
	}

	// Lookup the group and see if we're a member.
	_, present := (*revmap)[client.ClientId]
	if !present {
		(*revmap)[client.ClientId] = make(RevSet)
	}
	if (*revmap)[client.ClientId][name] != 0 {
		return 0, Busy
	}

	// Join and fire.
	rev, err := l.doWatchFire(group, 0, lock)
	(*revmap)[client.ClientId][name] = rev
        return rev, err
}

func (l *Local) GroupLeave(client *Client, group string, name string) (uint64, error) {
	lock := l.lock(group)
	defer lock.unlock()

	// Lookup the group and see if we're a member.
	revmap := l.groups[group]
	if revmap == nil || (*revmap)[client.ClientId][name] == 0 {
		return 0, NotFound
	}

	// Leave the group.
	_, present := (*revmap)[client.ClientId]
	if !present {
		(*revmap)[client.ClientId] = make(RevSet)
	}
	delete((*revmap)[client.ClientId], name)
	if len(*revmap) == 0 {
		delete(l.groups, group)
	}

	return l.doWatchFire(group, 0, lock)
}

func (l *Local) DataGet(key string) ([]byte, uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	return l.data.Read(key)
}

func (l *Local) DataSet(key string, value []byte, rev uint64) (uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Check the revisions.
	rev, err := l.doWatchFire(key, rev, lock)
	if err != nil {
		return rev, err
	}

	return rev, l.data.Write(key, value, rev)
}

func (l *Local) DataRemove(key string, rev uint64) (uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Check the revisions.
	rev, err := l.doWatchFire(key, rev, lock)
	if err != nil {
		return rev, err
	}

	// Delete and set the revision.
	return 0, l.data.Delete(key)
}

func (l *Local) WatchWait(client *Client, key string, rev uint64, timeout uint64) (uint64, error) {
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

func (l *Local) doWatchFire(key string, rev uint64, lock *Lock) (uint64, error) {
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

func (l *Local) WatchFire(key string, rev uint64) (uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	return l.doWatchFire(key, rev, lock)
}

func (l *Local) Purge(id ClientId) {
	paths := make([]string, 0)

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
		l.revs[item] = rev
	}

	return nil
}

func NewLocal(backend *storage.Backend) *Local {
	local := new(Local)
	local.data = backend
	local.sync = make(map[string]*Lock)
	local.revs = make(map[string]uint64)
	local.groups = make(map[string]*ClientMap)
	local.locks = make(map[string]*ClientMap)
	err := local.loadRevs()
	if err != nil {
		return nil
	}
	return local
}
