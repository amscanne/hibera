package core

import (
	"sync"
	"errors"
	"hibera/storage"
)

type Set map[string]bool
type RevMap map[ClientId]Set
type RevSet map[ClientId]bool

type Lock struct {
	sync.Mutex
	*sync.Cond
}

func (l *Lock) lock() {
	l.Mutex.Lock()
}

func (l *Lock) unlock() {
	l.Mutex.Unlock()
}

func (l *Lock) wait() {
	l.Cond.Wait()
}

func (l *Lock) notify() {
	l.Cond.Broadcast()
}

func NewLock() *Lock {
	lock := new(Lock)
	lock.Cond = sync.NewCond(&lock.Mutex)
	return lock
}

type Local struct {
	// Our backend.
	data *storage.Backend

	// Synchronization.
	sync map[string]*Lock
	revs map[string]uint64
	sync.Mutex

	// In-memory data.
	groups  map[string]*RevMap
	locks   map[string]*RevMap
	watches map[string]*RevSet
}

func (l *Local) lock(key string) *Lock {
	l.Mutex.Lock()
	lock := l.sync[key]
	if lock == nil {
		lock = NewLock()
		l.sync[key] = lock
	}
	lock.lock()
	l.Mutex.Unlock()
	return lock
}

var NOT_FOUND = errors.New("NOT FOUND")
var BUSY = errors.New("BUSY")

func (l *Local) Info() (Info, error) {
	return Info{}, nil
}

func (l *Local) DataList() ([]string, error) {
	return l.data.List()
}

func (l *Local) DataClear() error {
	return l.data.Clear()
}

func (l *Local) LockOwners(key string) ([]string, uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Get the lock.
	revmap := l.locks[key]
	if revmap == nil {
		return nil, 0, nil
	}

	// Lookup all the owners.
	owners := make([]string, 0)
	for _, names := range *revmap {
		for name := range names {
			owners = append(owners, name)
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
		newmap := make(RevMap)
		revmap = &newmap
		l.locks[key] = revmap
	}

	// Already held?
	_, present := (*revmap)[client.ClientId]
	if !present {
		(*revmap)[client.ClientId] = make(Set)
	}
	if (*revmap)[client.ClientId][name] {
		return 0, BUSY
	}

	// Count the number of holders.
	holders := uint64(0)
	for _, names := range *revmap {
		holders += uint64(len(names))
	}
	if holders >= limit {
		return 0, BUSY
	}

	// Acquire it.
	(*revmap)[client.ClientId][name] = true
	return l.doWatchFire(key, 0, lock)
}

func (l *Local) LockRelease(client *Client, key string) (uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	// Get the lock.
	revmap := l.locks[key]
	if revmap == nil || !(*revmap)[client.ClientId][key] {
		return 0, NOT_FOUND
	}

	// Release it.
	_, present := (*revmap)[client.ClientId]
	if !present {
		(*revmap)[client.ClientId] = make(Set)
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
		return nil, 0, nil
	}

	// Assembly a list of members.
	members := make([]string, 0, limit)
	count := uint64(0)
	for _, names := range *revmap {
		for name, _ := range names {
			members = append(members, name)
			count += 1
			if limit > 0 && count >= limit {
				break
			}
		}
	}

	// Return the members and rev.
	return members, l.revs[group], nil
}

func (l *Local) GroupJoin(client *Client, group string, name string) (uint64, error) {
	lock := l.lock(group)
	defer lock.unlock()

	// Lookup the group.
	revmap := l.groups[group]
	if revmap == nil {
		newmap := make(RevMap)
		revmap = &newmap
		l.groups[group] = revmap
	}

	// Lookup the group and see if we're a member.
	_, present := (*revmap)[client.ClientId]
	if !present {
		(*revmap)[client.ClientId] = make(Set)
	}
	if (*revmap)[client.ClientId][name] {
		return 0, BUSY
	}

	// Join and fire.
	(*revmap)[client.ClientId][name] = true
	return l.doWatchFire(group, 0, lock)
}

func (l *Local) GroupLeave(client *Client, group string, name string) (uint64, error) {
	lock := l.lock(group)
	defer lock.unlock()

	// Lookup the group and see if we're a member.
	revmap := l.groups[group]
	if revmap == nil || !(*revmap)[client.ClientId][name] {
		return 0, NOT_FOUND
	}

	// Leave the group.
	_, present := (*revmap)[client.ClientId]
	if !present {
		(*revmap)[client.ClientId] = make(Set)
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

func (l *Local) WatchWait(client *Client, key string, rev uint64) (uint64, error) {
	lock := l.lock(key)
	defer lock.unlock()

	for {
		// Wait until we are no longer on the given rev.
		if l.revs[key] != rev {
			break
		}
		lock.wait()
	}

	// Return the new rev.
	return l.revs[key], nil
}

func (l *Local) doWatchFire(key string, rev uint64, lock *Lock) (uint64, error) {
	// Check our condition.
	if rev != 0 && rev != (l.revs[key]+1) {
		return 0, NOT_FOUND
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
	l.Mutex.Lock()
	defer l.Mutex.Unlock()

	paths := make([]string, 0)

	// Kill off all ephemeral nodes.
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
	for _, clients := range l.watches {
		delete(*clients, id)
	}

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
	local.groups = make(map[string]*RevMap)
	local.locks = make(map[string]*RevMap)
	local.watches = make(map[string]*RevSet)
	err := local.loadRevs()
	if err != nil {
		return nil
	}
	return local
}
