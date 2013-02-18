package core

import (
    "hibera/storage"
)

var DEFAULT_KEYS = uint(128)

type Info struct {
}

type Cluster struct {
        // The local database.
	local *Local
}

func NewCluster(backend *storage.Backend, domain string, ids []string) *Cluster {
        cluster := new(Cluster)
	cluster.local = NewLocal(backend)
	return new(Cluster)
}

func filterList(items *[]SubName, match SubName, replace SubName) {
    for i, item := range *items {
        if item  == match {
            (*items)[i] = replace
        }
    }
}

func (c *Cluster) DataList() (*[]Key, error) {
    return c.local.DataList()
}

func (c *Cluster) DataClear() error {
    return c.local.DataClear()
}

func (c *Cluster) LockOwners(key Key, name SubName) ([]SubName, Revision, error) {
    owners, rev, err := c.local.LockOwners(key, name)
    filterList(&owners, name, "*")
    return owners, rev, err
}

func (c *Cluster) LockAcquire(id EphemId, key Key, timeout uint64, name SubName, limit uint64) (Revision, error) {
    return c.local.LockAcquire(id, key, timeout, name, limit)
}

func (c *Cluster) LockRelease(id EphemId, key Key, name SubName) (Revision, error) {
    return c.local.LockRelease(id, key, name)
}

func (c *Cluster) GroupJoin(id EphemId, group Key, name SubName) (Revision, error) {
    return c.local.GroupJoin(id, group, name)
}

func (c *Cluster) GroupMembers(group Key, name SubName, limit uint64) ([]SubName, Revision, error) {
    members, rev, err := c.local.GroupMembers(group, name, limit)
    filterList(&members, name, "*")
    return members, rev, err
}

func (c *Cluster) GroupLeave(id EphemId, group Key, name SubName) (Revision, error) {
    return c.local.GroupLeave(id, group, name)
}

func (c *Cluster) DataGet(key Key) ([]byte, Revision, error) {
    return c.local.DataGet(key)
}

func (c *Cluster) DataSet(key Key, value []byte, rev Revision) (Revision, error) {
    return c.local.DataSet(key, value, rev)
}

func (c *Cluster) DataRemove(key Key, rev Revision) (Revision, error) {
    return c.local.DataRemove(key, rev)
}

func (c *Cluster) WatchWait(id EphemId, key Key, rev Revision, timeout uint64) (Revision, error) {
    return c.local.WatchWait(id, key, rev, timeout)
}

func (c *Cluster) WatchFire(key Key, rev Revision) (Revision, error) {
    return c.local.WatchFire(key, rev)
}

func (c *Cluster) Purge(id EphemId) {
    c.local.Purge(id)
}
