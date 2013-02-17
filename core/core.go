package core

import (
    "log"
	"hibera/storage"
)

type ClientId uint64

type Core struct {
	data    *storage.Backend
        //master  *Master
        //proxy   *Proxy

	groups  map[string]map[ClientId]string
	locks   map[string]map[ClientId]string
	watches map[string]map[ClientId]bool

        clients map[ClientId]*Client
	cluster *Cluster
	ClientId
}

type Client struct {
	ClientId
        *Core
}

type Info struct {
}

func (c *Core) Info() (Info, error) {
    return Info{}, nil
}

func (c *Core) DataList() ([]string, error) {
	return nil, nil
}

func (c *Core) DataClear() error {
	return nil
}

func (c *Core) LockOwners(key string) ([]string, uint64, error) {
	return nil, 0, nil
}

func (c *Core) LockAcquire(client *Client, key string, timeout uint64, name string, limit uint64) (uint64, error) {
	return 0, nil
}

func (c *Core) LockRelease(client *Client, key string) (uint64, error) {
	return 0, nil
}

func (c *Core) GroupMembers(group string, name string, limit uint64) ([]string, uint64, error) {
	return nil, 0, nil
}

func (c *Core) GroupJoin(client *Client, group string, name string) (uint64, error) {
	return 0, nil
}

func (c *Core) GroupLeave(client *Client, group string, name string) (uint64, error) {
	return 0, nil
}

func (c *Core) DataGet(key string) ([]byte, uint64, error) {
	return nil, 0, nil
}

func (c *Core) DataSet(key string, value []byte, rev uint64) (uint64, error) {
	return 0, nil
}

func (c *Core) DataRemove(key string, rev uint64) (uint64, error) {
	return 0, nil
}

func (c *Core) WatchWait(client *Client, key string, rev uint64) (uint64, error) {
	return 0, nil
}

func (c *Core) WatchFire(key string, rev uint64) (uint64, error) {
	return 0, nil
}

func (c *Core) NewClient() *Client {
	id := c.ClientId
	c.ClientId += 1
	return &Client{id, c}
}

func (c *Core) FindClient(id ClientId) *Client {
    return c.clients[id]
}

func (c *Core) fireWatches(path []string) {
}

func (c *Core) DropClient(id ClientId) {
    paths := make([]string, 0)

    // Kill off all ephemeral nodes.
    for group, members := range c.groups {
        if len(members[id]) > 0 {
            paths = append(paths, group)
        }
        delete(members, id)
    }
    for lock, owners := range c.locks {
        if len(owners[id]) > 0 {
            paths = append(paths, lock)
        }
        delete(owners, id)
    }
    for _, clients := range c.watches {
        delete(clients, id)
    }

    // Remove the client.
    delete(c.clients, id)

    // Fire watches.
    c.fireWatches(paths)
}

func NewCore(domain string, keys uint, backend *storage.Backend) *Core {
	core := new(Core)
	core.data = backend
        ids, err := core.data.LoadIds(keys)
        if err != nil {
            log.Fatal("Unable to load ring: ", err)
            return nil
        }
	core.cluster = NewCluster(domain, ids)
	return core
}
