package core

import (
	"log"
	"hibera/storage"
)

type ClientId uint64

type Core struct {
	// The local datastore and proxy.
	local *Local
	proxy *Proxy

	// All active clients.
	clients map[ClientId]*Client
	ClientId

	cluster *Cluster
}

type Client struct {
	ClientId
	addr string
	*Core
}

func (c *Client) Name(name string) string {
	if name != "" {
		return name
	}
	return c.addr
}

type Info struct{}

func (c *Core) NewClient(addr string) *Client {
	id := c.ClientId
	client := &Client{id, addr, c}
	c.ClientId += 1
	c.clients[id] = client
	return client
}

func (c *Core) FindClient(id ClientId) *Client {
	return c.clients[id]
}

func (c *Core) DropClient(id ClientId) {
	// Remove the client.
	delete(c.clients, id)

	// Purge it.
	c.local.Purge(id)
}

func NewCore(domain string, keys uint, backend *storage.Backend) *Core {
	core := new(Core)
	core.local = NewLocal(backend)
	core.clients = make(map[ClientId]*Client)
	ids, err := backend.LoadIds(keys)
	if err != nil {
		log.Fatal("Unable to load ring: ", err)
		return nil
	}
	core.cluster = NewCluster(domain, ids)
	return core
}

func (c *Core) Info() (Info, error) {
	return c.local.Info()
}

func (c *Core) DataList() ([]string, error) {
	return c.local.DataList()
}

func (c *Core) DataClear() error {
	return c.local.DataClear()
}

func (c *Core) LockOwners(client *Client, key string, name string) ([]string, uint64, error) {
	return c.local.LockOwners(key, client.Name(name))
}

func (c *Core) LockAcquire(client *Client, key string, timeout uint64, name string, limit uint64) (uint64, error) {
	return c.local.LockAcquire(client, key, timeout, client.Name(name), limit)
}

func (c *Core) LockRelease(client *Client, key string) (uint64, error) {
	return c.local.LockRelease(client, key)
}

func (c *Core) GroupMembers(client *Client, group string, name string, limit uint64) ([]string, uint64, error) {
	return c.local.GroupMembers(group, client.Name(name), limit)
}

func (c *Core) GroupJoin(client *Client, group string, name string) (uint64, error) {
	return c.local.GroupJoin(client, group, client.Name(name))
}

func (c *Core) GroupLeave(client *Client, group string, name string) (uint64, error) {
	return c.local.GroupLeave(client, group, client.Name(name))
}

func (c *Core) DataGet(key string) ([]byte, uint64, error) {
	return c.local.DataGet(key)
}

func (c *Core) DataSet(key string, value []byte, rev uint64) (uint64, error) {
	return c.local.DataSet(key, value, rev)
}

func (c *Core) DataRemove(key string, rev uint64) (uint64, error) {
	return c.local.DataRemove(key, rev)
}

func (c *Core) WatchWait(client *Client, key string, rev uint64, timeout uint64) (uint64, error) {
	return c.local.WatchWait(client, key, rev, timeout)
}

func (c *Core) WatchFire(key string, rev uint64) (uint64, error) {
	return c.local.WatchFire(key, rev)
}
