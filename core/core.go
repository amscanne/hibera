package core

import (
	"log"
	"sync"
	"hibera/storage"
)

type ClientId uint64

type Core struct {
	// The local datastore and proxy.
	local *Local
	proxy *Proxy

	// All active clients.
	clients map[ClientId]*Client
	users   map[string]*User
	nextid  ClientId
	lock    *sync.Mutex

	cluster *Cluster
}

type Client struct {
	ClientId
	addr string
	user *User
	*Core
}

type User struct {
	ClientId
	userid string
	uuid   string
	refs   int
}

func (c *Client) Name(name string) string {
	if name != "" {
		return name
	}
	if c.user != nil {
		return c.user.uuid
	}
	return c.addr
}

type Info struct{}

func (c *Core) genid() ClientId {
	id := c.nextid
	c.nextid += 1
	return id
}

func (c *Core) NewClient(addr string) *Client {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Generate client with no user, and
	// a straight-forward id. The user can
	// associate some client-id with their
	// active connection during lookup.
	client := &Client{c.genid(), addr, nil, c}
	c.clients[client.ClientId] = client
	return client
}

func (c *Core) FindClient(id ClientId, userid string) *Client {
	c.lock.Lock()
	defer c.lock.Unlock()

	client := c.clients[id]
	if client == nil {
		return nil
	}

	// Create the user if it doesnt exist.
	if userid != "" {
		client.user = c.users[userid]

		if client.user == nil {
			uuid, err := storage.Uuid()
			if err != nil {
				// Something's really wrong here.
				// We'll assume the server is blasting
				// messages everything when things like
				// this are happening.
				return nil
			}

			// Create and initialize a new user.
			// This will be the common user for all requests that use the
			// userid string -- although this string will not necessarily
			// correspond directly to the uuid we generation for safety.
			client.user = new(User)
			client.user.ClientId = c.genid()
			client.user.userid = userid
			client.user.uuid = uuid
			client.user.refs = 1
			c.users[userid] = client.user
		} else {
			// Bump up the reference.
			client.user.refs += 1
		}
	}

	return c.clients[id]
}

func (c *Core) DropClient(id ClientId) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Lookup this client.
	client := c.clients[id]
	if client == nil {
		return
	}

	// Shuffle userid mappings.
	if client.user != nil {
		client.user.refs -= 1
		if client.user.refs == 0 {
			// Remove the user from the map and
			// purge all related keys from the
			// underlying storage system.
			delete(c.users, client.user.userid)
			c.local.Purge(client.user.ClientId)
			client.user = nil
		}
	}

	// Purge the client.
	delete(c.clients, id)
	c.local.Purge(id)
}

func NewCore(domain string, keys uint, backend *storage.Backend) *Core {
	core := new(Core)
	core.local = NewLocal(backend)
	core.clients = make(map[ClientId]*Client)
	core.users = make(map[string]*User)
	core.lock = new(sync.Mutex)
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
