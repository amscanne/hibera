package core

import (
	"hibera/storage"
)

type Client struct {
	id uint64
}

type Ref struct {
	name  string
	owner *Node
}

type Core struct {
	data    *storage.Backend
	groups  map[string][]Ref
	locks   map[string]Ref
	watches map[string]*Client

	cluster *Cluster
	clients uint64
}

type Info struct {
}

func (c *Client) Id() uint64 {
	return c.id
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

func (c *Core) LockOwner(key string) (string, uint64, error) {
	return "", 0, nil
}

func (c *Core) LockAcquire(client *Client, key string, timeout uint64, name string) (uint64, error) {
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
	id := c.clients
	c.clients += 1
	return &Client{id}
}

func (c *Core) FindClient(id uint64) *Client {
	return nil
}

func NewCore(domain string, seeds []string, backend *storage.Backend) *Core {
	core := new(Core)
	core.data = backend
	return core
}
