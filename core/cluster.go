package core

import (
	"net"
        "bytes"
	"hibera/storage"
	"encoding/json"
)

type Key string
type Revision uint64
type EphemId uint64
type SubName string

var DefaultKeys = uint(128)
var HiberaKey = Key("hibera")

type Redirect struct {
	URL string
}

func NewRedirect(URL string) *Redirect {
	return &Redirect{URL}
}

func (r Redirect) Error() string {
	return r.URL
}

type Cluster struct {
	// This node.
	self *Node

	// The cluster revision.
	// When the cluster has not yet been activated (i.e.
	// we've just started and not yet seen any active nodes
	// or participated in a paxos round) then this number
	// will be zero.
	Revision uint64

	// Our local datastore.
	// Some of these functions are exported and used directly
	// by the calling servers (HTTPServer).
        // NOTE: Not exported so that it is not serialized.
	*data

	// Our node map.
	// This represents the consensus of what the cluster looks
	// like and is computed by the changing versions above.
	// Similarly, some functions are exported and used directly
	// by the calling servers (GossipServer).
	*Nodes

	// Our ring.
	// This routes requests, etc.
	// This is computed each round based on the node map above.
        // NOTE: Not exported so that it is not serialized.
	*ring
}

func (c *Cluster) Id() string {
	return c.self.Id
}

func (c *Cluster) Info() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	err := enc.Encode(&c)
	return buf.Bytes(), err
}

func NewCluster(backend *storage.Backend, domain string, ids []string) *Cluster {
	uuid, err := storage.Uuid()
	if err != nil {
		return nil
	}

	c := new(Cluster)
	c.Nodes = NewNodes()
        c.self = c.Nodes.Add(nil, uuid, ids)
	c.data = NewData(backend)
	c.ring = NewRing(c.self, c.Nodes)

	return c
}

func (c *Cluster) IsActive() bool {
	return c.Revision != 0
}

func (c *Cluster) Activate(addr *net.UDPAddr, revision uint64) {
	// Mark this cluster version.
	if c.Revision == 0 {
		c.Revision = revision
	}
}

func (c *Cluster) Heartbeat(addr *net.UDPAddr, id string, revision uint64, dead []string) {
        // Update our nodes.
        c.Nodes.Heartbeat(addr, id)

	// Activate if necessary.
	if revision > 0 {
		c.Activate(addr, revision)
	}
}

func (c *Cluster) Check(key Key) error {
	return Redirect{""}
}
