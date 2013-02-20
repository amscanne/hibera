package core

import (
	"net"
	"sync"
	"hibera/storage"
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

type ClusterUpdate struct {
	// Nodes to be added/activated in the cluster.
	// These strings store the nodeIds, not the addresses.
	add []string

	// Nodes to be dropped/deactivated in the cluster.
	// Similarly, nodes are removed by nodeId, not the address.
	remove []string
}

type Cluster struct {
	// This node.
	self *Node

	// The cluster revision.
	// When the cluster has not yet been activated (i.e.
	// we've just started and not yet seen any active nodes
	// or participated in a paxos round) then this number
	// will be zero.
	revision uint64

	// Our local datastore.
	// Some of these functions are exported and used directly
	// by the calling servers (HTTPServer).
	*Data

	// Our node map.
	// This represents the consensus of what the cluster looks
	// like and is computed by the changing versions above.
	// Similarly, some functions are exported and used directly
	// by the calling servers (GossipServer).
	*Nodes

	// Our ring.
	// This routes requests, etc.
	// This is computed each round based on the node map above.
	*ring

        sync.Mutex
}

func (c *Cluster) Id() string {
	return c.self.Id()
}

func NewCluster(backend *storage.Backend, domain string, ids []string) *Cluster {
	uuid, err := storage.Uuid()
	if err != nil {
		return nil
	}

	c := new(Cluster)
	c.self = NewNode(nil, uuid)
	c.Nodes = NewNodes()
	c.Data = NewData(backend)
	c.ring = NewRing(c.self, c.Nodes)

	return c
}

func (c *Cluster) Revision() uint64 {
	return c.revision
}

func (c *Cluster) IsActive() bool {
	return c.revision != 0
}

func (c *Cluster) Activate(addr *net.UDPAddr, revision uint64) {
        c.Mutex.Lock()
        defer c.Mutex.Unlock()

	// Mark this cluster version.
	if c.revision == 0 {
		c.revision = revision
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
