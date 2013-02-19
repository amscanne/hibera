package core

import (
	"net"
	"hibera/storage"
)

type Key string
type Revision uint64
type EphemId uint64
type SubName string

var DefaultKeys = uint(128)

type Redirect struct {
	URL string
}

func NewRedirect(URL string) *Redirect {
	return &Redirect{URL}
}

func (r Redirect) Error() string {
	return r.URL
}

type Info struct{}

type Cluster struct {
	// Our internal id.
	id string

	// Our local database.
	// Some of these functions are
	// exported and used directly by
	// called (HTTPServer).
	*local

	// Our node map.
	// This is computed based on the
	// gossip server and represents
	// consensus of the cluster.
	*nodes

	// Our ring.
	// This routes requests, etc.
	// We put our domain there and the
	// domain of all nodes in the system.
	// We update it whenever the nodes
	// change.
	*ring
}

func (c *Cluster) Id() string {
	return c.id
}

func NewCluster(backend *storage.Backend, domain string, ids []string) *Cluster {
	uuid, err := storage.Uuid()
	if err != nil {
		return nil
	}

	c := new(Cluster)
	c.id = uuid
	c.nodes = NewNodes()
	c.local = NewLocal(backend)
	c.ring = NewRing(c.nodes)

	return c
}

func (c *Cluster) Version() uint64 {
	return 0
}

func (c *Cluster) ProposeVersion() uint64 {
	return 0
}

func (c *Cluster) IsProposePhase() bool {
	return false
}

func (c *Cluster) IsAcceptPhase() bool {
	return false
}

func (c *Cluster) PaxosNodes() []*net.UDPAddr {
	return nil
}

func (c *Cluster) AllNodes() []*net.UDPAddr {
	return nil
}

func (c *Cluster) DeadNodes() []*net.UDPAddr {
	return nil
}

func (c *Cluster) OnDrop(addr *net.UDPAddr) {
	c.nodes.markDrop(addr)
}

func (c *Cluster) OnPing(addr *net.UDPAddr, version uint64, id string, dead []string) {
	c.nodes.markSeen(addr, version, id)
	c.nodes.markDead(dead)
}

func (c *Cluster) OnPong(addr *net.UDPAddr, version uint64, id string, dead []string) {
	c.nodes.markSeen(addr, version, id)
	c.nodes.markDead(dead)
}

func (c *Cluster) OnPropose(addr *net.UDPAddr, version uint64) {
}

func (c *Cluster) OnPromise(addr *net.UDPAddr, version uint64) {
	// if c.nodes.majorityPromised(version, len(c.PaxosNodes()) {
	// }
}

func (c *Cluster) OnNoPromise(addr *net.UDPAddr, version uint64) {
	// Uh-oh. Someone has a newer version than us.
	// We'll rely on the other parts of the system
	// to suck down the latest cluster and we'll stop
	// participating in Paxos.
}

func (c *Cluster) OnAccept(addr *net.UDPAddr, version uint64) {
	// if c.nodes.majorityAccepted(version, len(c.PaxosNodes()) {
	// }
}

func (c *Cluster) OnAccepted(addr *net.UDPAddr, version uint64) {
}

func (c *Cluster) Check(key Key) error {
	return Redirect{""}
}
