package core

import (
	"net"
	"sync"
        "sync/atomic"
)

var DropLimit = uint32(10)

type Node struct {
	// The node address.
	addr *net.UDPAddr

	// The node id.
	id string

        // The node keys.
        keys []string

	// Active is whether or not this node
	// is in the ring. If active is false
	// and dropped is above our threshold
	// we will just disregard the node.
	active bool

	// The number of messages sent without
	// us hearing a peep back from the node.
	// When this reaches a maximum, we consider
	// the node no longer alive.
	dropped uint32
}

func (n *Node) Id() string {
	return n.id
}

func (n *Node) Keys() []string {
	return nil
}

func (n *Node) Addr() *net.UDPAddr {
	return n.addr
}

func (n *Node) Active() bool {
        return n.active
}

func (n *Node) Dropped() {
        atomic.AddUint32(&n.dropped, 1)
}

func (n *Node) Heartbeat() {
        atomic.StoreUint32(&n.dropped, 0)
}

func (n *Node) Alive() bool {
        return atomic.LoadUint32(&n.dropped) > DropLimit
}

func NewNode(addr *net.UDPAddr, id string) *Node {
	node := new(Node)
        node.addr = addr
	node.id = id
        node.active = false
        node.dropped = 0
	return node
}

type Nodes struct {
	all map[string]*Node
	sync.Mutex
}

func (nodes *Nodes) Heartbeat(addr *net.UDPAddr, id string) {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	node := nodes.all[addr.String()]
	if node == nil {
		node = NewNode(addr, id)
		nodes.all[addr.String()] = node
	}

        node.Heartbeat()
}

func (nodes *Nodes) Active() []*Node {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	active := make([]*Node, 0)
	for _, node := range nodes.all {
		if node.Active() {
			active = append(active, node)
		}
	}

	return active
}

func (nodes *Nodes) Dead() []*Node {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	dead := make([]*Node, 0)
	for _, node := range nodes.all {
		if node.Active() && !node.Alive() {
			dead = append(dead, node)
		}
	}

	return dead
}

func NewNodes() *Nodes {
	nodes := new(Nodes)
	nodes.all = make(map[string]*Node)
	return nodes
}
