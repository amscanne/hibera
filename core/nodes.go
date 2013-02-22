package core

import (
	"net"
	"sync"
        "sync/atomic"
        "hibera/storage"
)

var DropLimit = uint32(10)

type Node struct {
	// The node address.
	Addr *net.UDPAddr

	// The node id.
	Id string

        // The node keys.
        Keys []string

	// Active is whether or not this node
	// is in the ring. If active is false
	// and dropped is above our threshold
	// we will just disregard the node.
	Active bool

	// The number of messages sent without
	// us hearing a peep back from the node.
	// When this reaches a maximum, we consider
	// the node no longer alive.
	Dropped uint32
}

func (n *Node) IncDropped() {
        atomic.AddUint32(&n.Dropped, 1)
}

func (n *Node) RstDropped () {
        atomic.StoreUint32(&n.Dropped, 0)
}

func (n *Node) Alive() bool {
        return atomic.LoadUint32(&n.Dropped) > DropLimit
}

func NewNode(addr *net.UDPAddr, id string, keys []string) *Node {
	node := new(Node)
        node.Addr = addr
	node.Id = id
        node.Keys = keys
        node.Active = false
        node.Dropped = 0
	return node
}

type Nodes struct {
	all map[string]*Node
	sync.Mutex
}

func (nodes *Nodes) Heartbeat(addr *net.UDPAddr, id string) *Node {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	node := nodes.all[addr.String()]
	if node != nil && node.Id == id {
            // NOTE: We don't heartbeat nodes whose Ids
            // do not match what we expect. Eventually they
            // will be removed and replaced by other nodes,
            // but we allow that to happen organically.
            node.RstDropped()
	}

        return node
}

func (nodes *Nodes) Add(addr *net.UDPAddr, id string, keys []string) *Node {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

        if nodes.all[addr.String()] == nil {
            node := NewNode(addr, id,keys)
	    nodes.all[addr.String()] = node
            return node
        }

        return nil
}

func (nodes *Nodes) Active() []*Node {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	active := make([]*Node, 0)
	for _, node := range nodes.all {
		if node.Active {
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
		if node.Active && !node.Alive() {
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

func NewTestNodes(n uint, k uint) *Nodes {
	nodes := NewNodes()
        for i := uint(0); i < n; i += 1 {
            uuid, err := storage.Uuid()
            if err != nil {
                return nil
            }
            uuids, err := storage.Uuids(k)
            if err != nil {
                return nil
            }

            // Create an active node.
            node := NewNode(nil, uuid, uuids)
            node.Active = true
            nodes.all[string(i)] = node
        }
        return nodes
}
