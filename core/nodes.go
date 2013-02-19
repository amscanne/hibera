package core

import (
	"net"
	"sync"
)

var DropLimit = uint(10)

type Node struct {
	// The node address.
	addr *net.UDPAddr

	// The node id.
	id string

	// The latest seen version.
	version uint64

	// Active is whether or not this node
	// is in the ring. If active is false
	// and dropped is above our threshold
	// we will just disregard the node.
	active bool

	// The number of messages sent without
	// us hearing a peep back from the node.
	// When this reaches a maximum, we consider
	// the node no longer alive.
	dropped uint

	// The last promise message revision we
	// received from this node.
	promised uint64

	// The last accept message revision we
	// received from this node.
	accepted uint64
}

func (n *Node) Id() string {
	return n.id
}

func (n *Node) Ids() []string {
	return nil
}

func (n *Node) Addr() *net.UDPAddr {
	return n.addr
}

func (n *Node) String() string {
	return n.addr.String()
}

type Nodes struct {
	all map[string]*Node
	sync.Mutex
}

func (nodes *Nodes) markSeen(addr *net.UDPAddr, version uint64, id string) *Node {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	node := nodes.all[addr.String()]
	if node == nil {
		node = new(Node)
		node.id = id
		node.addr = addr
		node.active = false
		node.dropped = 0
		node.promised = 0
		node.accepted = 0
		nodes.all[addr.String()] = node
	} else {
		node.dropped = 0
	}
	if node.version < version {
		node.version = version
	}
	return node
}

func (nodes *Nodes) markDrop(addr *net.UDPAddr) {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	n := nodes.all[addr.String()]
	if n != nil {
		n.dropped += 1
	}
}

func (nodes *Nodes) markDead(dead []string) {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	for _, node := range dead {
		n := nodes.all[node]
		if n != nil {
			n.dropped += 1
		}
	}
}

func (nodes *Nodes) Active() []*Node {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	active := make([]*Node, 0)
	for _, node := range nodes.all {
		if node.active && node.dropped < DropLimit {
			active = append(active, node)
		}
	}
	return active
}

func (nodes *Nodes) Dead() []string {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	dead := make([]string, 0)
	for _, node := range nodes.all {
		if node.active && node.dropped > DropLimit {
			dead = append(dead, node.String())
		}
	}
	return dead
}

func (nodes *Nodes) GenerateUpdate() *ClusterUpdate {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	dead := make([]string, 0)
	unknown := make([]string, 0)
	for _, node := range nodes.all {
		if node.active && node.dropped > DropLimit {
			dead = append(dead, node.String())
		}
		if !node.active && node.dropped < DropLimit {
			unknown = append(unknown, node.String())
		}
	}

	if len(dead) == 0 && len(unknown) == 0 {
		return nil
	}

	return &ClusterUpdate{dead, unknown}
}

func (nodes *Nodes) ApplyUpdate(update *ClusterUpdate) {
	nodes.Mutex.Lock()
	defer nodes.Mutex.Unlock()

	for _, node := range update.add {
		nodes.all[node].active = true
	}
	for _, node := range update.remove {
		delete(nodes.all, node)
	}
}

func NewNodes() *Nodes {
	nodes := new(Nodes)
	nodes.all = make(map[string]*Node)
	return nodes
}
