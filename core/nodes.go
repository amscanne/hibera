package core

import (
	"net"
        "sync"
)

var DropLimit = uint(10)

type node struct {
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

func (n *node) Id() string {
	return ""
}

func (n *node) Ids() []string {
	return nil
}

type nodes struct {
	nodes map[string]*node
        sync.Mutex
}

func (nds *nodes) markSeen(addr *net.UDPAddr, version uint64, id string) *node {
    nds.Mutex.Lock()
    defer nds.Mutex.Unlock()

	n := nds.nodes[addr.String()]
	if n == nil {
		n = new(node)
		n.id = id
		n.addr = addr
		n.active = false
		n.dropped = 0
		n.promised = 0
		n.accepted = 0
		nds.nodes[addr.String()] = n
	} else {
		n.dropped = 0
	}
	if n.version < version {
		n.version = version
	}
	return n
}

func (nds *nodes) markDrop(addr *net.UDPAddr) {
    nds.Mutex.Lock()
    defer nds.Mutex.Unlock()

	n := nds.nodes[addr.String()]
	if n != nil {
		n.dropped += 1
	}
}

func (nds *nodes) markDead(nodes []string) {
    nds.Mutex.Lock()
    defer nds.Mutex.Unlock()

	for _, node := range nodes {
		n := nds.nodes[node]
		if n != nil {
			n.dropped += 1
		}
	}
}

func (nds *nodes) Active() []*node {
    nds.Mutex.Lock()
    defer nds.Mutex.Unlock()

    nodes := make([]*node, 0)
    for _, node := range nodes {
        if node.active && node.dropped < DropLimit {
            nodes = append(nodes, node)
        }
    }
    return nodes
}

func (nds *nodes) Dead() []*node {
    nds.Mutex.Lock()
    defer nds.Mutex.Unlock()

    nodes := make([]*node, 0)
    for _, node := range nodes {
        if node.active && node.dropped > DropLimit {
            nodes = append(nodes, node)
        }
    }
    return nodes
}

func (nds *nodes) Unknown() []*node {
    nds.Mutex.Lock()
    defer nds.Mutex.Unlock()

    nodes := make([]*node, 0)
    for _, node := range nodes {
        if !node.active && node.dropped < DropLimit {
            nodes = append(nodes, node)
        }
    }
    return nodes
}

func (nds *nodes) Update(added []string, dropped []string) error {
    nds.Mutex.Lock()
    defer nds.Mutex.Unlock()

    for _, addr := range added {
	n := nds.nodes[addr]
	if n != nil {
            n.active = true
        }
    }
    for _, addr := range added {
	delete(nds.nodes, addr)
    }

    return nil
}

func NewNodes() *nodes {
	nds := new(nodes)
	nds.nodes = make(map[string]*node)
	return nds
}
