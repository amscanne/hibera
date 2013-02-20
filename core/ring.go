package core

import (
	"os"
	"sync"
	"sort"
)

func defaultDomain() string {
	domain, err := os.Hostname()
	if err != nil {
		return ""
	}
	return domain
}

var DefaultDomain = defaultDomain()

type ring struct {
	// Our own node on the ring.
	self *Node

	// Our key map.
	// This is a simple map of all node Keys to the appropriate node.
	keymap map[string]*Node

	// Our node map.
	// The id map above is computed from the node map whenever it changes.
	nodes *Nodes

	// Sorted keys.
	// This is a sequence of sorted keys for fast searching.
	sorted []string

	// Our cache.
	// This is a simple cache of Key to the appropriate nodes.
	cache map[Key][]*Node

	sync.Mutex
}

func NewRing(self *Node, nodes *Nodes) *ring {
	r := new(ring)
	r.self = self
	r.nodes = nodes
	r.Recompute()
	return r
}

func (r *ring) Recompute() {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	// Make a new keymap.
	r.keymap = make(map[string]*Node)
	r.sorted = make([]string, 0)
	for _, node := range r.nodes.Active() {
		keys := node.Keys()
		for _, id := range keys {
			r.keymap[id] = node
			r.sorted = append(r.sorted, id)
		}
	}

	// Make a new sorted map.
	sort.Sort(sort.StringSlice(r.sorted))

	// Clear the cache.
	r.cache = make(map[Key][]*Node)
}

func (r *ring) IsMaster(key Key) bool {
	node := r.MasterFor(key)
	if node != nil && node == r.self {
		return true
	}
	return false
}

func (r *ring) MasterFor(key Key) *Node {
	nodes := r.NodesFor(key)
	if nodes == nil || len(nodes) == 0 {
		return nil
	}
	return nodes[0]
}

func (r *ring) NodesFor(key Key) []*Node {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	// Check if it's been cached.
	cached := r.cache[key]
	if cached != nil {
		return cached
	}

	// Search in our sorted array.
	nodes := make([]*Node, 0)
	start := sort.SearchStrings(r.sorted, string(key))
	current := start

	for {
		// Check if node satisfies our failure domain.
		node := r.keymap[r.sorted[current]]
		nodes = append(nodes, node)

		// Move to the next key.
		current = (current + 1) % len(r.sorted)
		if current == start {
			break
		}
	}

	// Cache the value.
	r.cache[key] = nodes
	return nodes
}
