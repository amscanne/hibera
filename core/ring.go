package core

import (
	"os"
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
	// Our id map.
	// This is the only public information
	// that will be serialized to the user.
	// (and versioned).
	IdMap map[string][]string

	// Our node map.
	// The id map is computed from the node
	// map whenever it officially changes.
	*Nodes
}

func NewRing(n *Nodes) *ring {
	r := new(ring)
	r.Nodes = n
	r.Recompute()
	return r
}

func (r *ring) Recompute() {
	r.IdMap = make(map[string][]string)
	for _, node := range r.Nodes.Active() {
		r.IdMap[node.Id()] = node.Ids()
	}
}

func (r *ring) IsMaster(key Key) bool {
	return false
}

func (r *ring) MasterFor(key Key) *Node {
	return nil
}

func (r *ring) NodesFor(key Key) []*Node {
	return nil
}
