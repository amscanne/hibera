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
	*nodes
}

func NewRing(n *nodes) *ring {
	r := new(ring)
	r.nodes = n
	r.Recompute()
	return r
}

func (r *ring) Recompute() {
	r.IdMap = make(map[string][]string)
	for _, node := range r.nodes.Active() {
		r.IdMap[node.Id()] = node.Ids()
	}
}
