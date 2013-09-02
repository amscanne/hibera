package cluster

import (
    "hibera/core"
    "hibera/utils"
    "sort"
    "sync"
)

type ring struct {
    // The count of slaves per key.
    slaves uint

    // Our key map.
    // This is a simple map of all node core.Keys to the appropriate node.
    keymap map[string]*core.Node

    // Our node map.
    // The id map above is computed from the node map whenever it changes.
    *core.Nodes

    // Sorted keys.
    // This is a sequence of sorted keys for fast searching.
    sorted []string

    // Our cache.
    // This is a simple cache of core.Key to the appropriate nodes.
    cache map[string][]*core.Node

    sync.Mutex
}

func NewRing(slaves uint, nodes *core.Nodes) *ring {
    r := new(ring)
    r.slaves = slaves
    r.Nodes = nodes
    r.Recompute()
    return r
}

func (r *ring) Size() int {
    return r.Nodes.Count()
}

func (r *ring) Recompute() {
    // Make a new keymap.
    keymap := make(map[string]*core.Node)
    sorted := make([]string, 0)

    for _, node := range r.Nodes.Inuse() {
        for _, key := range node.Keys {
            keymap[key] = node
            sorted = append(sorted, key)
        }
    }

    // Make a new sorted map.
    sort.Sort(sort.StringSlice(sorted))

    r.Mutex.Lock()
    defer r.Mutex.Unlock()

    r.keymap = keymap
    r.sorted = sorted

    // Clear the cache.
    r.cache = make(map[string][]*core.Node)
}

func (r *ring) IsMaster(key core.Key) bool {
    return r.MasterFor(key) == r.Nodes.Self()
}

func (r *ring) MasterFor(key core.Key) *core.Node {
    nodes := r.NodesFor(key)
    if nodes == nil || len(nodes) < 1 {
        return nil
    }
    return nodes[0]
}

func (r *ring) IsFailover(key core.Key) bool {
    slaves := r.SlavesFor(key)
    if slaves == nil {
        return false
    }
    for _, node := range slaves {
        if node == r.Nodes.Self() {
            return true
        }
        if node.Alive() {
            // There is a failover node available
            // before us. They are responsible for
            // failing over.
            return false
        }
    }
    return false
}

func (r *ring) IsSlave(key core.Key) bool {
    slaves := r.SlavesFor(key)
    if slaves == nil {
        return false
    }
    for _, node := range slaves {
        if node == r.Nodes.Self() {
            return true
        }
    }
    return false
}

func (r *ring) SlavesFor(key core.Key) []*core.Node {
    nodes := r.NodesFor(key)
    if nodes == nil || len(nodes) < 2 {
        return nil
    }
    return nodes[1:len(nodes)]
}

func (r *ring) IsNode(key core.Key, check *core.Node) bool {
    nodes := r.NodesFor(key)
    if nodes == nil {
        return false
    }
    for _, node := range nodes {
        if check == node {
            return true
        }
    }
    return false
}

func (r *ring) lookup(h string) []*core.Node {
    if len(r.sorted) == 0 {
        return nil
    }

    // Search in our sorted array.
    nodes := make([]*core.Node, 0)
    start := sort.SearchStrings(r.sorted, h)
    if start >= len(r.sorted) {
        // Wrap around the ring.
        start = 0
    }
    perdomain := make(map[string]bool)
    peraddr := make(map[string]bool)
    current := start

    for {
        // Check if node satisfies our failure domain.
        node := r.keymap[r.sorted[current]]

        // Check if this satisfies our count.
        // We *always* take the first N nodes.
        // And we always take the first N nodes
        // that will satisfy our domain requirements.
        if !peraddr[node.Addr] &&
            (len(nodes) < int(r.slaves) ||
                (!perdomain[node.Domain] && len(perdomain) < int(r.slaves))) {
            peraddr[node.Addr] = true
            perdomain[node.Domain] = true
            nodes = append(nodes, node)
        }

        // Check if we've satisfied all of our requirements.
        if len(nodes) >= int(r.slaves) && len(perdomain) >= int(r.slaves) {
            break
        }

        // Move to the next key.
        current = (current + 1) % len(r.sorted)
        if current == start {
            break
        }
    }

    return nodes
}

func (r *ring) NodesFor(key core.Key) []*core.Node {
    r.Mutex.Lock()
    defer r.Mutex.Unlock()

    // Check if it's been cached.
    h := utils.Hash(string(key))
    cached := r.cache[h]
    if cached == nil {
        // Do a manual lookup.
        nodes := r.lookup(h)

        // Cache the value.
        r.cache[h] = nodes
        cached = nodes
    }

    return cached
}
