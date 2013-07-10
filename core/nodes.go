package core

import (
    "hibera/utils"
    "sort"
    "sync"
    "sync/atomic"
)

var DropLimit = uint32(10)

type Node struct {
    // The node address (host:port).
    Addr string

    // The node Id (computed and cached).
    id  string

    // The node domain.
    Domains []string

    // The node keys.
    Keys []string

    // Active is whether or not the node has
    // failed liveness tests recently. If it has,
    // then the node will not be available.
    Active bool

    // The last revision modified.
    Modified Revision

    // The number of messages sent without
    // us hearing a peep back from the node.
    // When this reaches a maximum, we consider
    // the node no longer alive.
    Dropped uint32
}

func (n *Node) IncDropped() {
    atomic.AddUint32(&n.Dropped, 1)
}

func (n *Node) RstDropped() {
    atomic.StoreUint32(&n.Dropped, 0)
}

func (n *Node) Alive() bool {
    return atomic.LoadUint32(&n.Dropped) < DropLimit
}

func (n *Node) Id() string {
    if n.id == "" {
        sort.Sort(sort.StringSlice(n.Keys))
        n.id = utils.MHash(n.Keys)
    }
    return n.id
}

func NewNode(addr string, keys []string, domains []string) *Node {
    node := new(Node)
    node.Addr = addr
    node.Keys = keys
    node.Domains = domains
    node.Active = false
    node.Dropped = DropLimit
    return node
}

type Nodes struct {
    // The map of all nodes.
    all NodeInfo

    // Our own node (also in all).
    self *Node

    sync.Mutex
}

func (nodes *Nodes) Heartbeat(id string) bool {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    node := nodes.all[id]
    if node != nil && node.Id() == id {
        // NOTE: We don't heartbeat nodes whose Ids
        // do not match what we expect. Eventually they
        // will be removed and replaced by other nodes,
        // but we allow that to happen organically.
        node.RstDropped()
        return true
    }

    return false
}

func (nodes *Nodes) NoHeartbeat(id string) {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    node := nodes.all[id]
    if node != nil && node != nodes.self && node.Id() == id {
        node.IncDropped()
    }
}

func (nodes *Nodes) Activate(id string, rev Revision) bool {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    node := nodes.all[id]
    if node == nil {
        return false
    }

    // We activate this node.
    if !node.Active {
        node.Active = true
        node.Modified = rev
    }
    node.RstDropped()

    return true
}

func (nodes *Nodes) Fail(id string, rev Revision) bool {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    node := nodes.all[id]
    if node == nil {
        return false
    }

    // We deactivate this node.
    if node.Active {
        node.Active = false
        node.Modified = rev
        return true
    }
    return false
}

func (nodes *Nodes) Active() (map[string]string, error) {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    all := make(map[string]string)
    for id, node := range nodes.all {
        if node.Active {
            all[id] = node.Addr
        }
    }

    return all, nil
}

func (nodes *Nodes) Get(id string) (*Node, error) {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()
    return nodes.all[id], nil
}

func (nodes *Nodes) filter(fn func(node *Node) bool) []*Node {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    filtered := make([]*Node, 0, 0)
    for _, node := range nodes.all {
        if fn(node) {
            filtered = append(filtered, node)
        }
    }

    return filtered
}

func (nodes *Nodes) Inuse() []*Node {
    return nodes.filter(func(node *Node) bool {
        return node.Active
    })
}

func (nodes *Nodes) Dead() []*Node {
    return nodes.filter(func(node *Node) bool {
        return node.Active && node != nodes.self && !node.Alive()
    })
}

func (nodes *Nodes) Others() []*Node {
    return nodes.filter(func(node *Node) bool {
        return node.Active && node != nodes.self
    })
}

func (nodes *Nodes) Suspicious() []*Node {
    return nodes.filter(func(node *Node) bool {
        return node.Active && node.Dropped > 0
    })
}

func (nodes *Nodes) Self() *Node {
    return nodes.self
}

func (nodes *Nodes) Count() int {
    return len(nodes.all)
}

func (nodes *Nodes) Encode(rev Revision, next bool, na NodeInfo) (bool, error) {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    // Create a list of nodes modified after rev.
    adddomain := make(map[string]bool)
    removedomain := make(map[string]bool)
    changed := false

    for id, node := range nodes.all {

        if next && (!node.Active && node.Alive()) {
            // Check if we can't add this domain.
            doadd := true
            for _, domain := range node.Domains {
                if adddomain[domain] {
                    doadd = false
                    break
                }
            }
            if !doadd {
                continue
            }
            for _, domain := range node.Domains {
                adddomain[domain] = true
            }

            // Propose this as an active node.
            na[id] = NewNode(node.Addr, node.Keys, node.Domains)
            na[id].Active = node.Alive()
            na[id].Modified = rev
            changed = true

        } else if next && node.Active && node != nodes.self && !node.Alive() {
            // Check if we can't remove this domain.
            doremove := true
            for _, domain := range node.Domains {
                if removedomain[domain] {
                    doremove = false
                    break
                }
            }
            if !doremove {
                continue
            }
            for _, domain := range node.Domains {
                removedomain[domain] = true
            }

            // Propose this as a removed node.
            na[id] = NewNode(node.Addr, node.Keys, node.Domains)
            na[id].Active = false
            na[id].Modified = rev
            changed = true

        } else if node == nodes.self || node.Modified >= rev {
            na[id] = node
        }
    }

    return changed, nil
}

func (nodes *Nodes) Decode(na NodeInfo) (bool, error) {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    changed := false

    // Update all nodes with revs > Modified.
    for id, node := range na {
        orig_active := (nodes.all[id] != nil && nodes.all[id].Active)
        if nodes.all[id] == nil ||
            nodes.all[id].Modified < node.Modified {
            if id == nodes.self.Id() {
                nodes.self = node
            }
            nodes.all[id] = node
        }
        now_active := (nodes.all[id] != nil && nodes.all[id].Active)
        if orig_active != now_active {
            changed = true
        }
    }

    return changed, nil
}

func (nodes *Nodes) Reset() {
    for id, node := range nodes.all {
        if node != nodes.self {
            delete(nodes.all, id)
        }
    }
}

func NewNodes(addr string, keys []string, domains []string) *Nodes {
    nodes := new(Nodes)
    nodes.all = make(NodeInfo)

    // Nodes are create with hashed keys.
    // NOTE: all other nodes will be added
    // remotely, so they will always have
    // hashed values already. We add ourselves
    // here, so this is really the unique
    // entry point for nodes into the entire
    // system.
    hashed := make([]string, len(keys), len(keys))
    for i, key := range keys {
        hashed[i] = utils.Hash(key)
    }
    nodes.self = NewNode(addr, hashed, domains)
    nodes.all[nodes.self.Id()] = nodes.self

    return nodes
}

func NewTestNodes(n uint, k uint, domains []string) *Nodes {
    // Create ourselves.
    uuids, err := utils.Uuids(k)
    if err != nil {
        return nil
    }
    nodes := NewNodes("", uuids, domains)
    nodes.self.Active = true

    for i := uint(0); i < n; i += 1 {
        // Create a new node.
        // NOTE: The keys will not be hashed in the
        // test case here as they are above in NewNodes().
        // This is okay, it may give a slightly different
        // distribution but for some reason in my head I
        // feel that it might be advantage to extend this
        // mechanism to include other weird corner cases
        // later.
        uuids, err = utils.Uuids(k)
        if err != nil {
            return nil
        }
        node := NewNode("", uuids, domains)
        nodes.all[node.Id()] = node
        node.Active = true
    }

    return nodes
}
