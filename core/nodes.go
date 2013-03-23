package core

import (
    "net"
    "sync"
    "bytes"
    "sort"
    "sync/atomic"
    "encoding/json"
    "hibera/utils"
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

    // Active is whether or not this node
    // is in the ring. If active is false
    // and dropped is above our threshold
    // we will just disregard the node.
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
        n.id = mhash(n.Keys)
    }
    return n.id
}

func NewNode(keys []string, domains []string) *Node {
    node := new(Node)
    node.Keys = keys
    node.Domains = domains
    node.Active = false
    node.Dropped = DropLimit
    return node
}

type Nodes struct {
    // The map of all nodes.
    all map[string]*Node

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

func (nodes *Nodes) Active() []*Node {
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

func (nodes *Nodes) Encode(rev Revision, next bool) ([]byte, error) {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    // Create a list of nodes modified after rev.
    na := make(map[string]*Node)
    adddomain := make(map[string]bool)
    removedomain := make(map[string]bool)

    for id, node := range nodes.all {
        if next {
            if !node.Active && node.Alive() {
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
                na[id] = NewNode(node.Keys, node.Domains)
                na[id].Active = true
                na[id].Modified = rev

            } else if node.Active && node != nodes.self && !node.Alive() {
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
                na[id] = NewNode(node.Keys, node.Domains)
                na[id].Active = false
                na[id].Modified = rev
            }
        } else {
            if node == nodes.self || node.Modified >= rev {
                na[id] = node
            }
        }
    }
    if len(na) == 0 {
        return nil, nil
    }

    // Encode that list as JSON.
    buf := new(bytes.Buffer)
    enc := json.NewEncoder(buf)
    err := enc.Encode(&na)

    return buf.Bytes(), err
}

func bestAddr(addr1 string, addr2 string) string {
    // Check for empty addresses.
    if addr1 == "" {
        return addr2
    }
    if addr2 == "" {
        return addr1
    }

    // Try resolving the addresses.
    udpaddr1, err := net.ResolveUDPAddr("udp", addr1)
    if err != nil || udpaddr1.IP.IsLoopback() {
        return addr2
    }
    udpaddr2, err := net.ResolveUDPAddr("udp", addr2)
    if err != nil || udpaddr2.IP.IsLoopback() {
        return addr1
    }

    // Select the first, since none is better.
    return addr1
}

func (nodes *Nodes) Decode(data []byte) (bool, error) {
    changed := false

    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    // Decode our list of nodes from JSON.
    na := make(map[string]*Node)
    buf := bytes.NewBuffer(data)
    dec := json.NewDecoder(buf)
    err := dec.Decode(&na)

    if err != nil {
        return changed, err
    }

    // Update all nodes with revs > Modified.
    for id, node := range na {
        orig_active := (nodes.all[id] != nil && nodes.all[id].Active)
        if nodes.all[id] == nil ||
            nodes.all[id].Modified < node.Modified {
            if id == nodes.self.Id() {
                nodes.self = node
            }
            if nodes.all[id] != nil {
                node.Addr = bestAddr(node.Addr, nodes.all[id].Addr)
            }
            nodes.all[id] = node
        }
        now_active := (nodes.all[id] != nil && nodes.all[id].Active)
        if orig_active != now_active {
            changed = true
        }
    }

    return changed, err
}

func (nodes *Nodes) Update(id string, addr *net.UDPAddr) {
    nodes.Mutex.Lock()
    defer nodes.Mutex.Unlock()

    // Check if this node exists.
    node := nodes.all[id]
    if node == nil {
        return
    }

    // Make the address is up-to-date.
    node.Addr = addr.String()
}

func (nodes *Nodes) dumpNodes() {
    for id, node := range nodes.all {
        utils.Print("CLUSTER", "  %s %s active=%t dropped=%d",
            id, node.Addr, node.Active, node.Dropped)
    }
}

func (nodes *Nodes) Reset() {
    for id, node := range nodes.all {
        if node != nodes.self {
            delete(nodes.all, id)
        }
    }
}

func NewNodes(keys []string, domains []string) *Nodes {
    nodes := new(Nodes)
    nodes.all = make(map[string]*Node)

    // Nodes are create with hashed keys.
    // NOTE: all other nodes will be added
    // remotely, so they will always have
    // hashed values already. We add ourselves
    // here, so this is really the unique
    // entry point for nodes into the entire
    // system.
    hashed := make([]string, len(keys), len(keys))
    for i, key := range keys {
        hashed[i] = hash(key)
    }
    nodes.self = NewNode(hashed, domains)
    nodes.all[nodes.self.Id()] = nodes.self

    return nodes
}

func NewTestNodes(n uint, k uint, domains []string) *Nodes {
    // Create ourselves.
    uuids, err := utils.Uuids(k)
    if err != nil {
        return nil
    }
    nodes := NewNodes(uuids, domains)
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
        node := NewNode(uuids, domains)
        nodes.all[node.Id()] = node
        node.Active = true
    }

    return nodes
}
