package core

import (
    "errors"
    "fmt"
    "github.com/amscanne/hibera/utils"
    "os"
    "sort"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
)

var DropLimit = uint32(10)
var NoNode = errors.New("no node")

type Node struct {
    // The node Id (computed and cached).
    id  string

    // The node address (host:port).
    Addr string `json:"address"`

    // The node URL (fully specified).
    URL string `json:"url"`

    // The node domain.
    Domain string `json:"domain"`

    // The node keys.
    Keys []string `json:"keys"`

    // Active is whether or not the node has
    // failed liveness tests recently. If it has,
    // then the node will not be available.
    Active bool `json:"active"`

    // The last revision modified.
    Modified Revision `json:"modified"`

    // The last reported revision.
    Current Revision `json:"current"`

    // The number of messages sent without
    // us hearing a peep back from the node.
    // When this reaches a maximum, we consider
    // the node no longer alive.
    Dropped uint32 `json:"dropped"`
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

func APIFromURL(url string, addr string) string {
    parts := strings.SplitN(addr, ":", 2)
    ip := parts[0]
    var port string
    if len(parts) == 2 {
        port = parts[1]
    } else {
        port = strconv.FormatUint(uint64(utils.DefaultPort), 10)
    }
    url = strings.Replace(url, "%(addr)", addr, 1)
    url = strings.Replace(url, "%(ip)", ip, 1)
    url = strings.Replace(url, "%(port)", port, 1)
    return url
}

func (n *Node) API() string {
    return APIFromURL(n.URL, n.Addr)
}

func NewNode(addr string, url string, keys []string, domain string) *Node {
    node := new(Node)
    node.Addr = addr
    node.URL = url
    node.Keys = keys
    node.Domain = domain
    node.Active = false
    node.Dropped = 0
    node.Current = NoRevision
    node.Modified = NoRevision
    return node
}

type Nodes struct {
    // The map of all nodes.
    all NodeInfo

    // Our own node (also in all).
    self *Node

    sync.RWMutex
}

func (nodes *Nodes) Heartbeat(id string, rev Revision) bool {
    nodes.RWMutex.RLock()
    defer nodes.RWMutex.RUnlock()

    node := nodes.all[id]
    if node != nil && node.Id() == id {
        // NOTE: We don't heartbeat nodes whose Ids
        // do not match what we expect. Eventually they
        // will be removed and replaced by other nodes,
        // but we allow that to happen organically.
        node.RstDropped()
        if !rev.IsZero() && rev.GreaterThan(node.Current) {
            node.Current = rev
        }
        return true
    }

    return false
}

func (nodes *Nodes) NoHeartbeat(id string) {
    nodes.RWMutex.RLock()
    defer nodes.RWMutex.RUnlock()

    node := nodes.all[id]
    if node != nil && node != nodes.self && node.Id() == id {
        if node.Dropped == 0 {
            node.IncDropped()
        }
    }
}

func (nodes *Nodes) Activate(id string, rev Revision) error {
    nodes.RWMutex.RLock()
    defer nodes.RWMutex.RUnlock()

    node := nodes.all[id]
    if node == nil {
        return NoNode
    }

    // We activate this node.
    if !node.Active {
        node.Active = true
        node.Modified = rev
        node.Current = rev
        node.RstDropped()
    }

    return nil
}

func (nodes *Nodes) List(active bool) ([]string, error) {
    nodes.RWMutex.RLock()
    defer nodes.RWMutex.RUnlock()

    all := make([]string, 0, 0)
    i := 0
    for id, node := range nodes.all {
        if !active || node.Active {
            all = append(all, id)
            i += 1
        }
    }

    return all, nil
}

func (nodes *Nodes) Get(id string) (*Node, error) {
    nodes.RWMutex.RLock()
    defer nodes.RWMutex.RUnlock()

    return nodes.all[id], nil
}

func (nodes *Nodes) filter(fn func(node *Node) bool) []*Node {
    nodes.RWMutex.RLock()
    defer nodes.RWMutex.RUnlock()

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
    rev := nodes.self.Current
    return nodes.filter(func(node *Node) bool {
        return (node.Active &&
            node.Alive() &&
            node != nodes.self &&
            (node.Dropped > 0 || rev.GreaterThan(node.Current)))
    })
}

func (nodes *Nodes) HasSuspicious() bool {
    return nodes.self.Active && len(nodes.Suspicious()) > 0
}

func (nodes *Nodes) Self() *Node {
    return nodes.self
}

func (nodes *Nodes) Encode(next bool, na NodeInfo, N uint) (bool, error) {
    nodes.RWMutex.Lock()
    defer nodes.RWMutex.Unlock()

    // Create a list of nodes modified after rev.
    changed := false
    addokay := true
    notcurrent := uint(0)
    domains := make(map[string]bool)

    // Check that we are allowed to add nodes.
    // If there are nodes that are not currently marked as
    // dead, and those nodes are not yet up to the latest
    // revision -- then we can't add others. We will either
    // need to fail those nodes, or we will need to wait until
    // they are up to date. Note that they should become up to
    // date pretty quickly, as they are currently suspicious.
    if next {
        for _, node := range nodes.all {
            if node.Active && node.Alive() && nodes.self.Current.GreaterThan(node.Current) {
                notcurrent += 1
            }
        }
    }

    if notcurrent >= N {
        utils.Print("NODES", "%d (N=%d) NODES NOT CURRENT", notcurrent, N)
        addokay = false
    }

    for id, node := range nodes.all {

        if next && !node.Active && node.Alive() {
            // Check if we can't add this domain.
            // We limit ourselves to adding & removing nodes
            // from a single domain at a time. This is done
            // to ensure that the cluster can reach a consistent
            // state after each transition.
            if !addokay {
                utils.Print("NODES", "NOT ADDING %s, CURRENT LIMIT", id)
            } else if domains[node.Domain] || len(domains) < int(N) {
                // Propose this as an active node.
                domains[node.Domain] = true
                na[id] = NewNode(node.Addr, node.URL, node.Keys, node.Domain)
                na[id].Active = true
                na[id].Current = node.Current
                na[id].Modified = nodes.self.Current
                na[id].Dropped = 0
                changed = true
                utils.Print("NODES", "ADDING %s", id)
            } else {
                utils.Print("NODES", "NOT ADDING %s, DOMAIN LIMIT", id)
            }

        } else if next && node.Active && node != nodes.self && !node.Alive() {
            // Check if we can't remove this domain.
            if domains[node.Domain] || len(domains) < int(N) {
                // Propose this as a removed node.
                domains[node.Domain] = true
                na[id] = NewNode(node.Addr, node.URL, node.Keys, node.Domain)
                na[id].Active = false
                na[id].Current = node.Current
                na[id].Modified = nodes.self.Current
                na[id].Dropped = DropLimit
                changed = true
                utils.Print("NODES", "DROPPING %s", id)
            } else {
                utils.Print("NODES", "NOT DROPPING %s, DOMAIN LIMIT", id)
            }

        } else if node.Active || !nodes.self.Current.GreaterThan(node.Current.Next().Next()) {
            na[id] = NewNode(node.Addr, node.URL, node.Keys, node.Domain)
            na[id].Active = node.Active
            na[id].Current = node.Current
            na[id].Modified = node.Modified
            na[id].Dropped = 0
            utils.Print("NODES", "LEAVING %s", id)
        }
    }

    return changed, nil
}

func (nodes *Nodes) Decode(na NodeInfo) (bool, error) {
    nodes.RWMutex.Lock()
    defer nodes.RWMutex.Unlock()

    changed := false

    // Update all nodes with revs > Modified.
    for id, node := range na {
        orig_active := (nodes.all[id] != nil && nodes.all[id].Active)
        is_self := (nodes.self.Id() == id)
        new_node := false

        if nodes.all[id] == nil {
            nodes.all[id] = node
            new_node = true
        }
        if node.Modified.GreaterThan(nodes.all[id].Modified) {
            node.Current = nodes.all[id].Current
            node.Dropped = nodes.all[id].Dropped
            nodes.all[id] = node
            new_node = true
        }
        if new_node && is_self {
            nodes.self = node
        }

        now_active := (nodes.all[id] != nil && nodes.all[id].Active)
        if orig_active != now_active {
            changed = true
        }
    }

    return changed, nil
}

func (nodes *Nodes) Reset() {
    nodes.RWMutex.Lock()
    defer nodes.RWMutex.Unlock()

    for id, node := range nodes.all {
        if node != nodes.self {
            delete(nodes.all, id)
        }
    }
}

func (nodes *Nodes) Count() int {
    nodes.RWMutex.RLock()
    defer nodes.RWMutex.RUnlock()

    return len(nodes.all)
}

func NewNodes(addr string, url string, keys []string, domain string) *Nodes {
    nodes := new(Nodes)
    nodes.all = make(NodeInfo)

    // Do initial substitution on self.
    hostname, err := os.Hostname()
    if err != nil {
        url = strings.Replace(url, "%(hostname)", hostname, 1)
    }
    url = strings.Replace(url, "%(domain)", domain, 1)

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
    nodes.self = NewNode(addr, url, hashed, domain)
    nodes.all[nodes.self.Id()] = nodes.self

    return nodes
}

func NewTestNodes(n uint, k uint) *Nodes {
    // Create ourselves.
    uuids, err := utils.Uuids(k)
    if err != nil {
        return nil
    }
    nodes := NewNodes("localhost", "http://localhost", uuids, "self")
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
        url := fmt.Sprintf("http://other-%d", i)
        name := fmt.Sprintf("other-%d", i)
        node := NewNode(name, url, uuids, name)
        nodes.all[node.Id()] = node
        node.Active = true
    }

    return nodes
}
