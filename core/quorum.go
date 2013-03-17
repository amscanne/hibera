package core

import (
    "fmt"
    "errors"
    "hibera/client"
    "hibera/utils"
)

type QuorumResult struct {
    node  *Node
    value []byte
    rev   Revision
    err   error
}

func (c *Cluster) doSet(node *Node, key Key, value []byte, rev Revision) (Revision, error) {
    if node == c.Nodes.Self() {
        return c.data.DataSet(key, value, rev)
    }

    urls := make([]string, 1, 1)
    urls[0] = utils.MakeURL(node.Addr, "", nil)
    cl := client.NewHiberaAPI(urls, c.Id(), 0)
    rrev, err := cl.Set(string(key), value, uint64(rev))
    return Revision(rrev), err
}

func (c *Cluster) doGet(node *Node, key Key) ([]byte, Revision, error) {
    if node == c.Nodes.Self() {
        return c.data.DataGet(key)
    }

    urls := make([]string, 1, 1)
    urls[0] = utils.MakeURL(node.Addr, "", nil)
    cl := client.NewHiberaAPI(urls, c.Id(), 0)
    value, rev, err := cl.Get(string(key))
    return value, Revision(rev), err
}

func (c *Cluster) doRemove(node *Node, key Key, rev Revision) (Revision, error) {
    if node == c.Nodes.Self() {
        return c.data.DataRemove(key, rev)
    }

    urls := make([]string, 1, 1)
    urls[0] = utils.MakeURL(node.Addr, "", nil)
    cl := client.NewHiberaAPI(urls, c.Id(), 0)
    rrev, err := cl.Remove(string(key), uint64(rev))
    return Revision(rrev), err
}

func NewQuorumError(revcounts map[Revision]int) error {
    s := ""
    for rev, count := range revcounts {
        if len(s) > 0 {
            s += ","
        }
        s += fmt.Sprintf("rev=%d->%d", uint(rev), count)
    }
    return errors.New(fmt.Sprintf("QuorumError %s", s))
}

func (c *Cluster) quorum(ring *ring, key Key, fn func(*Node, chan<- *QuorumResult)) (*QuorumResult, error) {
    nodes := ring.NodesFor(key)
    self := c.Nodes.Self()
    res := make(chan *QuorumResult)
    var err error
    maxrev := Revision(0)

    utils.Print("QUORUM", "START %s (%d)", string(key), len(nodes))

    // Setup all the requests.
    for _, node := range nodes {
        if node != self {
            go fn(node, res)
        } else {
            self = node
        }
    }

    dumpResult := func(qrs *QuorumResult) {
        if qrs == nil {
            utils.Print("QUORUM", "  nil")
        } else {
            errstr := "false"
            if qrs.err != nil {
                errstr = fmt.Sprintf("true (%s)", qrs.err.Error())
            }
            utils.Print("QUORUM", "  node=%s rev=%d err=%s",
                qrs.node.Id(), uint(qrs.rev), errstr)
        }
    }

    // Read all results.
    qrs := make([]*QuorumResult, len(nodes), len(nodes))
    for i, node := range nodes {
        if node != self {
            qrs[i] = <-res
            dumpResult(qrs[i])

            if qrs[i] != nil && qrs[i].err != nil {
                c.Nodes.NoHeartbeat(qrs[i].node.Id())
            }
        }
    }

    // Check if there's a quorum.
    revcounts := make(map[Revision]int)
    revrefs := make(map[Revision]*QuorumResult)
    for i, _ := range nodes {
        if qrs[i] != nil && qrs[i].err == nil {
            revcounts[qrs[i].rev] += 1
            revrefs[qrs[i].rev] = qrs[i]
        }
        if qrs[i] != nil && qrs[i].err != nil {
            err = qrs[i].err
        }
        if qrs[i] != nil && qrs[i].rev > maxrev {
            maxrev = qrs[i].rev
        }
    }

    // Find a quorum.
    var self_qr *QuorumResult
    for rev, count := range revcounts {

        // If the remote nodes cover quorum.
        if count > (len(nodes) / 2) {
            utils.Print("QUORUM", "SUCCESS rev=%d", uint(rev))
            return revrefs[rev], nil

            // If the local node is one of the quorum.
        } else if self != nil && (count+1) > (len(nodes)/2) {

            // Try the local node.
            if self_qr == nil {
                go fn(self, res)
                self_qr = <-res
                dumpResult(self_qr)
            }

            // If it matches, we have quorum.
            if self_qr != nil && self_qr.err == nil && self_qr.rev == rev {
                utils.Print("QUORUM", "SUCCESS rev=%d", uint(rev))
                return self_qr, nil
            }
            if self_qr != nil && self_qr.err != nil {
                utils.Print("QUORUM", "LOCAL-FAILURE %s", self_qr.err.Error())
                err = self_qr.err
            }
            if self_qr != nil && self_qr.rev > maxrev {
                maxrev = self_qr.rev
            }
        }
    }
    if self_qr != nil {
        revcounts[self_qr.rev] += 1
        revrefs[self_qr.rev] = self_qr
        if self_qr.rev > maxrev {
            maxrev = self_qr.rev
        }
    }

    // Check for the case of local authority.
    if len(nodes) == 1 && nodes[0] == self {
        go fn(self, res)
        qr := <-res
        if qr != nil && qr.err == nil {
            utils.Print("QUORUM", "LOCAL-SUCCESS rev=%d", uint(qr.rev))
            return qr, nil
        } else if qr == nil {
            utils.Print("QUORUM", "LOCAL-FAILURE no result?")
            return nil, nil
        } else {
            utils.Print("QUORUM", "LOCAL-FAILURE %s", qr.err.Error())
            qr.rev = maxrev
            return qr, qr.err
        }
    }

    // No quorum found.
    if err == nil {
        err = NewQuorumError(revcounts)
    }
    utils.Print("QUORUM", "FAILURE %s", err.Error())
    return &QuorumResult{rev: maxrev}, err
}

func (c *Cluster) quorumGet(ring *ring, key Key) ([]byte, Revision, error) {
    fn := func(node *Node, res chan<- *QuorumResult) {
        value, rev, err := c.doGet(node, key)
        res <- &QuorumResult{node, value, rev, err}
    }
    qr, err := c.quorum(ring, key, fn)
    var value []byte
    var rev Revision
    if qr != nil {
        value = qr.value
        rev = qr.rev
    }
    return value, rev, err
}

func (c *Cluster) quorumSet(ring *ring, key Key, value []byte, rev Revision) (Revision, error) {
    fn := func(node *Node, res chan<- *QuorumResult) {
        rev, err := c.doSet(node, key, value, rev)
        res <- &QuorumResult{node, value, rev, err}
    }
    qr, err := c.quorum(ring, key, fn)
    if qr != nil {
        rev = qr.rev
    }
    return rev, err
}

func (c *Cluster) quorumRemove(ring *ring, key Key, rev Revision) (Revision, error) {
    fn := func(node *Node, res chan<- *QuorumResult) {
        rev, err := c.doRemove(node, key, rev)
        res <- &QuorumResult{node, nil, rev, err}
    }
    qr, err := c.quorum(ring, key, fn)
    if qr != nil {
        rev = qr.rev
    }
    return rev, err
}

type ListResult struct {
    items []Key
    err   error
}

func (c *Cluster) doList(node *Node) ([]Key, error) {
    if node == c.Nodes.Self() {
        return c.data.DataList()
    }

    urls := make([]string, 1, 1)
    urls[0] = utils.MakeURL(node.Addr, "", nil)
    cl := client.NewHiberaAPI(urls, c.Id(), 0)
    items, err := cl.List()
    if items == nil || err != nil {
        return nil, err
    }
    keys := make([]Key, len(items), len(items))
    for i, item := range items {
        keys[i] = Key(item)
    }
    return keys, nil
}

func (c *Cluster) doClear(node *Node) error {
    if node == c.Nodes.Self() {
        return c.data.DataClear()
    }

    urls := make([]string, 1, 1)
    urls[0] = utils.MakeURL(node.Addr, "", nil)
    cl := client.NewHiberaAPI(urls, c.Id(), 0)
    return cl.Clear()
}

func (c *Cluster) allList() ([]Key, error) {
    nodes := c.Nodes.Active()
    items := make(map[Key]bool)

    utils.Print("QUORUM", "LIST %d", len(nodes))

    // Setup all the requests.
    fn := func(node *Node, res chan<- *ListResult) {
        items, err := c.doList(node)
        res <- &ListResult{items, err}
    }
    reschan := make(chan *ListResult)
    for _, node := range nodes {
        go fn(node, reschan)
    }

    // Read all results.
    var err error
    for i, _ := range nodes {
        res := <-reschan
        if res.items != nil {
            utils.Print("QUORUM", "  %d %d", i, len(res.items))
            for _, item := range res.items {
                items[item] = true
            }
        }
        if res.err != nil {
            utils.Print("QUORUM", "  %d %s", i, res.err)
            err = res.err
        }
    }

    // Build our result.
    itemsarr := make([]Key, len(items), len(items))
    i := 0
    for item, _ := range items {
        itemsarr[i] = item
        i += 1
    }

    // Return the result.
    if len(itemsarr) > 0 {
        return itemsarr, nil
    }
    return itemsarr, err
}

func (c *Cluster) allClear() error {
    nodes := c.Nodes.Active()

    utils.Print("QUORUM", "CLEAR %d", len(nodes))

    // Setup all the requests.
    fn := func(node *Node, res chan<- error) {
        res <- c.doClear(node)
    }
    reschan := make(chan error)
    for _, node := range nodes {
        go fn(node, reschan)
    }

    // Read all results.
    var err error
    for i, _ := range nodes {
        nerr := <-reschan
        if nerr != nil {
            utils.Print("QUORUM", "  %d %s", i, err)
            err = nerr
        }
    }

    return err
}
