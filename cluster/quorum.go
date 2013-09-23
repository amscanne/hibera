package cluster

import (
    "errors"
    "fmt"
    "github.com/amscanne/hibera/client"
    "github.com/amscanne/hibera/core"
    "github.com/amscanne/hibera/utils"
)

type QuorumResult struct {
    node  *core.Node
    rev   core.Revision
    value []byte
    err   error
}

func (c *Cluster) getClient(url string) *client.HiberaAPI {
    return client.NewHiberaAPI([]string{url}, c.root, c.Nodes.Self().Id(), 0, RootNamespace, false)
}

func (c *Cluster) doSet(node *core.Node, ns core.Namespace, key core.Key, rev core.Revision, value []byte) (core.Revision, error) {
    if node == c.Nodes.Self() {
        utils.Print("QUORUM", "    SET-LOCAL key=%s rev=%s", key, rev.String())
        return c.Data.DataSet(ns, key, rev, value)
    }

    utils.Print("QUORUM", "    SET-REMOTE key=%s rev=%s", key, rev.String())
    cl := c.getClient(node.API())
    defer cl.Close()
    ok, rrev, err := cl.NSDataSet(ns, key, rev, value)
    if err != nil {
        return rrev, err
    }
    if !ok {
        return rrev, core.RevConflict
    }
    return rrev, nil
}

func (c *Cluster) doInfo(node *core.Node, ns core.Namespace, key core.Key) (core.Revision, error) {
    if node == c.Nodes.Self() {
        utils.Print("QUORUM", "    INFO-LOCAL key=%s", key)
        _, rev, err := c.Data.DataGet(ns, key, false)
        return rev, err
    }

    utils.Print("QUORUM", "    INFO-REMOTE key=%s", key)
    cl := c.getClient(node.API())
    defer cl.Close()
    return cl.NSDataInfo(ns, key, core.NoRevision, 1, false)
}

func (c *Cluster) doGet(node *core.Node, ns core.Namespace, key core.Key) ([]byte, core.Revision, error) {
    if node == c.Nodes.Self() {
        utils.Print("QUORUM", "    GET-LOCAL key=%s", key)
        return c.Data.DataGet(ns, key, true)
    }

    utils.Print("QUORUM", "    GET-REMOTE key=%s", key)
    cl := c.getClient(node.API())
    defer cl.Close()
    return cl.NSDataGet(ns, key, core.NoRevision, 1, false)
}

func (c *Cluster) doRemove(node *core.Node, ns core.Namespace, key core.Key, rev core.Revision) (core.Revision, error) {
    if node == c.Nodes.Self() {
        utils.Print("QUORUM", "    REMOVE-LOCAL key=%s rev=%s", key, rev.String())
        return c.Data.DataRemove(ns, key, rev)
    }

    utils.Print("QUORUM", "    REMOVE-REMOTE key=%s rev=%s", key, rev.String())
    cl := c.getClient(node.API())
    defer cl.Close()
    ok, rrev, err := cl.NSDataRemove(ns, key, rev)
    if err != nil {
        return rrev, err
    }
    if !ok {
        return rrev, core.RevConflict
    }
    return rrev, err
}

func NewQuorumError(revcounts map[core.Revision]int) error {
    s := ""
    for rev, count := range revcounts {
        if len(s) > 0 {
            s += ","
        }
        s += fmt.Sprintf("rev=%s->%d", rev.String(), count)
    }
    return errors.New(fmt.Sprintf("QuorumError %s", s))
}

func (c *Cluster) quorum(ring *ring, key core.Key, fn func(*core.Node, chan<- *QuorumResult)) (*QuorumResult, error) {
    nodes := ring.NodesFor(key)
    real_self := c.Nodes.Self()
    var self *core.Node
    res := make(chan *QuorumResult)
    var err error
    maxrev := core.NoRevision

    utils.Print("QUORUM", "START '%s' (%d/%d)", key, len(nodes), ring.Size())

    // Grab the quorum lock.
    c.quorumLock.L.Lock()
    if c.quorumInProgress[key] {
        // Indicate that there are waiters.
        c.quorumWaiters[key] = c.quorumWaiters[key] + 1

        // Wait for the quorum to be finished on this key.
        for c.quorumInProgress[key] {
            c.quorumLock.Wait()
        }

        // Release our wait indication.
        c.quorumWaiters[key] = c.quorumWaiters[key] - 1
        if c.quorumWaiters[key] == 0 {
            delete(c.quorumWaiters, key)
        }
    }

    // Indicate that there is a quorum is progress.
    c.quorumInProgress[key] = true
    c.quorumLock.L.Unlock()

    defer func() {
        c.quorumLock.L.Lock()
        // Indicate that there is no quorum in progress.
        delete(c.quorumInProgress, key)

        // If there are waiters for this key, notify.
        if c.quorumWaiters[key] != 0 {
            c.quorumLock.Broadcast()
        }
        c.quorumLock.L.Unlock()
    }()

    // Setup all the requests.
    for _, node := range nodes {
        if node != real_self {
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
            utils.Print("QUORUM", "  NODE id=%s rev=%s err=%s",
                qrs.node.Id(), qrs.rev.String(), errstr)
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
    revcounts := make(map[core.Revision]int)
    revrefs := make(map[core.Revision]*QuorumResult)
    for i, _ := range nodes {
        if qrs[i] != nil && qrs[i].err == nil {
            revcounts[qrs[i].rev] += 1
            revrefs[qrs[i].rev] = qrs[i]
        }
        if qrs[i] != nil && qrs[i].err != nil {
            err = qrs[i].err
        }
        if qrs[i] != nil && qrs[i].rev.GreaterThan(maxrev) {
            maxrev = qrs[i].rev
        }
    }

    // Find a quorum.
    var self_qr *QuorumResult
    do_self := func() {
        if self != nil && self_qr == nil {
            go fn(self, res)
            self_qr = <-res
            dumpResult(self_qr)
        }
    }

    for rev, count := range revcounts {

        // If the remote nodes cover quorum.
        if count > (len(nodes) / 2) {

            // Ensure that if we were supposed to aggregate
            // ourself, then it is included in the result.
            do_self()

            utils.Print("QUORUM", "SUCCESS rev=%s", rev.String())
            return revrefs[rev], nil

            // If the local node is one of the quorum.
            // This is a complex structure to support the writing
            // of the local node only after the remote nodes have
            // successfully written. Note that above and below we
            // may have to do the local function regardless.

        } else if self != nil && (count+1) > (len(nodes)/2) {

            // Try the local node.
            do_self()

            // If it matches, we have quorum.
            if self_qr != nil && self_qr.err == nil && self_qr.rev.Equals(rev) {
                utils.Print("QUORUM", "SUCCESS rev=%s", rev.String())
                return self_qr, nil
            }
            if self_qr != nil && self_qr.err != nil {
                utils.Print("QUORUM", "LOCAL-FAILURE %s", self_qr.err.Error())
                err = self_qr.err
            }

            // The local node did not match...
            // We stop at this point and remember the maxrev.
            if self_qr != nil && self_qr.rev.GreaterThan(maxrev) {
                maxrev = self_qr.rev
            }
        }
    }

    // If we haven't yet committed ourself.
    do_self()

    // Check for the case of local authority.
    if len(nodes) == 1 && self_qr != nil {
        return self_qr, nil
    }

    // Aggregrate our self result.
    if self_qr != nil {
        revcounts[self_qr.rev] += 1
        revrefs[self_qr.rev] = self_qr
        if self_qr.rev.GreaterThan(maxrev) {
            maxrev = self_qr.rev
        }
    }

    // No quorum found.
    if err == nil {
        err = NewQuorumError(revcounts)
    }
    utils.Print("QUORUM", "FAILURE %s", err.Error())
    return &QuorumResult{rev: maxrev}, err
}

func (c *Cluster) quorumInfo(ring *ring, ns core.Namespace, key core.Key) (core.Revision, error) {
    fn := func(node *core.Node, res chan<- *QuorumResult) {
        utils.Print("QUORUM", "  INFO key=%s", key)
        rev, err := c.doInfo(node, ns, key)
        res <- &QuorumResult{node, rev, nil, err}
    }
    qr, err := c.quorum(ring, key, fn)
    var rev core.Revision
    if qr != nil && err == nil {
        rev = qr.rev
        err = qr.err
    }
    return rev, err
}

func (c *Cluster) quorumGet(ring *ring, ns core.Namespace, key core.Key) ([]byte, core.Revision, error) {
    fn := func(node *core.Node, res chan<- *QuorumResult) {
        utils.Print("QUORUM", "  GET key=%s", key)
        value, rev, err := c.doGet(node, ns, key)
        res <- &QuorumResult{node, rev, value, err}
    }
    qr, err := c.quorum(ring, key, fn)
    var value []byte
    var rev core.Revision
    if qr != nil && err == nil {
        value = qr.value
        rev = qr.rev
        err = qr.err
    }
    return value, rev, err
}

func (c *Cluster) quorumSet(ring *ring, ns core.Namespace, key core.Key, rev core.Revision, value []byte) (core.Revision, error) {
    fn := func(node *core.Node, res chan<- *QuorumResult) {
        utils.Print("QUORUM", "  SET key=%s rev=%s len=%d",
            key, rev.String(), len(value))
        cur_rev, err := c.doInfo(node, ns, key)
        if err != nil || !cur_rev.Equals(rev) {
            rev, err := c.doSet(node, ns, key, rev, value)
            res <- &QuorumResult{node, rev, value, err}
        }
    }
    qr, err := c.quorum(ring, key, fn)
    if qr != nil && err == nil {
        rev = qr.rev
        err = qr.err
    }
    return rev, err
}

func (c *Cluster) quorumRemove(ring *ring, ns core.Namespace, key core.Key, rev core.Revision) (core.Revision, error) {
    fn := func(node *core.Node, res chan<- *QuorumResult) {
        utils.Print("QUORUM", "  REMOVE key=%s rev=%s",
            key, rev.String())
        rev, err := c.doRemove(node, ns, key, rev)
        res <- &QuorumResult{node, rev, nil, err}
    }
    qr, err := c.quorum(ring, key, fn)
    if qr != nil && err == nil {
        rev = qr.rev
        err = qr.err
    }
    return rev, err
}

type ListResult struct {
    items map[core.Key]uint
    err   error
}

func (c *Cluster) doList(node *core.Node, ns core.Namespace) (map[core.Key]uint, error) {
    if node == c.Nodes.Self() {
        utils.Print("QUORUM", "    LIST-LOCAL")
        return c.Data.DataList(ns)
    }

    utils.Print("QUORUM", "    LIST-REMOTE")
    cl := c.getClient(node.API())
    defer cl.Close()
    return cl.NSDataList(ns, false)
}

func (c *Cluster) allList(ns core.Namespace) (map[core.Key]uint, error) {
    nodes := c.Nodes.Inuse()
    items := make(map[core.Key]uint)

    utils.Print("QUORUM", "LIST %d", len(nodes))

    // Setup all the requests.
    fn := func(node *core.Node, res chan<- *ListResult) {
        items, err := c.doList(node, ns)
        res <- &ListResult{items, err}
    }
    reschan := make(chan *ListResult)
    for _, node := range nodes {
        go fn(node, reschan)
    }

    // Read all results.
    for i, _ := range nodes {
        res := <-reschan
        if res.items != nil {
            utils.Print("QUORUM", "  %d %d", i, len(res.items))
            for item, count := range res.items {
                items[item] = items[item] + count
            }
        }
        if res.err != nil {
            utils.Print("QUORUM", "  %d %s", i, res.err)
        }
    }

    // Return our result.
    return items, nil
}
