package cluster

import (
    "hibera/core"
    "hibera/utils"
)

type Redirect struct {
    URL string
}

type NotActivatedError struct {
}

type PermissionError struct {
    core.Key
}

func (n *NotActivatedError) Error() string {
    return ""
}

func (r *Redirect) Error() string {
    return r.URL
}

func (p *PermissionError) Error() string {
    return string(p.Key)
}

type Request interface {
    // The server id of the request.
    ServerId() string

    // Disable timeouts on the connection.
    DisableTimeouts()

    // A channel which will be activated when the
    // unerlying connection drops.
    Notifier() <-chan bool

    // Return the ephemeral id of this request.
    EphemId() core.EphemId

    // Authentication token for this request.
    Auth() core.Token

    // Namespace used for this request.
    Namespace() core.Namespace
}

func (c *Cluster) doRedirect(req Request, key core.Key) (bool, error) {
    // Check that the cluster is activated.
    if !c.Active() {
        return false, &NotActivatedError{}
    }

    // Check for other servers.
    if c.Nodes.Heartbeat(req.ServerId(), core.NoRevision) {
        utils.Print("CLUSTER", "REDIRECT IS-SERVER key=%s", key)
        return true, nil
    }

    // Redirect to the master.
    master := c.ring.MasterFor(key)
    if master == nil || master == c.Nodes.Self() {
        utils.Print("CLUSTER", "REDIRECT IS-MASTER key=%s", key)
        return false, nil
    }

    utils.Print("CLUSTER", "REDIRECT OKAY key=%s", key)
    return false, &Redirect{master.Addr}
}

func (c *Cluster) Authorize(ns core.Namespace, auth core.Token, key core.Key, read bool, write bool, execute bool) error {
    // Check for a token.
    if c.Access.Check(ns, auth, key, read, write, execute) {
        return nil
    }

    // No permissions available.
    utils.Print("CLUSTER", "AUTHORIZE FAIL namespace=%s key=%s", ns, key)
    return &PermissionError{key}
}

func (c *Cluster) Info(req Request) ([]byte, core.Revision, error) {
    utils.Print("CLUSTER", "INFO")

    err := c.Authorize(req.Namespace(), req.Auth(), RootKey, true, false, false)
    if err != nil {
        return nil, core.NoRevision, err
    }

    // NOTE: We fetch the cluster info from the current node,
    // this may not be the *current* cluster info, but since
    // it may have changed by the time the result gets back to
    // the client anyways, it makes sense to handle this here.
    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    bytes, err := c.lockedEncode(false)
    return bytes, c.rev, err
}

func (c *Cluster) Activate(req Request, N uint) (core.Revision, error) {
    utils.Print("CLUSTER", "ACTIVATE")

    err := c.Authorize(req.Namespace(), req.Auth(), RootKey, true, true, false)
    if err != nil {
        return c.rev, err
    }

    if c.Active() {
        // If we're already activated, then this call
        // can be used just to change the replication
        // factor used across the cluster. Simply send
        // the client to the current master node.
        _, err = c.doRedirect(req, RootKey)
        if err != nil {
            return c.rev, err
        }
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    return c.rev, c.lockedActivate(N)
}

func (c *Cluster) Deactivate(req Request) (core.Revision, error) {
    utils.Print("CLUSTER", "DEACTIVATE")

    err := c.Authorize(req.Namespace(), req.Auth(), RootKey, true, true, false)
    if err != nil {
        return c.rev, err
    }

    // NOTE: The deactivate call is sent to a specific node,
    // we do not redirect to the master node in this case.
    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    return c.rev, c.lockedDeactivate()
}

func (c *Cluster) Version() core.Revision {
    return c.rev
}

func (c *Cluster) NodeList(req Request, active bool) ([]string, core.Revision, error) {
    utils.Print("CLUSTER", "NODE-LIST")

    err := c.Authorize(req.Namespace(), req.Auth(), RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(req, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    nodes, err := c.Nodes.List(active)
    return nodes, c.rev, err
}

func (c *Cluster) NodeGet(req Request, id string) (*core.Node, core.Revision, error) {
    utils.Print("CLUSTER", "NODE-LIST")

    err := c.Authorize(req.Namespace(), req.Auth(), RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(req, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    node, err := c.Nodes.Get(id)
    return node, c.rev, err
}

func (c *Cluster) DataList(req Request) ([]core.Key, core.Revision, error) {
    utils.Print("CLUSTER", "DATA-LIST")

    err := c.Authorize(req.Namespace(), req.Auth(), RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(req, RootKey)
    if err != nil {
        return nil, c.rev, err
    }

    if server {
        items, err := c.Data.DataList(req.Namespace())
        return items, c.rev, err
    }
    items, err := c.allList(req.Namespace())
    return items, c.rev, err
}

func (c *Cluster) DataGet(req Request, key core.Key, rev core.Revision, timeout uint) ([]byte, core.Revision, error) {
    utils.Print("CLUSTER", "DATA-GET key=%s", key)

    err := c.Authorize(req.Namespace(), req.Auth(), key, true, false, false)
    if err != nil {
        return nil, core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil {
        return nil, core.NoRevision, err
    }

    if !server && key == RootKey {
        return nil, core.NoRevision, &PermissionError{key}
    }
    valid := func() bool { return c.ring.IsMaster(key) }
    req.DisableTimeouts()
    return c.Data.DataWatch(req.EphemId(), req.Namespace(), key, rev, timeout, req.Notifier(), valid)
}

func (c *Cluster) DataSet(req Request, key core.Key, rev core.Revision, value []byte) (core.Revision, error) {
    utils.Print("CLUSTER", "DATA-SET key=%s len(value)=%d rev=%s", key, len(value), rev.String())

    err := c.Authorize(req.Namespace(), req.Auth(), key, false, true, false)
    if err != nil {
        return core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil {
        return core.NoRevision, err
    }

    if server {
        return c.Data.DataSet(req.Namespace(), key, rev, value)
    }
    if key == RootKey {
        return core.NoRevision, &PermissionError{key}
    }
    return c.quorumSet(c.ring, req.Namespace(), key, rev, value)
}

func (c *Cluster) DataRemove(req Request, key core.Key, rev core.Revision) (core.Revision, error) {
    utils.Print("CLUSTER", "DATA-REMOVE key=%s rev=%s", key, rev.String())

    err := c.Authorize(req.Namespace(), req.Auth(), key, false, true, false)
    if err != nil {
        return core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil {
        return core.NoRevision, err
    }

    if server {
        return c.Data.DataRemove(req.Namespace(), key, rev)
    }
    if key == RootKey {
        return c.rev, &PermissionError{key}
    }
    return c.quorumRemove(c.ring, req.Namespace(), key, rev)
}

func (c *Cluster) SyncMembers(req Request, key core.Key, name string, limit uint) (core.SyncInfo, core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-MEMBERS key=%s name=%s limit=%d", key, name, limit)

    err := c.Authorize(req.Namespace(), req.Auth(), key, true, false, true)
    if err != nil {
        return core.NoSyncInfo, core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil || server {
        return core.NoSyncInfo, core.NoRevision, err
    }

    return c.Data.SyncMembers(req.Namespace(), key, name, limit)
}

func (c *Cluster) SyncJoin(req Request, key core.Key, name string, limit uint, timeout uint) (int, core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-JOIN key=%s name=%s limit=%d timeout=%d", key, name, limit, timeout)

    err := c.Authorize(req.Namespace(), req.Auth(), key, false, true, true)
    if err != nil {
        return -1, core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil || server {
        return -1, core.NoRevision, err
    }

    valid := func() bool { return c.ring.IsMaster(key) }
    req.DisableTimeouts()
    return c.Data.SyncJoin(req.EphemId(), req.Namespace(), key, name, limit, timeout, req.Notifier(), valid)
}

func (c *Cluster) SyncLeave(req Request, key core.Key, name string) (core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-LEAVE key=%s name=%s", key, name)

    err := c.Authorize(req.Namespace(), req.Auth(), key, false, true, true)
    if err != nil {
        return core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil || server {
        return core.NoRevision, err
    }

    return c.Data.SyncLeave(req.EphemId(), req.Namespace(), key, name)
}

func (c *Cluster) EventWait(req Request, key core.Key, rev core.Revision, timeout uint) (core.Revision, error) {
    utils.Print("CLUSTER", "EVENT-WAIT key=%s rev=%s timeout=%d", key, rev.String(), timeout)

    err := c.Authorize(req.Namespace(), req.Auth(), key, true, false, true)
    if err != nil {
        return core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil || server {
        return core.NoRevision, err
    }

    valid := func() bool { return c.ring.IsMaster(key) }
    req.DisableTimeouts()
    return c.Data.EventWait(req.EphemId(), req.Namespace(), key, rev, timeout, req.Notifier(), valid)
}

func (c *Cluster) EventFire(req Request, key core.Key, rev core.Revision) (core.Revision, error) {
    utils.Print("CLUSTER", "EVENT-FIRE key=%s rev=%s", key, rev.String())

    err := c.Authorize(req.Namespace(), req.Auth(), key, false, true, true)
    if err != nil {
        return core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil || server {
        return core.NoRevision, err
    }

    return c.Data.EventFire(req.Namespace(), key, rev)
}

func (c *Cluster) AccessList(req Request) ([]core.Token, core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-LIST")

    // NOTE: For access operations, we also authenticate
    // against the root namespace. This is because namespace
    // updates can generate cluster revision updates, we
    // don't want to allow these for arbitrary API users.
    err := c.Authorize(RootNamespace, req.Auth(), RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(req, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    items := c.Access.List(req.Namespace())
    return items, c.rev, nil
}

func (c *Cluster) AccessGet(req Request, auth core.Token) (*core.Permissions, core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-GET auth=%s", auth)

    // NOTE: See AccessList() above.
    err := c.Authorize(RootNamespace, req.Auth(), RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(req, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    value, err := c.Access.Get(req.Namespace(), auth)
    return value, c.rev, err
}

func (c *Cluster) AccessUpdate(req Request, auth core.Token, key core.Key, read bool, write bool, execute bool) (core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-UPDATE auth=%s key=%s", auth, key)

    // NOTE: See AccessList() above.
    err := c.Authorize(RootNamespace, req.Auth(), RootKey, false, true, false)
    if err != nil {
        return c.rev, err
    }

    server, err := c.doRedirect(req, RootKey)
    if err != nil || server {
        return c.rev, err
    }

    err = c.Access.Update(req.Namespace(), auth, key, read, write, execute)
    return c.rev, err
}
