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
    return p.Key.Key
}

type Connection interface {
    // The server id of the underlying connection.
    ServerId() string

    // Authentication token used by the connection.
    Auth() string

    // Namespace used by the connection.
    Namespace() core.Namespace

    // Disable timeouts.
    DisableTimeouts()

    // A channel which will be activated when the
    // unerlying connection drops.
    Notifier() <-chan bool

    // Return the ephemeral id of this connection.
    EphemId() core.EphemId
}

func (c *Cluster) doRedirect(conn Connection, key core.Key) (bool, error) {
    // Check that the cluster is activated.
    if !c.Active() {
        return false, &PermissionError{key}
    }

    // Check for other servers.
    if c.Nodes.Heartbeat(conn.ServerId(), core.ZeroRevision) {
        utils.Print("CLUSTER", "REDIRECT IS-SERVER key=%s", key.String())
        return true, nil
    }

    // Redirect to the master.
    master := c.ring.MasterFor(key)
    if master == nil || master == c.Nodes.Self() {
        // Check that the cluster is activated.
        if !c.Active() {
            utils.Print("CLUSTER", "REDIRECT NOT-ACTIVATED key=%s", key.String())
            return false, &NotActivatedError{}
        } else {
            utils.Print("CLUSTER", "REDIRECT IS-MASTER key=%s", key.String())
            return false, nil
        }
    }

    utils.Print("CLUSTER", "REDIRECT OKAY key=%s", key.String())
    return false, &Redirect{master.Addr}
}

func (c *Cluster) Authorize(key core.Key, auth string, read bool, write bool, execute bool) error {
    // Check for a token.
    if c.Access.Check(key, auth, read, write, execute) {
        utils.Print("CLUSTER", "AUTHORIZE OKAY key=%s auth=%s",
            key.String(), auth)
        return nil
    }

    // No permissions available.
    utils.Print("CLUSTER", "AUTHORIZE FAIL key=%s auth=%s",
        key.String(), auth)
    return &PermissionError{key}
}

func (c *Cluster) Info(conn Connection) ([]byte, core.Revision, error) {
    utils.Print("CLUSTER", "INFO")

    err := c.Authorize(HiberaKey, conn.Auth(), true, false, false)
    if err != nil {
        return nil, core.ZeroRevision, err
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

func (c *Cluster) Activate(conn Connection, N uint) (core.Revision, error) {
    utils.Print("CLUSTER", "ACTIVATE")

    err := c.Authorize(HiberaKey, conn.Auth(), true, true, false)
    if err != nil {
        return c.rev, err
    }

    if c.Active() {
        // If we're already activated, then this call
        // can be used just to change the replication
        // factor used across the cluster. Simply send
        // the client to the current master node.
        _, err = c.doRedirect(conn, HiberaKey)
        if err != nil {
            return c.rev, err
        }
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    return c.rev, c.lockedActivate(N)
}

func (c *Cluster) Deactivate(conn Connection) (core.Revision, error) {
    utils.Print("CLUSTER", "DEACTIVATE")

    err := c.Authorize(HiberaKey, conn.Auth(), true, true, false)
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

func (c *Cluster) NodeList(conn Connection, active bool) ([]string, core.Revision, error) {
    utils.Print("CLUSTER", "NODE-LIST")

    err := c.Authorize(HiberaKey, conn.Auth(), true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, HiberaKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    nodes, err := c.Nodes.List(active)
    return nodes, c.rev, err
}

func (c *Cluster) NodeGet(conn Connection, id string) (*core.Node, core.Revision, error) {
    utils.Print("CLUSTER", "NODE-LIST")

    err := c.Authorize(HiberaKey, conn.Auth(), true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, HiberaKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    node, err := c.Nodes.Get(id)
    return node, c.rev, err
}

func (c *Cluster) DataList(conn Connection) ([]core.Key, core.Revision, error) {
    utils.Print("CLUSTER", "DATA-LIST")

    err := c.Authorize(RootKeyFor(conn.Namespace()), conn.Auth(), true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, RootKeyFor(conn.Namespace()))
    if err != nil {
        return nil, c.rev, err
    }

    if server {
        items, err := c.Data.DataList(conn.Namespace())
        return items, c.rev, err
    }
    items, err := c.allList(conn.Namespace())
    return items, c.rev, err
}

func (c *Cluster) DataGet(conn Connection, key core.Key, rev core.Revision, timeout uint) ([]byte, core.Revision, error) {
    utils.Print("CLUSTER", "DATA-GET key=%s", key.String())

    err := c.Authorize(key, conn.Auth(), true, false, false)
    if err != nil {
        return nil, core.ZeroRevision, err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil {
        return nil, core.ZeroRevision, err
    }

    if !server && key == HiberaKey {
        return nil, core.ZeroRevision, &PermissionError{key}
    }
    valid := func() bool { return c.ring.IsMaster(key) }
    conn.DisableTimeouts()
    return c.Data.DataWatch(conn.EphemId(), key, rev, timeout, conn.Notifier(), valid)
}

func (c *Cluster) DataSet(conn Connection, key core.Key, rev core.Revision, value []byte) (core.Revision, error) {
    utils.Print("CLUSTER", "DATA-SET key=%s len(value)=%d rev=%s", key.String(), len(value), (*rev).String())

    err := c.Authorize(key, conn.Auth(), false, true, false)
    if err != nil {
        return core.ZeroRevision, err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil {
        return core.ZeroRevision, err
    }

    if server {
        return c.Data.DataSet(key, rev, value)
    }
    if key == HiberaKey {
        return core.ZeroRevision, &PermissionError{key}
    }
    return c.quorumSet(c.ring, key, rev, value)
}

func (c *Cluster) DataRemove(conn Connection, key core.Key, rev core.Revision) (core.Revision, error) {
    utils.Print("CLUSTER", "DATA-REMOVE key=%s rev=%s", key.String(), (*rev).String())

    err := c.Authorize(key, conn.Auth(), false, true, false)
    if err != nil {
        return core.ZeroRevision, err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil {
        return core.ZeroRevision, err
    }

    if server {
        return c.Data.DataRemove(key, rev)
    }
    if key == HiberaKey {
        return c.rev, &PermissionError{key}
    }
    return c.quorumRemove(c.ring, key, rev)
}

func (c *Cluster) SyncMembers(conn Connection, key core.Key, name string, limit uint) (core.SyncInfo, core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-MEMBERS key=%s name=%s limit=%d", key.String(), name, limit)

    err := c.Authorize(key, conn.Auth(), true, false, true)
    if err != nil {
        return core.NoSyncInfo, core.ZeroRevision, err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return core.NoSyncInfo, core.ZeroRevision, err
    }

    return c.Data.SyncMembers(key, name, limit)
}

func (c *Cluster) SyncJoin(conn Connection, key core.Key, name string, limit uint, timeout uint) (int, core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-JOIN key=%s name=%s limit=%d timeout=%d", key.String(), name, limit, timeout)

    err := c.Authorize(key, conn.Auth(), false, true, true)
    if err != nil {
        return -1, core.ZeroRevision, err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return -1, core.ZeroRevision, err
    }

    valid := func() bool { return c.ring.IsMaster(key) }
    conn.DisableTimeouts()
    return c.Data.SyncJoin(conn.EphemId(), key, name, limit, timeout, conn.Notifier(), valid)
}

func (c *Cluster) SyncLeave(conn Connection, key core.Key, name string) (core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-LEAVE key=%s name=%s", key.String(), name)

    err := c.Authorize(key, conn.Auth(), false, true, true)
    if err != nil {
        return core.ZeroRevision, err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return core.ZeroRevision, err
    }

    return c.Data.SyncLeave(conn.EphemId(), key, name)
}

func (c *Cluster) EventWait(conn Connection, key core.Key, rev core.Revision, timeout uint) (core.Revision, error) {
    utils.Print("CLUSTER", "EVENT-WAIT key=%s rev=%s timeout=%d", key.String(), (*rev).String(), timeout)

    err := c.Authorize(key, conn.Auth(), true, false, true)
    if err != nil {
        return core.ZeroRevision, err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return core.ZeroRevision, err
    }

    valid := func() bool { return c.ring.IsMaster(key) }
    conn.DisableTimeouts()
    return c.Data.EventWait(conn.EphemId(), key, rev, timeout, conn.Notifier(), valid)
}

func (c *Cluster) EventFire(conn Connection, key core.Key, rev core.Revision) (core.Revision, error) {
    utils.Print("CLUSTER", "EVENT-FIRE key=%s rev=%s", key.String(), (*rev).String())

    err := c.Authorize(key, conn.Auth(), false, true, true)
    if err != nil {
        return core.ZeroRevision, err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return core.ZeroRevision, err
    }

    return c.Data.EventFire(key, rev)
}

func (c *Cluster) AccessList(conn Connection) ([]string, core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-LIST")

    err := c.Authorize(RootKeyFor(conn.Namespace()), conn.Auth(), true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, RootKeyFor(conn.Namespace()))
    if err != nil || server {
        return nil, c.rev, err
    }

    items := c.Access.List(conn.Namespace())
    return items, c.rev, nil
}

func (c *Cluster) AccessGet(conn Connection, auth core.Key) (*core.Token, core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-GET auth=%s", auth.String())

    if auth.Namespace != conn.Namespace() {
        return nil, core.ZeroRevision, &PermissionError{auth}
    }

    err := c.Authorize(RootKeyFor(conn.Namespace()), conn.Auth(), true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, RootKeyFor(conn.Namespace()))
    if err != nil || server {
        return nil, c.rev, err
    }

    value, err := c.Access.Get(auth)
    return value, c.rev, err
}

func (c *Cluster) AccessUpdate(conn Connection, auth core.Key, path string, read bool, write bool, execute bool) (core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-UPDATE auth=%s path=%s", auth.String(), path)

    if auth.Namespace != conn.Namespace() {
        return core.ZeroRevision, &PermissionError{auth}
    }

    err := c.Authorize(RootKeyFor(conn.Namespace()), conn.Auth(), false, true, false)
    if err != nil {
        return c.rev, err
    }

    server, err := c.doRedirect(conn, RootKeyFor(conn.Namespace()))
    if err != nil || server {
        return c.rev, err
    }

    err = c.Access.Update(auth, path, read, write, execute)
    return c.rev, err
}
