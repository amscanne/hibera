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
    key string
}

func (n *NotActivatedError) Error() string {
    return ""
}

func (r *Redirect) Error() string {
    return r.URL
}

func (p *PermissionError) Error() string {
    return p.key
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
        return false, &PermissionError{string(key)}
    }

    // Check for other servers.
    if c.Nodes.Heartbeat(conn.ServerId(), 0) {
        utils.Print("CLUSTER", "REDIRECT IS-SERVER key=%s", string(key))
        return true, nil
    }

    // Redirect to the master.
    master := c.ring.MasterFor(key)
    if master == nil || master == c.Nodes.Self() {
        // Check that the cluster is activated.
        if !c.Active() {
            utils.Print("CLUSTER", "REDIRECT NOT-ACTIVATED key=%s", string(key))
            return false, &NotActivatedError{}
        } else {
            utils.Print("CLUSTER", "REDIRECT IS-MASTER key=%s", string(key))
            return false, nil
        }
    }

    utils.Print("CLUSTER", "REDIRECT OKAY key=%s", string(key))
    return false, &Redirect{master.Addr}
}

func (c *Cluster) Authorize(conn Connection, key core.Key, read bool, write bool, execute bool) error {
    // Check for a token.
    if c.Access.Check(conn.Namespace(), conn.Auth(), string(key), read, write, execute) {
        utils.Print("CLUSTER", "AUTHORIZE OKAY ns=%s auth=%s key=%s",
            conn.Namespace(), conn.Auth(), string(key))
        return nil
    }

    // No permissions available.
    utils.Print("CLUSTER", "AUTHORIZE FAIL ns=%s auth=%s key=%s",
        conn.Namespace(), conn.Auth(), string(key))
    return &PermissionError{string(key)}
}

func (c *Cluster) Info(conn Connection) ([]byte, core.Revision, error) {
    utils.Print("CLUSTER", "INFO")

    err := c.Authorize(conn, RootKey, true, false, false)
    if err != nil {
        return nil, 0, err
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

func (c *Cluster) Activate(conn Connection) (core.Revision, error) {
    utils.Print("CLUSTER", "ACTIVATE")

    err := c.Authorize(conn, RootKey, true, true, false)
    if err != nil {
        return c.rev, err
    }

    // NOTE: The activate call is sent to a specific node,
    // we do not redirect to the master node in this case.
    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    return c.rev, c.lockedActivate()
}

func (c *Cluster) Deactivate(conn Connection) (core.Revision, error) {
    utils.Print("CLUSTER", "DEACTIVATE")

    err := c.Authorize(conn, RootKey, true, true, false)
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

    err := c.Authorize(conn, RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    nodes, err := c.Nodes.List(active)
    return nodes, c.rev, err
}

func (c *Cluster) NodeGet(conn Connection, id string) (*core.Node, core.Revision, error) {
    utils.Print("CLUSTER", "NODE-LIST")

    err := c.Authorize(conn, RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    node, err := c.Nodes.Get(id)
    return node, c.rev, err
}

func (c *Cluster) DataList(conn Connection) ([]core.Key, core.Revision, error) {
    utils.Print("CLUSTER", "DATA-LIST")

    err := c.Authorize(conn, RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, RootKey)
    if err != nil {
        return nil, c.rev, err
    }

    if server {
        items, err := c.Data.DataList()
        return items, c.rev, err
    }
    items, err := c.allList()
    return items, c.rev, err
}

func (c *Cluster) DataGet(conn Connection, key core.Key, rev core.Revision, timeout uint) ([]byte, core.Revision, error) {
    utils.Print("CLUSTER", "DATA-GET key=%s", string(key))

    err := c.Authorize(conn, key, true, false, false)
    if err != nil {
        return nil, core.Revision(0), err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil {
        return nil, core.Revision(0), err
    }

    if !server && key == RootKey {
        return nil, core.Revision(0), &PermissionError{string(key)}
    }
    valid := func() bool { return c.ring.IsMaster(key) }
    conn.DisableTimeouts()
    return c.Data.DataWatch(conn.EphemId(), key, rev, timeout, conn.Notifier(), valid)
}

func (c *Cluster) DataSet(conn Connection, key core.Key, rev core.Revision, value []byte) (core.Revision, error) {
    utils.Print("CLUSTER", "DATA-SET key=%s len(value)=%d rev=%d", string(key), len(value), uint64(rev))

    err := c.Authorize(conn, key, false, true, false)
    if err != nil {
        return core.Revision(0), err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil {
        return core.Revision(0), err
    }

    if server {
        return c.Data.DataSet(key, rev, value)
    }
    if key == RootKey {
        return core.Revision(0), &PermissionError{string(key)}
    }
    return c.quorumSet(c.ring, key, rev, value)
}

func (c *Cluster) DataRemove(conn Connection, key core.Key, rev core.Revision) (core.Revision, error) {
    utils.Print("CLUSTER", "DATA-REMOVE key=%s rev=%d", string(key), uint64(rev))

    err := c.Authorize(conn, key, false, true, false)
    if err != nil {
        return core.Revision(0), err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil {
        return core.Revision(0), err
    }

    if server {
        return c.Data.DataRemove(key, rev)
    }
    if key == RootKey {
        return c.rev, &PermissionError{string(key)}
    }
    return c.quorumRemove(c.ring, key, rev)
}

func (c *Cluster) SyncMembers(conn Connection, key core.Key, name string, limit uint) (int, []string, core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-MEMBERS key=%s name=%s limit=%d", string(key), name, limit)

    err := c.Authorize(conn, key, true, false, true)
    if err != nil {
        return -1, nil, core.Revision(0), err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return -1, nil, core.Revision(0), err
    }

    return c.Data.SyncMembers(key, name, limit)
}

func (c *Cluster) SyncJoin(conn Connection, key core.Key, name string, limit uint, timeout uint) (int, core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-JOIN key=%s name=%s limit=%d timeout=%d", string(key), name, limit, timeout)

    err := c.Authorize(conn, key, false, true, true)
    if err != nil {
        return -1, core.Revision(0), err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return -1, core.Revision(0), err
    }

    valid := func() bool { return c.ring.IsMaster(key) }
    conn.DisableTimeouts()
    return c.Data.SyncJoin(conn.EphemId(), key, name, limit, timeout, conn.Notifier(), valid)
}

func (c *Cluster) SyncLeave(conn Connection, key core.Key, name string) (core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-LEAVE key=%s name=%s", string(key), name)

    err := c.Authorize(conn, key, false, true, true)
    if err != nil {
        return core.Revision(0), err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return core.Revision(0), err
    }

    return c.Data.SyncLeave(conn.EphemId(), key, name)
}

func (c *Cluster) EventWait(conn Connection, key core.Key, rev core.Revision, timeout uint) (core.Revision, error) {
    utils.Print("CLUSTER", "EVENT-WAIT key=%s rev=%d timeout=%d", string(key), uint64(rev), timeout)

    err := c.Authorize(conn, key, true, false, true)
    if err != nil {
        return core.Revision(0), err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return core.Revision(0), err
    }

    valid := func() bool { return c.ring.IsMaster(key) }
    conn.DisableTimeouts()
    return c.Data.EventWait(conn.EphemId(), key, rev, timeout, conn.Notifier(), valid)
}

func (c *Cluster) EventFire(conn Connection, key core.Key, rev core.Revision) (core.Revision, error) {
    utils.Print("CLUSTER", "EVENT-FIRE key=%s rev=%d", string(key), uint64(rev))

    err := c.Authorize(conn, key, false, true, true)
    if err != nil {
        return core.Revision(0), err
    }

    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return core.Revision(0), err
    }

    return c.Data.EventFire(key, rev)
}

func (c *Cluster) AccessList(conn Connection) ([]string, core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-LIST")

    err := c.Authorize(conn, RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    items := c.Access.List(conn.Namespace())
    return items, c.rev, nil
}

func (c *Cluster) AccessGet(conn Connection, key string) (*core.Token, core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-GET key=%s", string(key))

    err := c.Authorize(conn, RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    value, err := c.Access.Get(conn.Namespace(), key)
    return value, c.rev, err
}

func (c *Cluster) AccessGrant(conn Connection, key string, path string, read bool, write bool, execute bool) (core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-GRANT key=%s", string(key))

    err := c.Authorize(conn, RootKey, false, true, false)
    if err != nil {
        return c.rev, err
    }

    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return c.rev, err
    }

    err = c.Access.Update(conn.Namespace(), key, path, read, write, execute)
    return c.rev, err
}

func (c *Cluster) AccessRevoke(conn Connection, key string) (core.Revision, error) {
    utils.Print("CLUSTER", "ACCESS-REVOKE key=%s", string(key))

    err := c.Authorize(conn, RootKey, false, true, false)
    if err != nil {
        return c.rev, err
    }

    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return c.rev, err
    }

    c.Access.Remove(conn.Namespace(), key)
    return c.rev, nil
}
