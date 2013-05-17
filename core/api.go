package core

import (
    "hibera/utils"
)

type Redirect struct {
    URL string
}

type PermissionError struct {
}

func (r *Redirect) Error() string {
    return r.URL
}

func (r *PermissionError) Error() string {
    return ""
}

func (c *Cluster) doRedirect(conn *Connection, key Key) (bool, error) {
    // Check for other servers.
    if c.Nodes.Heartbeat(conn.Name("")) {
        return true, nil
    }

    // Redirect to the master.
    master := c.ring.MasterFor(key)
    if master == nil || master == c.Nodes.Self() {
        return false, nil
    }

    return false, &Redirect{master.Addr}
}

func (c *Cluster) Authorize(auth string, key Key, read bool, write bool, execute bool) error {
    // Check for a token.
    if c.Access.Check(auth, string(key), read, write, execute) {
        return nil
    }

    // No permissions available.
    return &PermissionError{}
}

func (c *Cluster) Info(conn *Connection, rev Revision) (string, []byte, Revision, error) {
    utils.Print("CLUSTER", "INFO")
    err := c.Authorize(conn.Auth(), RootKey, true, false, false)
    if err != nil {
        return "", nil, 0, err
    }
    // NOTE: We fetch the cluster info from the current node,
    // this may not be the *current* cluster info, but since
    // it may have changed by the time the result gets back to
    // the client anyways, it makes sense to handle this here.
    bytes, err := c.doEncode(rev, false)
    return c.id, bytes, c.rev, err
}

func (c *Cluster) Activate(conn *Connection) (Revision, error) {
    utils.Print("CLUSTER", "ACTIVATE")
    err := c.Authorize(conn.Auth(), RootKey, true, true, false)
    if err != nil {
        return c.rev, err
    }
    // NOTE: The activate call is sent to a specific node,
    // we do not redirect to the master node in this case.
    return c.rev, c.doActivate()
}

func (c *Cluster) Deactivate(conn *Connection) (Revision, error) {
    utils.Print("CLUSTER", "DEACTIVATE")
    err := c.Authorize(conn.Auth(), RootKey, true, true, false)
    if err != nil {
        return c.rev, err
    }
    // NOTE: The deactivate call *does* go back to the master. It
    // will in fact, deactivate the entire cluster (so as to not 
    // leave some small subset hanging around, provided everything
    // is online at the time).
    server, err := c.doRedirect(conn, RootKey)
    if err != nil {
        return c.rev, err
    }
    if server {
        return c.rev, c.doDeactivate()
    }
    return c.rev, c.allDeactivate()
}

func (c *Cluster) Id() string {
    return c.Nodes.Self().Id()
}

func (c *Cluster) Version() Revision {
    return c.rev
}

func (c *Cluster) List(conn *Connection) ([]Key, Revision, error) {
    utils.Print("CLUSTER", "DATA-LIST")
    err := c.Authorize(conn.Auth(), RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }
    server, err := c.doRedirect(conn, RootKey)
    if err != nil {
        return nil, c.rev, err
    }
    if server {
        items, err := c.data.DataList()
        return items, c.rev, err
    }
    items, err := c.allList()
    return items, c.rev, err
}

func (c *Cluster) Get(conn *Connection, key Key, rev Revision, timeout uint) ([]byte, Revision, error) {
    utils.Print("CLUSTER", "DATA-GET key=%s", string(key))
    err := c.Authorize(conn.Auth(), key, true, false, false)
    if err != nil {
        return nil, Revision(0), err
    }
    _, err = c.doRedirect(conn, key)
    if err != nil {
        return nil, Revision(0), err
    }
    valid := func() bool { return c.ring.IsMaster(key) }
    return c.data.DataWatch(conn.EphemId(), key, rev, timeout, *conn.notifier, valid)
}

func (c *Cluster) Set(conn *Connection, key Key, rev Revision, value []byte) (Revision, error) {
    utils.Print("CLUSTER", "DATA-SET key=%s len(value)=%d rev=%d", string(key), len(value), uint64(rev))
    err := c.Authorize(conn.Auth(), key, false, true, false)
    if err != nil {
        return Revision(0), err
    }
    server, err := c.doRedirect(conn, key)
    if err != nil {
        return Revision(0), err
    }
    if server {
        return c.data.DataSet(key, rev, value)
    }
    return c.quorumSet(c.ring, key, rev, value)
}

func (c *Cluster) Remove(conn *Connection, key Key, rev Revision) (Revision, error) {
    utils.Print("CLUSTER", "DATA-REMOVE key=%s rev=%d", string(key), uint64(rev))
    err := c.Authorize(conn.Auth(), key, false, true, false)
    if err != nil {
        return Revision(0), err
    }
    server, err := c.doRedirect(conn, key)
    if err != nil {
        return Revision(0), err
    }
    if server {
        return c.data.DataRemove(key, rev)
    }
    return c.quorumRemove(c.ring, key, rev)
}

func (c *Cluster) Clear(conn *Connection) (Revision, error) {
    utils.Print("CLUSTER", "DATA-CLEAR")
    err := c.Authorize(conn.Auth(), RootKey, false, true, false)
    if err != nil {
        return c.rev, err
    }
    server, err := c.doRedirect(conn, RootKey)
    if err != nil {
        return c.rev, err
    }
    if server {
        return c.rev, c.data.DataClear()
    }
    return c.rev, c.allClear()
}

func (c *Cluster) Members(conn *Connection, key Key, name string, limit uint) (int, []string, Revision, error) {
    utils.Print("CLUSTER", "SYNC-MEMBERS key=%s name=%s limit=%d", string(key), name, limit)
    err := c.Authorize(conn.Auth(), key, true, false, true)
    if err != nil {
        return -1, nil, Revision(0), err
    }
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return -1, nil, Revision(0), err
    }
    return c.data.SyncMembers(key, name, limit)
}

func (c *Cluster) Join(conn *Connection, key Key, name string, limit uint, timeout uint) (int, Revision, error) {
    utils.Print("CLUSTER", "SYNC-JOIN key=%s name=%s limit=%d timeout=%d", string(key), name, limit, timeout)
    err := c.Authorize(conn.Auth(), key, false, true, true)
    if err != nil {
        return -1, Revision(0), err
    }
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return -1, Revision(0), err
    }
    valid := func() bool { return c.ring.IsMaster(key) }
    return c.data.SyncJoin(conn.EphemId(), key, name, limit, timeout, *conn.notifier, valid)
}

func (c *Cluster) Leave(conn *Connection, key Key, name string) (Revision, error) {
    utils.Print("CLUSTER", "SYNC-LEAVE key=%s name=%s", string(key), name)
    err := c.Authorize(conn.Auth(), key, false, true, true)
    if err != nil {
        return Revision(0), err
    }
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return Revision(0), err
    }
    return c.data.SyncLeave(conn.EphemId(), key, name)
}

func (c *Cluster) Wait(conn *Connection, key Key, rev Revision, timeout uint) (Revision, error) {
    utils.Print("CLUSTER", "EVENT-WAIT key=%s rev=%d timeout=%d", string(key), uint64(rev), timeout)
    err := c.Authorize(conn.Auth(), key, true, false, true)
    if err != nil {
        return Revision(0), err
    }
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return Revision(0), err
    }
    valid := func() bool { return c.ring.IsMaster(key) }
    return c.data.EventWait(conn.EphemId(), key, rev, timeout, *conn.notifier, valid)
}

func (c *Cluster) Fire(conn *Connection, key Key, rev Revision) (Revision, error) {
    utils.Print("CLUSTER", "EVENT-FIRE key=%s rev=%d", string(key), uint64(rev))
    err := c.Authorize(conn.Auth(), key, false, true, true)
    if err != nil {
        return Revision(0), err
    }
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return Revision(0), err
    }
    return c.data.EventFire(key, rev)
}

func (c *Cluster) AuthList(conn *Connection) ([]string, Revision, error) {
    utils.Print("CLUSTER", "AUTH-LIST")
    err := c.Authorize(conn.Auth(), RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    items := c.Access.List()
    return items, c.rev, nil
}

func (c *Cluster) AuthGet(conn *Connection, key string) ([]byte, Revision, error) {
    utils.Print("CLUSTER", "AUTH-GET key=%s", string(key))
    err := c.Authorize(conn.Auth(), RootKey, true, false, false)
    if err != nil {
        return nil, c.rev, err
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return nil, c.rev, err
    }

    value, err := c.Access.Get(key)
    return value, c.rev, err
}

func (c *Cluster) AuthSet(conn *Connection, key string, value []byte) (Revision, error) {
    utils.Print("CLUSTER", "AUTH-SET key=%s", string(key))
    err := c.Authorize(conn.Auth(), RootKey, false, true, false)
    if err != nil {
        return c.rev, err
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return c.rev, err
    }

    err = c.Access.Set(key, value)
    return c.rev, err
}

func (c *Cluster) AuthRemove(conn *Connection, key string) (Revision, error) {
    utils.Print("CLUSTER", "AUTH-REMOVE key=%s", string(key))
    err := c.Authorize(conn.Auth(), RootKey, false, true, false)
    if err != nil {
        return c.rev, err
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return c.rev, err
    }

    c.Access.Remove(key)
    return c.rev, nil
}

func (c *Cluster) AuthClear(conn *Connection) (Revision, error) {
    utils.Print("CLUSTER", "AUTH-CLEAR")
    err := c.Authorize(conn.Auth(), RootKey, false, true, false)
    if err != nil {
        return c.rev, err
    }

    c.Mutex.Lock()
    defer c.Mutex.Unlock()
    server, err := c.doRedirect(conn, RootKey)
    if err != nil || server {
        return c.rev, err
    }

    c.Access.Reset()
    c.changeRevision(c.rev+1, false)
    return c.rev, nil
}
