package core

import (
    "hibera/utils"
)

type Redirect struct {
    URL string
}

func (r *Redirect) Error() string {
    return r.URL
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

func (c *Cluster) Authorize(auth string) bool {
    return auth == c.auth
}

func (c *Cluster) Info(conn *Connection, rev Revision) ([]byte, Revision, error) {
    utils.Print("CLUSTER", "INFO")
    bytes, err := c.Nodes.Encode(rev, false)
    return bytes, c.rev, err
}

func (c *Cluster) Reset(conn *Connection) error {
    server, err := c.doRedirect(conn, HiberaKey)
    if err != nil {
        return err
    }
    if server {
        return c.Activate()
    }

    return c.allReset()
}

func (c *Cluster) Id() string {
    return c.Nodes.Self().Id()
}

func (c *Cluster) Version() Revision {
    return c.rev
}

func (c *Cluster) List(conn *Connection) ([]Key, error) {
    utils.Print("CLUSTER", "DATA-LIST")
    if c.Nodes.Heartbeat(conn.Name("")) {
        return c.data.DataList()
    }
    return c.allList()
}

func (c *Cluster) Get(conn *Connection, key Key, rev Revision, timeout uint) ([]byte, Revision, error) {
    utils.Print("CLUSTER", "DATA-GET key=%s", string(key))
    _, err := c.doRedirect(conn, key)
    if err != nil {
        return nil, Revision(0), err
    }
    alive := func() bool { return conn.alive() && c.ring.IsMaster(key) }
    return c.data.DataWatch(conn.EphemId(), key, rev, timeout, alive)
}

func (c *Cluster) Set(conn *Connection, key Key, rev Revision, value []byte) (Revision, error) {
    utils.Print("CLUSTER", "DATA-SET key=%s len(value)=%d rev=%d", string(key), len(value), uint64(rev))
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
    server, err := c.doRedirect(conn, key)
    if err != nil {
        return Revision(0), err
    }
    if server {
        return c.data.DataRemove(key, rev)
    }
    return c.quorumRemove(c.ring, key, rev)
}

func (c *Cluster) Clear(conn *Connection) error {
    utils.Print("CLUSTER", "DATA-CLEAR")
    if c.Nodes.Heartbeat(conn.Name("")) {
        return c.data.DataClear()
    }
    return c.allClear()
}

func (c *Cluster) Members(conn *Connection, key Key, name string, limit uint) (int, []string, Revision, error) {
    utils.Print("CLUSTER", "SYNC-MEMBERS key=%s name=%s limit=%d", string(key), name, limit)
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return -1, nil, Revision(0), err
    }
    return c.data.SyncMembers(key, name, limit)
}

func (c *Cluster) Join(conn *Connection, key Key, name string, limit uint, timeout uint) (int, Revision, error) {
    utils.Print("CLUSTER", "SYNC-JOIN key=%s name=%s limit=%d timeout=%d", string(key), name, limit, timeout)
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return -1, Revision(0), err
    }
    alive := func() bool { return conn.alive() && c.ring.IsMaster(key) }
    return c.data.SyncJoin(conn.EphemId(), key, name, limit, timeout, alive)
}

func (c *Cluster) Leave(conn *Connection, key Key, name string) (Revision, error) {
    utils.Print("CLUSTER", "SYNC-LEAVE key=%s name=%s", string(key), name)
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return Revision(0), err
    }
    return c.data.SyncLeave(conn.EphemId(), key, name)
}

func (c *Cluster) Wait(conn *Connection, key Key, rev Revision, timeout uint) (Revision, error) {
    utils.Print("CLUSTER", "EVENT-WAIT key=%s rev=%d timeout=%d", string(key), uint64(rev), timeout)
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return Revision(0), err
    }
    alive := func() bool { return conn.alive() && c.ring.IsMaster(key) }
    return c.data.EventWait(conn.EphemId(), key, rev, timeout, alive)
}

func (c *Cluster) Fire(conn *Connection, key Key, rev Revision) (Revision, error) {
    utils.Print("CLUSTER", "EVENT-FIRE key=%s rev=%d", string(key), uint64(rev))
    server, err := c.doRedirect(conn, key)
    if err != nil || server {
        return Revision(0), err
    }
    return c.data.EventFire(key, rev)
}
