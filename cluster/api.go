package cluster

import (
    "hibera/core"
    "hibera/utils"
)

type Redirect struct {
    URL string
}

type NotActivatedError struct{}

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

    // We always authorize INFO against the root namespace.
    err := c.Authorize(RootNamespace, req.Auth(), RootKey, true, false, false)
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

    // We always authorize ACTIVATE against the root namepsace.
    err := c.Authorize(RootNamespace, req.Auth(), RootKey, true, true, false)
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

    return c.doActivate(N)
}

func (c *Cluster) Deactivate(req Request) (core.Revision, error) {
    utils.Print("CLUSTER", "DEACTIVATE")

    // Same as activate, we authorize DEACTIVATE against the root namespace.
    err := c.Authorize(RootNamespace, req.Auth(), RootKey, true, true, false)
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

func (c *Cluster) DataList(req Request) (map[core.Key]uint, core.Revision, error) {
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

    // NOTE: The root keys are always filtered.
    // See get() and set() where it is also handled specially.
    delete(items, RootKey)

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

    // NOTE: We don't allow reading the root key.
    // See also special behavior in set() and list().
    if !server && key == RootKey {
        return nil, core.NoRevision, &PermissionError{key}
    }

    valid := func() bool { return c.ring.IsMaster(key) }
    notifier := req.Notifier()

    return c.Data.DataWatch(
        req.EphemId(),
        req.Namespace(), key,
        rev, timeout,
        notifier, valid)
}

func (c *Cluster) DataSet(req Request, key core.Key, rev core.Revision, value []byte) (core.Revision, error) {
    utils.Print("CLUSTER", "DATA-SET key=%s length=%d rev=%s", key, len(value), rev.String())

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

    // NOTE: We disallow clients setting the root keys.
    // It is also filtered in get() and list().
    if key == RootKey {
        return core.NoRevision, &PermissionError{key}
    }

    if rev.IsZero() {
        // NOTE: Even if err != nil, the revision will be NoRevision.
        // We will try this only once. It's quite possible that this
        // will conflict with some other client. However, clients that
        // do not succeed will get a server error and will retry. So
        // if you keep posting will 0, eventually you will set some key
        // to whatever value you wanted.
        _, rev, err = c.Data.DataGet(req.Namespace(), key)
        if err != nil {
            return core.NoRevision, err
        }

        // Use the next given reivsion.
        rev = rev.Next()
    }

    // Because quorum is going to open connections to multiple
    // servers, it has to be capable of sending the value many
    // times.  We can't splice() the socket, so we allow the
    // quorum functions to use a cache as necessary to send data.
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

    if rev.IsZero() {
        // NOTE: See above in DataGet().
        _, rev, err = c.Data.DataGet(req.Namespace(), key)
        if err != nil {
            return core.NoRevision, err
        }

        // Use the next given reivsion.
        rev = rev.Next()
    }

    return c.quorumRemove(c.ring, req.Namespace(), key, rev)
}

func (c *Cluster) SyncMembers(req Request, key core.Key, data string, limit uint) (core.SyncInfo, core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-MEMBERS key=%s data=%s limit=%d", key, data, limit)

    err := c.Authorize(req.Namespace(), req.Auth(), key, true, false, true)
    if err != nil {
        return core.NoSyncInfo, core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil || server {
        return core.NoSyncInfo, core.NoRevision, err
    }

    return c.Data.SyncMembers(req.Namespace(), key, data, limit)
}

func (c *Cluster) SyncJoin(req Request, key core.Key, data string, limit uint, timeout uint) (int, core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-JOIN key=%s data=%s limit=%d timeout=%d", key, data, limit, timeout)

    err := c.Authorize(req.Namespace(), req.Auth(), key, false, true, true)
    if err != nil {
        return -1, core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil || server {
        return -1, core.NoRevision, err
    }

    valid := func() bool { return c.ring.IsMaster(key) }
    notifier := req.Notifier()

    return c.Data.SyncJoin(req.EphemId(), req.Namespace(), key, data, limit, timeout, notifier, valid)
}

func (c *Cluster) SyncLeave(req Request, key core.Key, data string) (core.Revision, error) {
    utils.Print("CLUSTER", "SYNC-LEAVE key=%s data=%s", key, data)

    err := c.Authorize(req.Namespace(), req.Auth(), key, false, true, true)
    if err != nil {
        return core.NoRevision, err
    }

    server, err := c.doRedirect(req, key)
    if err != nil || server {
        return core.NoRevision, err
    }

    return c.Data.SyncLeave(req.EphemId(), req.Namespace(), key, data)
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
    notifier := req.Notifier()

    return c.Data.EventWait(req.EphemId(), req.Namespace(), key, rev, timeout, notifier, valid)
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

    err := c.Authorize(req.Namespace(), req.Auth(), RootKey, true, false, false)
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

    err := c.Authorize(req.Namespace(), req.Auth(), RootKey, true, false, false)
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

    // If the namespace doesn't exist yet, then we
    // authorize against the root namespace. This allows
    // access in the root namespace to create new ones.
    ns := req.Namespace()
    if !c.Access.Has(ns) {
        ns = RootNamespace
    }

    err := c.Authorize(ns, req.Auth(), RootKey, false, true, false)
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
