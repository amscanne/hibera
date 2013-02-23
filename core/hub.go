package core

import (
    "sync/atomic"
)

type ConnectionId uint64
type Connection struct {
    // A reference to the associated core.
    *Hub

    // A unique Connection (per-connection).
    // This is generated automatically and can
    // not be set by the user (unlike the id for
    // the conn below).
    ConnectionId

    // The address associated with this conn.
    addr string

    // The user associated (if there is one).
    // This will be looked up on the first if
    // the user provided a generated ConnectionId.
    client *Client
}

type EphemId uint64
type ClientId uint64
type UserId string

type Client struct {
    // A unique ClientId. This is used as the
    // ephemeralId for the cluster operations.
    ClientId

    // The user string for identifying the Connection.
    // This will be passed in via a header.
    UserId

    // The number of active Connection objects
    // refering to this User object. The User
    // objects are reference counted and garbage
    // collected when all connections disconnect.
    refs int32
}

type Hub struct {
    // Our connection and conn maps.
    connections map[ConnectionId]*Connection
    clients     map[UserId]*Client
    nextid      uint64

    // The underlying cluster.
    // We maintain a reference to this so that
    // ephemeral nodes can be purged when all the
    // relevant connections have been dropped.
    *Cluster
}

func (c *Connection) Name(name string) string {
    if name != "" {
        return name
    }
    if c.client != nil {
        return string(c.client.UserId)
    }
    return string(c.addr)
}

func (c *Connection) EphemId() EphemId {
    if c.client != nil {
        return EphemId(c.client.ClientId)
    }
    return EphemId(c.ConnectionId)
}

func (c *Hub) genid() uint64 {
    return atomic.AddUint64(&c.nextid, 1)
}

func (c *Hub) NewConnection(addr string) *Connection {
    // Generate conn with no user, and
    // a straight-forward id. The user can
    // associate some conn-id with their
    // active connection during lookup.
    conn := &Connection{c, ConnectionId(c.genid()), addr, nil}
    c.connections[conn.ConnectionId] = conn
    return conn
}

func (c *Hub) FindConnection(id ConnectionId, userid UserId) *Connection {
    conn := c.connections[id]
    if conn == nil {
        return nil
    }

    // Create the user if it doesnt exist.
    // NOTE: There are some race conditions here between the
    // map lookup and reference increment / creation. These
    // should probably be fixed, but by my reckoning the current
    // outcome of these conditions will be some clients having
    // failed calls.
    if userid != "" {
        conn.client = c.clients[userid]
        if conn.client == nil {
            // Create and initialize a new user.
            conn.client = new(Client)
            conn.client.ClientId = ClientId(c.genid())
            conn.client.UserId = userid
            conn.client.refs = 1
            c.clients[userid] = conn.client
        } else {
            // Bump up the reference.
            atomic.AddInt32(&conn.client.refs, 1)
        }
    }

    return c.connections[id]
}

func (c *Hub) DropConnection(id ConnectionId) {
    // Lookup this conn.
    conn := c.connections[id]
    if conn == nil {
        return
    }

    // Shuffle userid mappings.
    if conn.client != nil {
        if atomic.AddInt32(&conn.client.refs, -1) == 0 {
            // Remove the user from the map and
            // purge all related keys from the
            // underlying storage system.
            delete(c.clients, conn.client.UserId)
            c.Cluster.Purge(EphemId(conn.client.ClientId))
            conn.client = nil
        }
    }

    // Purge the connection.
    delete(c.connections, id)
    c.Cluster.Purge(EphemId(id))
}

func (c *Connection) Drop() {
    c.Hub.DropConnection(c.ConnectionId)
}

func NewHub(cluster *Cluster) *Hub {
    hub := new(Hub)
    hub.connections = make(map[ConnectionId]*Connection)
    hub.clients = make(map[UserId]*Client)
    hub.Cluster = cluster
    return hub
}
