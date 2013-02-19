package core

import (
	"log"
        "sync/atomic"
	"hibera/storage"
)

type ConnectionId uint64
type Connection struct {
        // A reference to the associated core.
	core *Core

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

type Core struct {
	// Our connection and conn maps.
	connections map[ConnectionId]*Connection
	clients map[UserId]*Client
	nextid uint64

        // The underlying cluster.
        // This is exposed by the API() call.
	cluster *Cluster
}

func (c *Connection) Name(name string) SubName {
	if name != "" {
		return SubName(name)
	}
	if c.client != nil {
		return SubName(c.client.UserId)
	}
	return SubName(c.addr)
}

func (c *Connection) EphemId() EphemId {
    if c.client != nil {
        return EphemId(c.client.ClientId)
    }
    return EphemId(c.ConnectionId)
}

func (c *Core) genid() uint64 {
        return atomic.AddUint64(&c.nextid, 1)
}

func (c *Core) NewConnection(addr string) *Connection {
	// Generate conn with no user, and
	// a straight-forward id. The user can
	// associate some conn-id with their
	// active connection during lookup.
	conn := &Connection{c, ConnectionId(c.genid()), addr, nil}
	c.connections[conn.ConnectionId] = conn
	return conn
}

func (c *Core) FindConnection(id ConnectionId, userid UserId) *Connection {
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

func (c *Core) DropConnection(id ConnectionId) {
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
			c.cluster.Purge(EphemId(conn.client.ClientId))
			conn.client = nil
		}
	}

	// Purge the connection.
	delete(c.connections, id)
	c.cluster.Purge(EphemId(id))
}

func (c *Connection) Drop() {
    c.core.DropConnection(c.ConnectionId)
}

func (c *Core) Info() ([]byte, error) {
    return nil, nil
}

func (c *Core) API() API {
    return c.cluster
}

func NewCore(domain string, keys uint, backend *storage.Backend) *Core {
	core := new(Core)

        // Create our connection cache.
	core.connections = make(map[ConnectionId]*Connection)
	core.clients = make(map[UserId]*Client)

        // Create our cluster.
	ids, err := backend.LoadIds(keys)
	if err != nil {
		log.Fatal("Unable to load ring: ", err)
		return nil
	}
	core.cluster = NewCluster(backend, domain, ids)

	return core
}
