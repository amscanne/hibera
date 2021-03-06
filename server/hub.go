package server

import (
    "github.com/amscanne/hibera/cluster"
    "github.com/amscanne/hibera/core"
    "github.com/amscanne/hibera/utils"
    "net"
    "strings"
    "sync"
    "sync/atomic"
    "time"
)

// The timeout for broken connections.
var EphemeralTimeout = time.Second

type ConnectionId uint64

type Connection struct {
    // A reference to the associated core.
    *Hub

    // The underlying connection.
    raw net.Conn

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

    // Whether this connection has been initialized
    // with a client (above). Client may stil be nil.
    // NOTE: This will also initialize the notifier.
    inited bool
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

    // The data used for members of sets. The UserId
    // above is not exposed for security reasons.
    Name string

    // The print-friendly version of this name.
    // This is used if the above data is empty.
    friendlyName string

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

    // Synchronization.
    sync.Mutex

    // The underlying cluster.
    // We maintain a reference to this so that
    // ephemeral nodes can be purged when all the
    // relevant connections have been dropped.
    *cluster.Cluster
}

func (c *Connection) Data(data string) string {
    if data != "" {
        return data
    }
    if c.client != nil {
        return c.client.friendlyName
    }
    return c.ServerId()
}

func (c *Connection) ServerId() string {
    if c.client != nil {
        return c.client.Name
    }
    return string(c.addr)
}

func (c *Connection) EphemId() core.EphemId {
    if c.client != nil {
        return core.EphemId(c.client.ClientId)
    }
    return core.EphemId(c.ConnectionId)
}

func (c *Hub) genid() uint64 {
    return atomic.AddUint64(&c.nextid, 1)
}

func (c *Hub) NewConnection(raw net.Conn) *Connection {
    // Generate conn with no user, and a straight-forward id.
    // The user can associate the appropriate conn-id with their
    // active connection once they send the first headers along.
    addr := raw.RemoteAddr().String()
    conn := &Connection{c, raw, ConnectionId(c.genid()), addr, nil, false}

    // Disable the timeout.
    raw.SetReadDeadline(time.Time{})
    raw.SetWriteDeadline(time.Time{})

    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    c.connections[conn.ConnectionId] = conn
    utils.Print("HUB", "CONNECTED conid=%d", conn.ConnectionId)

    return conn
}

func (c *Hub) FindConnection(id ConnectionId, userid UserId) *Connection {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    conn := c.connections[id]
    if conn == nil {
        return nil
    }

    // Check if it's already initialized.
    if conn.inited {
        return conn
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
            conn.client.Name = string(userid)
            noport := strings.Split(string(conn.addr), ":")[0]
            conn.client.friendlyName = utils.Hash(string(userid))[:8] + "@" + noport
            conn.client.refs = 1
            c.clients[userid] = conn.client
        } else {
            // Bump up the reference.
            conn.client.refs += 1
        }
        utils.Print("HUB", "MAPPED conid=%d userid=%s clientid=%d ephemid=%d name=%s",
            conn.ConnectionId, conn.client.UserId, conn.client.ClientId,
            conn.EphemId(), conn.Data(""))
    }

    conn.inited = true
    return conn
}

func (c *Hub) DropConnection(conn *Connection) {
    c.Mutex.Lock()
    defer c.Mutex.Unlock()

    // Delete this connection.
    utils.Print("HUB", "DROPPED conid=%d", conn.ConnectionId)
    delete(c.connections, conn.ConnectionId)
    c.Cluster.Purge(core.EphemId(conn.ConnectionId))

    // Shuffle userid mappings.
    if conn.client != nil {
        conn.client.refs -= 1
        if conn.client.refs == 0 {
            // Remove the user from the map and
            // purge all related keys from the
            // underlying storage system.
            utils.Print("HUB", "DROPPED userid=%s clientid=%d",
                conn.client.UserId, conn.client.ClientId)
            delete(c.clients, conn.client.UserId)
            c.Cluster.Purge(core.EphemId(conn.client.ClientId))
        }
        conn.client = nil
    }
}

func (c *Connection) Drop() {
    c.Hub.DropConnection(c)
}

func (c *Hub) dumpHub() {
    utils.Print("HUB", "HUB connections=%d clients=%d",
        len(c.connections), len(c.clients))
    for _, conn := range c.connections {
        var clid uint64
        if conn.client != nil {
            clid = uint64(conn.client.ClientId)
        } else {
            clid = uint64(0)
        }
        utils.Print("HUB", "CONNECTION id=%d addr=%s client=%d",
            uint64(conn.ConnectionId), conn.addr, clid)
    }
    for _, client := range c.clients {
        utils.Print("HUB", "CLIENT id=%d userid=%s refs=%d",
            uint64(client.ClientId), client.UserId, client.refs)
    }
}

func NewHub(c *cluster.Cluster) *Hub {
    hub := new(Hub)
    hub.connections = make(map[ConnectionId]*Connection)
    hub.clients = make(map[UserId]*Client)
    hub.Cluster = c

    // NOTE: We start with a nextid that is 1, because
    // some parts of the code consider nextid == 0 to be
    // an invalid connection. Sure, it'll only ever bug
    // out on the first connection *ever* but still.
    hub.nextid = 1

    return hub
}
