package server

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "hibera/cluster"
    "hibera/core"
    "hibera/utils"
    "io"
    "log"
    "net"
    "net/http"
    "strconv"
    "strings"
)

var UnhandledRequest = errors.New("Unhandled request")
var DefaultActive = uint(256)

type Listener struct {
    net.Listener
    avail chan bool
    *Hub
}

type HTTPConnection struct {
    net.Conn
    *Listener
    *Connection
}

type Addr struct {
    ConnectionId
}

type HTTPServer struct {
    *cluster.Cluster
    *Hub
    *Listener
    *http.Server
}

// This type is used only to serialize
// the JSON version of this object on the
// wire. The client has a separate def'n.
type syncInfo struct {
    Index   int
    Members []string
}

func (l Listener) Accept() (net.Conn, error) {
    // Wait for a slot to be available.
    <-l.avail

    // Accept the connection.
    // Set an initial deadline for the initial read.
    c, err := l.Listener.Accept()
    if err != nil {
        l.avail <- true
        return c, err
    }

    return HTTPConnection{c, &l, l.Hub.NewConnection(c)}, nil
}

func (c HTTPConnection) RemoteAddr() net.Addr {
    return Addr{c.Connection.ConnectionId}
}

func (c HTTPConnection) Close() error {
    // Inform the core about this dropped conn.
    c.Connection.Drop()
    err := c.Conn.Close()

    // Let the Accept() know that a connection is available.
    c.Listener.avail <- true

    return err
}

func (a Addr) Network() string {
    return "hibera"
}

func (a Addr) String() string {
    return strconv.FormatUint(uint64(a.ConnectionId), 10)
}

func (s *HTTPServer) intParam(r *http.Request, name string) uint64 {
    values := r.Form[name]
    if len(values) == 0 {
        return 0
    }
    rval, err := strconv.ParseUint(values[0], 0, 64)
    if err != nil {
        return 0
    }
    return rval
}

func (s *HTTPServer) boolParam(r *http.Request, name string) bool {
    values := r.Form[name]
    if len(values) == 0 {
        return false
    }
    lower := values[0]
    if lower == "1" ||
        lower == "true" || lower == "t" ||
        lower == "y" || lower == "yes" {
        return true
    }
    return false
}

func (s *HTTPServer) strParam(r *http.Request, name string) string {
    values := r.Form[name]
    if len(values) == 0 {
        return ""
    }
    return values[0]
}

func (s *HTTPServer) getContent(r *http.Request) ([]byte, error) {
    length := r.ContentLength
    if length < 0 {
        return nil, nil
    }
    buf := make([]byte, length)
    n, err := io.ReadFull(r.Body, buf)
    if n != int(length) || err != nil {
        return nil, err
    }
    return buf, nil
}

func (s *HTTPServer) getConnection(r *http.Request, auth string, notifier <-chan bool) *Connection {
    // Pull out the relevant conn.
    // NOTE: This is a hack. The underlying connection is our
    // special connection object -- and we return a string which
    // is the connection Id. So we turn it back into an integer
    // and used it to look up the Connection object.
    id, err := strconv.ParseUint(r.RemoteAddr, 0, 64)
    if err != nil {
        return nil
    }

    if len(r.Header["X-Client-Id"]) > 0 {
        // Return a connection with the asssociate conn.
        return s.Hub.FindConnection(ConnectionId(id),
            UserId(r.Header["X-Client-Id"][0]), r.Host, auth, notifier)
    }

    // Return a connection with no associated conn.
    return s.Hub.FindConnection(ConnectionId(id), "", r.Host, auth, notifier)
}

func (s *HTTPServer) process(w http.ResponseWriter, r *http.Request) {
    // Check the authorization header.
    var auth string
    if len(r.Header["X-Authorization"]) > 0 {
        auth = r.Header["X-Authorization"][0]
    }

    // Pull out a connection (with the close notifiers).
    closeNotifier, ok := w.(http.CloseNotifier)
    if !ok {
        // Something is very wrong -- no close notifier?
        http.Error(w, "", http.StatusInternalServerError)
        return
    }

    // Fully read the content.
    content, err := s.getContent(r)
    if err != nil {
        http.Error(w, "", http.StatusNoContent)
        return
    }

    // Grab the connection object.
    conn := s.getConnection(r, auth, closeNotifier.CloseNotify())
    if conn == nil {
        http.Error(w, "", http.StatusNotFound)
        return
    }

    // Debug.
    utils.Print("HTTP", "%s %s?%s conn=%s", r.Method, r.URL.Path, r.URL.RawQuery, conn.ServerId())

    // Extract out parameters.
    // NOTE: We do this after reading the body because we
    // don't want the parsing code to try to do full extraction
    // of form parameters from the body.
    r.ParseForm()
    parts := strings.SplitN(r.URL.Path[1:], "/", 2)

    // Prepare our response.
    // If we don't handle the request in the switch below,
    // then we default to the Error here which is Unhandled.
    buf := new(bytes.Buffer)
    enc := json.NewEncoder(buf)
    err = UnhandledRequest
    rev := core.Revision(0)

    // Process the request.
    if len(parts) == 1 {
        switch parts[0] {

        case "":
            switch r.Method {
            case "GET":
                var id string
                var data []byte
                rev = core.Revision(s.intParam(r, "rev"))
                id, data, rev, err = s.Cluster.Info(conn, rev)
                if err == nil {
                    w.Header().Set("X-Cluster-Id", id)
                    _, err = buf.Write(data)
                }
                break
            case "POST":
                rev, err = s.Cluster.Activate(conn)
                break
            case "DELETE":
                rev, err = s.Cluster.Deactivate(conn)
                break
            }
            break

        case "data":
            switch r.Method {
            case "GET":
                var items []core.Key
                items, rev, err = s.Cluster.DataList(conn)
                if err == nil {
                    err = enc.Encode(items)
                }
                break
            }
            break

        case "access":
            switch r.Method {
            case "GET":
                var tokens []string
                tokens, rev, err = s.Cluster.AccessList(conn)
                if err == nil {
                    err = enc.Encode(tokens)
                }
                break
            }
            break

        case "nodes":
            switch r.Method {
            case "GET":
                var nodes map[string]string
                nodes, rev, err = s.Cluster.NodeList(conn)
                if err == nil {
                    err = enc.Encode(nodes)
                }
                break
            case "POST":
                rev, err = s.Cluster.Activate(conn)
                break
            case "DELETE":
                rev, err = s.Cluster.Deactivate(conn)
                break
            }
            break
        }

    } else if len(parts) == 2 {

        switch parts[0] {
        case "sync":
            switch r.Method {
            case "GET":
                name := conn.Name(s.strParam(r, "name"))
                limit := uint(s.intParam(r, "limit"))
                info := syncInfo{}
                info.Index, info.Members, rev, err = s.Cluster.SyncMembers(conn, core.Key(parts[1]), name, limit)
                if err == nil {
                    err = enc.Encode(info)
                }
                break
            case "POST":
                name := conn.Name(s.strParam(r, "name"))
                limit := uint(s.intParam(r, "limit"))
                timeout := uint(s.intParam(r, "timeout"))
                var index int
                index, rev, err = s.Cluster.SyncJoin(conn, core.Key(parts[1]), name, limit, timeout)
                if err == nil {
                    err = enc.Encode(index)
                }
                break
            case "DELETE":
                name := conn.Name(s.strParam(r, "name"))
                rev, err = s.Cluster.SyncLeave(conn, core.Key(parts[1]), name)
                break
            }
            break
        case "data":
            switch r.Method {
            case "GET":
                var value []byte
                rev = core.Revision(s.intParam(r, "rev"))
                timeout := uint(s.intParam(r, "timeout"))
                value, rev, err = s.Cluster.DataGet(conn, core.Key(parts[1]), rev, timeout)
                if err == nil {
                    _, err = buf.Write(value)
                }
                break
            case "POST":
                rev = core.Revision(s.intParam(r, "rev"))
                rev, err = s.Cluster.DataSet(conn, core.Key(parts[1]), rev, content)
                break
            case "DELETE":
                rev = core.Revision(s.intParam(r, "rev"))
                rev, err = s.Cluster.DataRemove(conn, core.Key(parts[1]), rev)
                break
            }
            break
        case "access":
            switch r.Method {
            case "GET":
                var value *core.Token
                value, rev, err = s.Cluster.AccessGet(conn, parts[1])
                if err == nil {
                    err = enc.Encode(value)
                }
                break
            case "POST":
                path := s.strParam(r, "path")
                read := s.boolParam(r, "read")
                write := s.boolParam(r, "write")
                execute := s.boolParam(r, "execute")
                rev, err = s.Cluster.AccessGrant(conn, parts[1], path, read, write, execute)
                break
            case "DELETE":
                rev, err = s.Cluster.AccessRevoke(conn, parts[1])
                break
            }
            break
        case "nodes":
            switch r.Method {
            case "GET":
                var value *core.Node
                value, rev, err = s.Cluster.NodeGet(conn, parts[1])
                if err == nil {
                    err = enc.Encode(value)
                }
                break
            }
            break
        case "event":
            switch r.Method {
            case "GET":
                rev = core.Revision(s.intParam(r, "rev"))
                timeout := uint(s.intParam(r, "timeout"))
                rev, err = s.Cluster.EventWait(conn, core.Key(parts[1]), rev, timeout)
                break
            case "POST":
                rev = core.Revision(s.intParam(r, "rev"))
                rev, err = s.Cluster.EventFire(conn, core.Key(parts[1]), rev)
                break
            }
            break
        }
    }

    switch err.(type) {
    case nil:
        // Always set the appropriate headers on return.
        // We fully specify the Content-Length that was written
        // and always return a revision (although often it will
        // be 0 because the call didn't have a revision).
        revstr := strconv.FormatUint(uint64(rev), 10)
        w.Header().Set("X-Revision", revstr)
        w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
        utils.Print("HTTP", "200 X-Revision %s", revstr)
        io.Copy(w, buf)
        break

    case *cluster.Redirect:
        // If we've gotten back a redirect error, then
        // we send the client to where it belongs.
        // NOTE: We actually use a permentant redirect
        // so that the next time this key is tried --
        // smart clients might even try the newly given
        // location first.
        url := utils.MakeURL(err.Error(), r.URL.Path+"?"+r.URL.RawQuery, nil)
        utils.Print("HTTP", "301 %s", url)
        http.Redirect(w, r, url, http.StatusMovedPermanently)

    case *cluster.PermissionError:
        // The user doesn't have appropriate permissions.
        url := utils.MakeURL(err.Error(), r.URL.Path+"?"+r.URL.RawQuery, nil)
        utils.Print("HTTP", "403 %s", url)
        http.Error(w, err.Error(), http.StatusForbidden)
        break

    case *cluster.NotActivatedError:
        // This cluster has not yet been activated.
        url := utils.MakeURL(err.Error(), r.URL.Path+"?"+r.URL.RawQuery, nil)
        utils.Print("HTTP", "503 %s", url)
        http.Error(w, err.Error(), http.StatusServiceUnavailable)
        break

    default:
        // Even with errors, we set a revision header.
        // The error could result from a revision conflict,
        // so this is important information and can't just
        // be ignored by the client.
        w.Header().Set("X-Revision", strconv.FormatUint(uint64(rev), 10))
        utils.Print("HTTP", "501")
        http.Error(w, err.Error(), http.StatusInternalServerError)
        break
    }
}

func NewHTTPServer(cluster *cluster.Cluster, addr string, port uint, active uint) *HTTPServer {
    // Create our object.
    server := new(HTTPServer)

    // Create our hooked listener.
    ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
    if err != nil {
        log.Print("Unable to bind HTTP server: ", err)
        return nil
    }
    server.Hub = NewHub(cluster)
    server.Listener = &Listener{ln, make(chan bool, active), server.Hub}
    server.Cluster = cluster

    // Fill the available channel slots.
    for i := uint(0); i < active; i += 1 {
        server.Listener.avail <- true
    }

    // Create our http server, and provide connections
    // using our hook listener to track active clients.
    handler := func(w http.ResponseWriter, r *http.Request) {
        server.process(w, r)
    }
    server.Server = &http.Server{
        Handler:      http.HandlerFunc(handler),
        ReadTimeout:  0,
        WriteTimeout: 0,
    }

    // No error occured.
    return server
}

func (s *HTTPServer) Run() {
    s.Serve(s.Listener)
}
