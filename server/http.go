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
    "net"
    "net/http"
    "os"
    "strconv"
    "strings"
    "syscall"
    "time"
)

var UnhandledRequest = errors.New("Unhandled request")

type Listener struct {
    net.Listener
    active uint
    avail  chan bool
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

func (l Listener) Close() error {
    // Close the listen socket.
    err := l.Listener.Close()
    if err != nil {
        return err
    }

    return nil
}

func (l Listener) Drain() {
    timeout := time.After(time.Second)

    // Drain the connection pool.
    for i := 0; i < int(l.active); i += 1 {
        select {
        case <-l.avail:
            break
        case <-timeout:
            return
        }
    }
}

func (c HTTPConnection) RemoteAddr() net.Addr {
    return Addr{c.Connection.ConnectionId}
}

func (c HTTPConnection) Close() error {
    // Inform the core about this dropped req.
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

func (s *HTTPServer) revParam(r *http.Request, name string) (core.Revision, error) {
    values := r.Form[name]
    if len(values) == 0 {
        return core.NoRevision, nil
    }
    rev, err := core.RevisionFromString(values[0])
    if err != nil {
        return core.NoRevision, err
    }
    return rev, nil
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

func (s *HTTPServer) getConnection(r *http.Request) *Connection {
    // Pull out the relevant req.
    // NOTE: This is a hack. The underlying connection is our
    // special connection object -- and we return a string which
    // is the connection Id. So we turn it back into an integer
    // and used it to look up the Connection object.
    id, err := strconv.ParseUint(r.RemoteAddr, 0, 64)
    if err != nil {
        return nil
    }

    if len(r.Header["X-Client-Id"]) > 0 {
        // Return a connection with the asssociate req.
        return s.Hub.FindConnection(ConnectionId(id),
            UserId(r.Header["X-Client-Id"][0]))
    }

    // Return a connection with no associated req.
    return s.Hub.FindConnection(ConnectionId(id), "")
}

func (s *HTTPServer) getRequest(w http.ResponseWriter, r *http.Request) *Request {
    var auth string
    var ns string

    // Grab the connection.
    conn := s.getConnection(r)
    if conn == nil {
        return nil
    }

    // Get the close notifier.
    cn, ok := w.(http.CloseNotifier)
    if !ok {
        return nil
    }

    // Check the authorization header.
    if len(r.Header["X-Authorization"]) > 0 {
        auth = r.Header["X-Authorization"][0]
    }

    // Check if we have an explicit namespace.
    if len(r.Header["X-Namespace"]) > 0 {
        ns = r.Header["X-Namespace"][0]
    } else {
        // Our namespace may be given by the host header.
        // If this is given as an IP address, then we use the
        // default namespace.
        parts := strings.SplitN(r.Host, ":", 2)
        if net.ParseIP(parts[0]) == nil {
            // Use the given host as the namespace.
            ns = parts[0]
        } else {
            // Use the default namespace.
            ns = ""
        }
    }

    return NewRequest(conn, core.Token(auth), core.Namespace(ns), cn.CloseNotify())
}

func (s *HTTPServer) process(w http.ResponseWriter, r *http.Request) {

    // Fully read the content.
    content, err := s.getContent(r)
    if err != nil {
        http.Error(w, "", http.StatusNoContent)
        return
    }

    // Grab the connection object.
    req := s.getRequest(w, r)
    if req == nil {
        http.Error(w, "", http.StatusNotFound)
        return
    }

    // Extract out parameters.
    // NOTE: We do this after reading the body because we
    // don't want the parsing code to try to do full extraction
    // of form parameters from the body.
    r.ParseForm()
    parts := strings.SplitN(r.URL.Path[1:], "/", 3)

    // Prepare our response.
    // If we don't handle the request in the switch below,
    // then we default to the Error here which is Unhandled.
    buf := new(bytes.Buffer)
    enc := json.NewEncoder(buf)
    err = UnhandledRequest
    rev := core.NoRevision

    // Process the request.
    if parts[0] == "v1.0" {
        // It's an API request.
        // We got through our normal parsing.
        parts = parts[1:]
    } else {
        // It's not an API request.
        // Map it through to the data for convenience.
        // NOTE: We route it through using the entire
        // URL. This is simply to keep the 'public' data
        // in a distinct namespace from the other data.
        parts = []string{"data", r.URL.Path}
    }

    if len(parts) == 1 {
        switch parts[0] {

        case "":
            switch r.Method {
            case "GET":
                var data []byte
                data, rev, err = s.Cluster.Info(req)
                if err == nil {
                    _, err = buf.Write(data)
                }
                break
            case "POST":
                replication := uint(s.intParam(r, "replication"))
                rev, err = s.Cluster.Activate(req, replication)
                break
            case "DELETE":
                rev, err = s.Cluster.Deactivate(req)
                break
            }
            break

        case "data":
            switch r.Method {
            case "GET":
                var items map[core.Key]uint
                items, rev, err = s.Cluster.DataList(req)
                if err == nil {
                    err = enc.Encode(items)
                }
                break
            }
            break

        case "access":
            switch r.Method {
            case "GET":
                var tokens []core.Token
                tokens, rev, err = s.Cluster.AccessList(req)
                if err == nil {
                    err = enc.Encode(tokens)
                }
                break
            }
            break
        }

    } else if len(parts) == 2 {

        key := core.Key(parts[1])
        auth := core.Token(parts[1])

        switch parts[0] {
        case "sync":
            switch r.Method {
            case "GET":
                data := req.Data(s.strParam(r, "data"))
                limit := uint(s.intParam(r, "limit"))
                var info core.SyncInfo
                info, rev, err = s.Cluster.SyncMembers(req, key, data, limit)
                if err == nil {
                    err = enc.Encode(info)
                }
                break
            case "POST":
                data := req.Data(s.strParam(r, "data"))
                limit := uint(s.intParam(r, "limit"))
                timeout := uint(s.intParam(r, "timeout"))
                var index int
                index, rev, err = s.Cluster.SyncJoin(req, key, data, limit, timeout)
                if err == nil {
                    err = enc.Encode(index)
                }
                break
            case "DELETE":
                data := req.Data(s.strParam(r, "data"))
                rev, err = s.Cluster.SyncLeave(req, key, data)
                break
            }
            break
        case "data":
            switch r.Method {
            case "GET":
                var value []byte
                rev, err = s.revParam(r, "rev")
                if err == nil {
                    timeout := uint(s.intParam(r, "timeout"))
                    value, rev, err = s.Cluster.DataGet(req, key, rev, timeout)
                    if err == nil {
                        _, err = buf.Write(value)
                    }
                }
                break
            case "POST":
                rev, err = s.revParam(r, "rev")
                if err == nil {
                    rev, err = s.Cluster.DataSet(req, key, rev, content)
                }
                break
            case "DELETE":
                rev, err = s.revParam(r, "rev")
                if err == nil {
                    rev, err = s.Cluster.DataRemove(req, key, rev)
                }
                break
            }
            break
        case "access":
            switch r.Method {
            case "GET":
                var value *core.Permissions
                value, rev, err = s.Cluster.AccessGet(req, auth)
                if err == nil {
                    err = enc.Encode(value)
                }
                break
            case "POST":
                path := core.Key(s.strParam(r, "path"))
                read := s.boolParam(r, "read")
                write := s.boolParam(r, "write")
                execute := s.boolParam(r, "execute")
                rev, err = s.Cluster.AccessUpdate(req, auth, path, read, write, execute)
                break
            }
            break
        case "event":
            switch r.Method {
            case "GET":
                rev, err = s.revParam(r, "rev")
                if err == nil {
                    timeout := uint(s.intParam(r, "timeout"))
                    rev, err = s.Cluster.EventWait(req, key, rev, timeout)
                }
                break
            case "POST":
                rev, err = s.revParam(r, "rev")
                if err == nil {
                    rev, err = s.Cluster.EventFire(req, key, rev)
                }
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
        w.Header().Set("X-Revision", rev.String())
        w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
        utils.Print("HTTP", "200 X-Revision %s", rev.String())
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
        break

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
        w.Header().Set("X-Revision", rev.String())
        if err == core.RevConflict || err == core.NotFound {
            // Revision conflict.
            url := utils.MakeURL(err.Error(), r.URL.Path+"?"+r.URL.RawQuery, nil)
            utils.Print("HTTP", "409 %s", url)
            http.Error(w, err.Error(), http.StatusConflict)
        } else {
            // Even with errors, we set a revision header.
            // The error could result from a revision conflict,
            // so this is important information and can't just
            // be ignored by the client.
            utils.Print("HTTP", "500")
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
        break
    }
}

func NewHTTPServer(cluster *cluster.Cluster, restart int, addr string, port uint) (*HTTPServer, error) {

    // Bind our port.
    var ln net.Listener
    var err error

    if restart < 0 {
        // Create our hooked listener.
        ln, err = net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
    } else {
        // Create a server wrapped around the fd.
        ln, err = net.FileListener(os.NewFile(uintptr(restart), fmt.Sprintf("%s:%d", addr, port)))
    }
    if err != nil {
        return nil, err
    }

    // Figure out our active limit (1/2 open limit).
    // We use 1/2 for clients because we often need
    // a pipe for detecting errors, and 2 more sockets
    // for doing quorum! Yikes, too many FDs.
    // NOTE: This is also used to initialize the active
    // limit for the cluster. This is the most number of
    // simultaneous syncs that can occured at any moment.
    var rlim syscall.Rlimit
    err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim)
    if err != nil {
        return nil, err
    }
    active := uint(rlim.Cur) / 2
    cluster.SetSyncLimit(active / 4)

    // Create our object.
    server := new(HTTPServer)
    server.Hub = NewHub(cluster)
    server.Listener = &Listener{ln, active, make(chan bool, active), server.Hub}
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
    return server, nil
}

func (s *HTTPServer) Run() {
    s.Serve(s.Listener)
}

func (s *HTTPServer) Stop() {
    // Stop accepting new connections.
    s.Listener.Close()

    // Fire off all existing connections.
    s.Cluster.Data.Flush()

    // Drain existing connections.
    s.Listener.Drain()
}

func (s *HTTPServer) Restart() *os.File {
    // Ensure it's a TCP socket.
    tcp_ln, ok := s.Listener.Listener.(*net.TCPListener)
    if !ok {
        return nil
    }

    // Grab our restart handle.
    file, err := tcp_ln.File()
    if err != nil {
        return nil
    }

    // Return the Fd.
    return file
}
