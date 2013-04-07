package server

import (
    "fmt"
    "net"
    "log"
    "io"
    "strconv"
    "bytes"
    "strings"
    "net/http"
    "errors"
    "time"
    "encoding/json"
    "hibera/core"
    "hibera/client"
    "hibera/utils"
)

var UnhandledRequest = errors.New("Unhandled request")
var DefaultActive = uint(256)

type Listener struct {
    net.Listener
    avail chan bool
    *core.Cluster
}

type Connection struct {
    net.Conn
    *Listener
    *core.Connection
}

type Addr struct {
    core.ConnectionId
}

type HTTPServer struct {
    *core.Cluster
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

    c, err := l.Listener.Accept()
    if err != nil {
        l.avail<- true
        return c, err
    }

    // NOTE: This function should only be called from
    // contexts where we are not expecting anything to
    // be read from the socket. i.e. from within the
    // request handler itself. If it is called from other
    // places, there is the possibility that it will eat
    // actual useful input from the user.
    alive := func() bool {
        c.SetReadDeadline(time.Now())
        if _, err := c.Read(make([]byte, 1, 1)); err == io.EOF {
            return false
        } else {
            var zero time.Time
            c.SetReadDeadline(zero)
        }
        return true
    }
    return Connection{c, &l, l.Cluster.NewConnection(c.RemoteAddr().String(), alive)}, nil
}

func (c Connection) RemoteAddr() net.Addr {
    return Addr{c.Connection.ConnectionId}
}

func (c Connection) Close() error {
    // Inform the core about this dropped conn.
    c.Connection.Drop()
    err := c.Conn.Close()

    // Let the Accept() know that a connection is available.
    c.Listener.avail<- true

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

func (s *HTTPServer) getConnection(r *http.Request, auth string) *core.Connection {
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
        return s.Cluster.FindConnection(core.ConnectionId(id),
            core.UserId(r.Header["X-Client-Id"][0]), auth)
    }

    // Return a connection with no associated conn.
    return s.Cluster.FindConnection(core.ConnectionId(id), "", auth)
}

func (s *HTTPServer) process(w http.ResponseWriter, r *http.Request) {
    utils.Print("HTTP", "%s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)

    // Check the authorization header.
    var auth string
    if len(r.Header["X-Authorization"]) > 0 {
        auth = r.Header["X-Authorization"][0]
    }

    // Pull out a connection.
    conn := s.getConnection(r, auth)
    if conn == nil {
        http.Error(w, "", http.StatusNotFound)
        return
    }

    // Fully read the content.
    content, err := s.getContent(r)
    if err != nil {
        http.Error(w, "", http.StatusNoContent)
        return
    }

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
                rev = core.Revision(s.intParam(r, "rev"))
                var id string
                var data []byte
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
                rev, items, err = s.Cluster.List(conn)
                if err == nil {
                    err = enc.Encode(items)
                }
                break
            case "DELETE":
                rev, err = s.Cluster.Clear(conn)
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
                info.Index, info.Members, rev, err = s.Cluster.Members(conn, core.Key(parts[1]), name, limit)
                if err == nil {
                    err = enc.Encode(info)
                }
                break
            case "POST":
                name := conn.Name(s.strParam(r, "name"))
                limit := uint(s.intParam(r, "limit"))
                timeout := uint(s.intParam(r, "timeout"))
                var index int
                index, rev, err = s.Cluster.Join(conn, core.Key(parts[1]), name, limit, timeout)
                if err == nil {
                    err = enc.Encode(index)
                }
                break
            case "DELETE":
                name := conn.Name(s.strParam(r, "name"))
                rev, err = s.Cluster.Leave(conn, core.Key(parts[1]), name)
                break
            }
            break
        case "data":
            switch r.Method {
            case "GET":
                var value []byte
                rev = core.Revision(s.intParam(r, "rev"))
                timeout := uint(s.intParam(r, "timeout"))
                value, rev, err = s.Cluster.Get(conn, core.Key(parts[1]), rev, timeout)
                if err == nil {
                    _, err = buf.Write(value)
                }
                break
            case "POST":
                rev = core.Revision(s.intParam(r, "rev"))
                rev, err = s.Cluster.Set(conn, core.Key(parts[1]), rev, content)
                break
            case "DELETE":
                rev = core.Revision(s.intParam(r, "rev"))
                rev, err = s.Cluster.Remove(conn, core.Key(parts[1]), rev)
                break
            }
            break
        case "event":
            switch r.Method {
            case "GET":
                rev = core.Revision(s.intParam(r, "rev"))
                timeout := uint(s.intParam(r, "timeout"))
                rev, err = s.Cluster.Wait(conn, core.Key(parts[1]), rev, timeout)
                break
            case "POST":
                rev = core.Revision(s.intParam(r, "rev"))
                rev, err = s.Cluster.Fire(conn, core.Key(parts[1]), rev)
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

    case *core.Redirect:
        // If we've gotten back a redirect error, then
        // we send the client to where it belongs.
        // NOTE: We actually use a permentant redirect
        // so that the next time this key is tried --
        // smart clients might even try the newly given
        // location first.
        url := utils.MakeURL(err.Error(), r.URL.Path+"?"+r.URL.RawQuery, nil)
        utils.Print("HTTP", "301 %s", url)
        http.Redirect(w, r, url, http.StatusMovedPermanently)

    case *core.PermissionError:
        // The user doesn't have appropriate permissions.
        url := utils.MakeURL(err.Error(), r.URL.Path+"?"+r.URL.RawQuery, nil)
        utils.Print("HTTP", "403 %s", url)
        http.Error(w, err.Error(), http.StatusForbidden)
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

func NewHTTPServer(cluster *core.Cluster, addr string, port uint, active uint) *HTTPServer {
    // Create our object.
    server := new(HTTPServer)

    // Create our hooked listener.
    if len(addr) == 0 {
        addr = DefaultBind
    }
    if port == 0 {
        port = client.DefaultPort
    }
    ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
    if err != nil {
        log.Print("Unable to bind HTTP server: ", err)
        return nil
    }
    server.Listener = &Listener{ln, make(chan bool, active), cluster}
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
        Handler: http.HandlerFunc(handler),
        ReadTimeout: time.Second,
        WriteTimeout: time.Second,
    }

    // No error occured.
    return server
}

func (s *HTTPServer) Run() {
    s.Serve(s.Listener)
}
