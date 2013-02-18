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
	"encoding/json"
	"hibera/core"
)

var UnhandledRequest = errors.New("")

type Listener struct {
	*core.Core
	net.Listener
}

type Connection struct {
	*core.Connection
	net.Conn
}

type Addr struct {
	core.ConnectionId
}

type HTTPServer struct {
	*core.Core
	*Listener
	*http.Server
}

func (l Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		c = Connection{l.Core.NewConnection(c.RemoteAddr().String()), c}
	}
	return c, err
}

func (c Connection) RemoteAddr() net.Addr {
	return Addr{c.Connection.ConnectionId}
}

func (c Connection) Close() error {
	// Inform the core about this dropped conn.
	c.Connection.Drop()
	return c.Conn.Close()
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

func (s *HTTPServer) getConnection(r *http.Request) *core.Connection {
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
	    return s.Core.FindConnection(core.ConnectionId(id),
                                         core.UserId(r.Header["X-Client-Id"][0]))
        }

        // Return a connection with no associated conn.
        return s.Core.FindConnection(core.ConnectionId(id), "")
}

func (s *HTTPServer) process(w http.ResponseWriter, r *http.Request) {
        // Pull out a connection.
        conn := s.getConnection(r)
	if conn == nil {
		http.Error(w, "", 403)
		return
	}

	// Fully read the content.
	content, err := s.getContent(r)
	if err != nil {
		http.Error(w, "", 500)
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

	switch len(parts) {
	case 1:
		switch parts[0] {
		case "":
			switch r.Method {
			case "GET":
                            var data []byte
	                    data, err = s.Core.Info()
                            if err == nil {
                                _, err = buf.Write(data)
                            }
			    break
			}
			break

		case "data":
			switch r.Method {
			case "GET":
                            var items *[]core.Key
	                    items, err = s.Core.API().DataList()
                            if err == nil {
	                        err = enc.Encode(items)
                            }
			    break
			case "DELETE":
	                    err = s.Core.API().DataClear()
			    break
			}
			break
		}
                break

	case 2:
		switch parts[0] {
		case "locks":
			switch r.Method {
			case "GET":
                                name := conn.Name(s.strParam(r, "name"))
                                var owners []core.SubName
	                        owners, rev, err = s.Core.API().LockOwners(core.Key(parts[1]), name)
	                        if err == nil {
                                    err = enc.Encode(owners)
                                }
				break
			case "POST":
                                name := conn.Name(s.strParam(r, "name"))
                                limit := s.intParam(r, "limit")
                                timeout := s.intParam(r, "timeout")
	                        rev, err = s.Core.API().LockAcquire(conn.EphemId(), core.Key(parts[1]), timeout, name, limit)
				break
			case "DELETE":
                                name := conn.Name(s.strParam(r, "name"))
	                        rev, err = s.Core.API().LockRelease(conn.EphemId(), core.Key(parts[1]), name)
				break
			}
			break
		case "groups":
			switch r.Method {
			case "GET":
                                name := conn.Name(s.strParam(r, "name"))
                                limit := s.intParam(r, "limit")
                                var members []core.SubName
	                        members, rev, err = s.Core.API().GroupMembers(core.Key(parts[1]), name, limit)
	                        if err == nil {
                                    err = enc.Encode(members)
	                        }
				break
			case "POST":
                                name := conn.Name(s.strParam(r, "name"))
	                        rev, err = s.Core.API().GroupJoin(conn.EphemId(), core.Key(parts[1]), name)
				break
			case "DELETE":
                                name := conn.Name(s.strParam(r, "name"))
	                        rev, err = s.Core.API().GroupLeave(conn.EphemId(), core.Key(parts[1]), name)
				break
			}
			break
		case "data":
			switch r.Method {
			case "GET":
                                var value []byte
	                        value, rev, err = s.Core.API().DataGet(core.Key(parts[1]))
	                        if err == nil {
                                    _, err = buf.Write(value)
	                        }
				break
			case "POST":
                                rev = core.Revision(s.intParam(r, "rev"))
	                        rev, err = s.Core.API().DataSet(core.Key(parts[1]), content, rev)
				break
			case "DELETE":
                                rev = core.Revision(s.intParam(r, "rev"))
	                        rev, err = s.Core.API().DataRemove(core.Key(parts[1]), rev)
				break
			}
			break
		case "watches":
			switch r.Method {
			case "GET":
                                rev = core.Revision(s.intParam(r, "rev"))
                                timeout := s.intParam(r, "timeout")
	                        rev, err = s.Core.API().WatchWait(conn.EphemId(), core.Key(parts[1]), rev, timeout)
				break
			case "POST":
                                rev = core.Revision(s.intParam(r, "rev"))
	                        rev, err = s.Core.API().WatchFire(core.Key(parts[1]), rev)
				break
			}
			break
		}
	}

	if err != nil {
		http.Error(w, err.Error(), 501)
	} else {
                // Always set the appropriate headers on return.
                // We fully specify the Content-Length that was written
                // and always return a revision (although often it will
                // be 0 because the call didn't have a revision).
		w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
		w.Header().Set("X-Revision", strconv.FormatUint(uint64(rev), 10))
		io.Copy(w, buf)
	}
}

func NewHTTPServer(core *core.Core, addr string, port uint) *HTTPServer {
	// Create our object.
	server := new(HTTPServer)

	// Create our hooked listener.
	if len(addr) == 0 {
		addr = DEFAULT_BIND
	}
	if port == 0 {
		port = DEFAULT_PORT
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		log.Print("Unable to bind HTTP server: ", err)
		return nil
	}
	server.Listener = &Listener{core, ln}
	server.Core = core

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
