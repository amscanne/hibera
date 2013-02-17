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
	"hibera/core"
)

type Listener struct {
	net.Listener
}

type Connection struct {
	uuid string
	net.Conn
}

type Addr struct {
	uuid string
}

type HTTPServer struct {
    *core.Core
    *http.Server
    *Listener
}

func (l Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		c = Connection{uuid: core.Uuid(), Conn: c}
	}
	return c, err
}

func (c Connection) RemoteAddr() net.Addr {
	// NOTE: This returns a special identifier.
	// We use this in the handle methods below
	// in order to associate all operations with
	// a specific connection. This is not the
	// actual remote address.
	return Addr{uuid: c.uuid}
}

func (c Connection) Close() error {
	log.Printf("%s", c.Conn.RemoteAddr())
	rv := c.Conn.Close()
	return rv
}

func (a Addr) Network() string {
	return "hibera"
}

func (a Addr) String() string {
	return a.uuid
}

func serv_list_data(output *bytes.Buffer) error {
	return nil
}

func serv_clear_data() error {
	return nil
}

func serv_state_lock(key string, output *bytes.Buffer) (uint64, error) {
	return 0, nil
}

func serv_acquire_lock(uuid string, key string, timeout uint64, name string) (uint64, error) {
	return 0, nil
}

func serv_release_lock(uuid string, key string) (uint64, error) {
	return 0, nil
}

func serv_members_group(group string, output *bytes.Buffer, name string, limit uint64) (uint64, error) {
	return 0, nil
}

func serv_join_group(uuid string, group string, name string) (uint64, error) {
	return 0, nil
}

func serv_leave_group(uuid string, group string, name string) (uint64, error) {
	return 0, nil
}

func serv_get_data(key string, output *bytes.Buffer) (uint64, error) {
	return 0, nil
}

func serv_set_data(key string, input io.ReadCloser, rev uint64) (uint64, error) {
	return 0, nil
}

func serv_remove_data(key string, rev uint64) (uint64, error) {
	return 0, nil
}

func serv_do_watch(uuid string, key string, rev uint64) (uint64, error) {
	return 0, nil
}

func serv_fire_watch(key string, rev uint64) (uint64, error) {
	return 0, nil
}

func intParam(r *http.Request, name string) uint64 {
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

func strParam(r *http.Request, name string) string {
	values := r.Form[name]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (s *HTTPServer) process(w http.ResponseWriter, r *http.Request) {
	// Extract out parameters.
	r.ParseForm()
	parts := strings.SplitN(r.URL.Path[1:], "/", 2)

	// Pull out the relevant client.
	uuid := r.RemoteAddr

	// Prepare our response.
	buf := new(bytes.Buffer)
	err := errors.New("Unhandled Request")
	rev := uint64(0)

	switch len(parts) {
	case 1:
		switch parts[0] {
		case "data":
			switch r.Method {
			case "GET":
				err = serv_list_data(buf)
				break
			case "DELETE":
				err = serv_clear_data()
				break
			}
			break
		}
	case 2:
		switch parts[0] {
		case "locks":
			switch r.Method {
			case "GET":
				rev, err = serv_state_lock(parts[1], buf)
				break
			case "POST":
				rev, err = serv_acquire_lock(uuid, parts[1], intParam(r, "timeout"), strParam(r, "name"))
				break
			case "DELETE":
				rev, err = serv_release_lock(uuid, parts[1])
				break
			}
			break
		case "groups":
			switch r.Method {
			case "GET":
				rev, err = serv_members_group(parts[1], buf, strParam(r, "name"), intParam(r, "limit"))
				break
			case "POST":
				rev, err = serv_join_group(uuid, parts[1], strParam(r, "name"))
				break
			case "DELETE":
				rev, err = serv_leave_group(uuid, parts[1], strParam(r, "name"))
				break
			}
			break
		case "data":
			switch r.Method {
			case "GET":
				rev, err = serv_get_data(parts[1], buf)
				break
			case "POST":
				rev, err = serv_set_data(parts[1], r.Body, intParam(r, "rev"))
				break
			case "DELETE":
				rev, err = serv_remove_data(parts[1], intParam(r, "rev"))
				break
			}
			break
		case "watches":
			switch r.Method {
			case "GET":
				rev, err = serv_do_watch(uuid, parts[1], intParam(r, "rev"))
				break
			case "POST":
				rev, err = serv_fire_watch(parts[1], intParam(r, "rev"))
				break
			}
			break
		}
	}

	if err != nil {
		http.Error(w, "", 501)
	} else {
		w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
		w.Header().Set("X-Revision", strconv.FormatUint(rev, 10))
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
            log.Fatal("Unable to bind HTTP server: ", err)
            return nil
	}
	server.Listener = &Listener{ln}
        server.Core = core

	// Create our http server, and provide connections
	// using our hook listener to track active clients.
        handler := func(w http.ResponseWriter, r *http.Request) {
            server.process(w, r)
        }
	server.Server = &http.Server{
                Handler: http.HandlerFunc(handler),
		ReadTimeout: 0,
		WriteTimeout: 0,
	}

	// No error occured.
	return server
}

func (s *HTTPServer) Run() {
    s.Serve(s.Listener)
}
