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
	"io/ioutil"
	"encoding/json"
	"hibera/core"
)

type Listener struct {
	*core.Core
	net.Listener
}

type Connection struct {
	*core.Client
	net.Conn
}

type Addr struct {
	*core.Client
}

type HTTPServer struct {
	*core.Core
	*Listener
	*http.Server
}

func (l Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		c = Connection{l.Core.NewClient(), c}
	}
	return c, err
}

func (c Connection) RemoteAddr() net.Addr {
	return Addr{c.Client}
}

func (c Connection) Close() error {
        if c.Client != nil {
            // Inform the core about this dropped client.
            c.Client.Core.DropClient(c.Client.ClientId)
            c.Client = nil
        }
	return c.Conn.Close()
}

func (a Addr) Network() string {
	return "hibera"
}

func (a Addr) String() string {
        if a.Client != nil {
            // Return the string client id.
	    return string(a.Client.ClientId)
        }
        // We've already closed.
        return "<disconnected>"
}

func (s *HTTPServer) info(output *bytes.Buffer) error {
	enc := json.NewEncoder(output)
	info, err := s.Core.Info()
	if err != nil {
		return err
	}
	return enc.Encode(info)
}

func (s *HTTPServer) data_list(output *bytes.Buffer) error {
	enc := json.NewEncoder(output)
	items, err := s.Core.DataList()
	if err != nil {
		return err
	}
	return enc.Encode(items)
}

func (s *HTTPServer) data_clear() error {
	return s.Core.DataClear()
}

func (s *HTTPServer) lock_owner(key string, output *bytes.Buffer) (uint64, error) {
	enc := json.NewEncoder(output)
	owners, rev, err := s.Core.LockOwners(key)
	if err != nil {
		return 0, err
	}
	return rev, enc.Encode(owners)
}

func (s *HTTPServer) lock_acquire(client *core.Client, key string, timeout uint64, name string, limit uint64) (uint64, error) {
	return s.Core.LockAcquire(client, key, timeout, name, limit)
}

func (s *HTTPServer) lock_release(client *core.Client, key string) (uint64, error) {
	return s.Core.LockRelease(client, key)
}

func (s *HTTPServer) group_members(group string, output *bytes.Buffer, name string, limit uint64) (uint64, error) {
	enc := json.NewEncoder(output)
	items, rev, err := s.Core.GroupMembers(group, name, limit)
	if err != nil {
		return 0, err
	}
	return rev, enc.Encode(items)
}

func (s *HTTPServer) group_join(client *core.Client, group string, name string) (uint64, error) {
	return s.Core.GroupJoin(client, group, name)
}

func (s *HTTPServer) group_leave(client *core.Client, group string, name string) (uint64, error) {
	return s.Core.GroupLeave(client, group, name)
}

func (s *HTTPServer) data_get(key string, output *bytes.Buffer) (uint64, error) {
	value, rev, err := s.Core.DataGet(key)
	if err != nil {
		return 0, err
	}
	_, err = output.Write(value)
	return rev, err
}

func (s *HTTPServer) data_set(key string, value []byte, rev uint64) (uint64, error) {
	return s.Core.DataSet(key, value, rev)
}

func (s *HTTPServer) data_remove(key string, rev uint64) (uint64, error) {
	return s.Core.DataRemove(key, rev)
}

func (s *HTTPServer) watch_wait(client *core.Client, key string, rev uint64) (uint64, error) {
	return s.Core.WatchWait(client, key, rev)
}

func (s *HTTPServer) watch_fire(key string, rev uint64) (uint64, error) {
	return s.Core.WatchFire(key, rev)
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
	id, err := strconv.ParseUint(r.RemoteAddr, 0, 64)
	if err != nil {
		http.Error(w, "", 403)
		return
	}
	client := s.Core.FindClient(core.ClientId(id))
	if client == nil {
		http.Error(w, "", 403)
		return
	}

	// Prepare our response.
	buf := new(bytes.Buffer)
	err = errors.New("Unhandled Request")
	rev := uint64(0)

	switch len(parts) {
	case 1:
		switch parts[0] {
                case "":
                    switch r.Method {
			case "GET":
				err = s.info(buf)
				break
                    }
                    break

		case "data":
			switch r.Method {
			case "GET":
				err = s.data_list(buf)
				break
			case "DELETE":
				err = s.data_clear()
				break
			}
			break
		}
	case 2:
		switch parts[0] {
		case "locks":
			switch r.Method {
			case "GET":
				rev, err = s.lock_owner(parts[1], buf)
				break
			case "POST":
				rev, err = s.lock_acquire(client, parts[1], intParam(r, "timeout"), strParam(r, "name"), intParam(r, "limit"))
				break
			case "DELETE":
				rev, err = s.lock_release(client, parts[1])
				break
			}
			break
		case "groups":
			switch r.Method {
			case "GET":
				rev, err = s.group_members(parts[1], buf, strParam(r, "name"), intParam(r, "limit"))
				break
			case "POST":
				rev, err = s.group_join(client, parts[1], strParam(r, "name"))
				break
			case "DELETE":
				rev, err = s.group_leave(client, parts[1], strParam(r, "name"))
				break
			}
			break
		case "data":
			switch r.Method {
			case "GET":
				rev, err = s.data_get(parts[1], buf)
				break
			case "POST":
				data, err := ioutil.ReadAll(r.Body)
				if err == nil {
					rev, err = s.data_set(parts[1], data, intParam(r, "rev"))
				}
				break
			case "DELETE":
				rev, err = s.data_remove(parts[1], intParam(r, "rev"))
				break
			}
			break
		case "watches":
			switch r.Method {
			case "GET":
				rev, err = s.watch_wait(client, parts[1], intParam(r, "rev"))
				break
			case "POST":
				rev, err = s.watch_fire(parts[1], intParam(r, "rev"))
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
