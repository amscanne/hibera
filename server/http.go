package server

import (
	"fmt"
	"net"
	"log"
	"strings"
	"net/http"
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

func handler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	parts := strings.SplitN(r.URL.Path[1:], "/", 3)

	switch len(parts) {
	case 1:
		fmt.Fprintf(w, "{%s}", parts[0])
		break
	case 2:
		fmt.Fprintf(w, "{%s} -> %s", parts[0], parts[1])
		break
	case 3:
		fmt.Fprintf(w, "{%s} -> %s -> %s", parts[0], parts[1], parts[2])
		break
	}

	//fmt.Fprintf(w, "%s %s!", r.URL.Path[1:], r.Form["timeout"])
	//fmt.Fprintf(w, "%s!", r.RemoteAddr)
}

func RunHTTP(addr string, port int) error {

	// Initialize our default handler.
	http.HandleFunc("/", handler)

	// Create our hooked listener.
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		return err
	}
	hook := Listener{ln}

	// Create our http server, and provide connections
	// using our hook listener to track active clients.
	s := &http.Server{
		ReadTimeout:  0,
		WriteTimeout: 0,
	}
	s.Serve(hook)

	// No error occured.
	return nil
}
