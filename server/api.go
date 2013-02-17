package server

import (
	"hibera/core"
)

var DEFAULT_BIND = ""
var DEFAULT_PORT = uint(2033)

type Server struct {
	*HTTPServer
	*GossipServer
}

func NewServer(core *core.Core, addr string, port uint, seeds []string) *Server {
	http := NewHTTPServer(core, addr, port)
	if http == nil {
		return nil
	}

	gossip := NewGossipServer(core, addr, port, seeds)
	if gossip == nil {
		return nil
	}

	return &Server{http, gossip}
}

func (s *Server) Run() {
	go s.GossipServer.Run()
	s.HTTPServer.Run()
}
