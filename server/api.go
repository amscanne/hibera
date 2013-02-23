package server

import (
    "hibera/core"
)

var DefaultBind = ""

type Server struct {
    *HTTPServer
    *GossipServer
}

func NewServer(hub *core.Hub, cluster *core.Cluster, addr string, port uint, seeds []string) *Server {
    http := NewHTTPServer(hub, cluster, addr, port)
    if http == nil {
        return nil
    }

    gossip := NewGossipServer(cluster, addr, port, seeds)
    if gossip == nil {
        return nil
    }

    return &Server{http, gossip}
}

func (s *Server) Run() {
    go s.GossipServer.Run()
    s.HTTPServer.Run()
}
