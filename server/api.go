package server

import (
    "hibera/cluster"
    "hibera/utils"
)

var DefaultBind = ""

type Server struct {
    *HTTPServer
    *GossipServer
}

func NewServer(c *cluster.Cluster, addr string, port uint, seeds []string, active uint) *Server {
    http := NewHTTPServer(c, addr, port, active)
    if http == nil {
        return nil
    }

    gossip := NewGossipServer(c, addr, port, seeds)
    if gossip == nil {
        return nil
    }

    return &Server{http, gossip}
}

func (s *Server) Run() {
    go s.GossipServer.Run()
    utils.Print("SERVER", "Starting...")
    s.HTTPServer.Run()
}
