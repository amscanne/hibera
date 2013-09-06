package server

import (
    "hibera/cluster"
    "hibera/utils"
)

type Server struct {
    *HTTPServer
    *GossipServer
}

func NewServer(c *cluster.Cluster, restart int, addr string, port uint, seeds []string, active uint) (*Server, error) {
    http, err := NewHTTPServer(c, restart, addr, port, active)
    if err != nil {
        return nil, err
    }

    gossip, err := NewGossipServer(c, addr, port, seeds)
    if err != nil {
        return nil, err
    }

    return &Server{http, gossip}, nil
}

func (s *Server) Run() {
    go s.GossipServer.Run()
    utils.Print("SERVER", "Starting...")
    s.HTTPServer.Run()
}
