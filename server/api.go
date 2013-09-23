package server

import (
    "github.com/amscanne/hibera/cluster"
    "github.com/amscanne/hibera/utils"
)

type Server struct {
    *HTTPServer
    *GossipServer
}

func NewServer(c *cluster.Cluster, restart int, addr string, port uint, seeds []string) (*Server, error) {
    http, err := NewHTTPServer(c, restart, addr, port)
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
