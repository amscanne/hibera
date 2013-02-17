package server

import (
	"fmt"
	"log"
	"net"
	"hibera/core"
)

type GossipServer struct {
	*core.Core
	conn *net.UDPConn
}

func (s *GossipServer) Serve() {
	packet := make([]byte, 1024)
	for {
		n, addr, err := s.conn.ReadFromUDP(packet)
		if err != nil {
			continue
		}
		fmt.Printf("Received %d bytes from %s.", addr.String(), n)
	}
}

func NewGossipServer(core *core.Core, addr string, port uint) *GossipServer {
	udpaddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		log.Fatal("Unable to resolve address: ", err)
		return nil
	}

	conn, err := net.ListenUDP("udp", udpaddr)
	if err != nil {
		log.Fatal("Unable to bind Gossip server: ", err)
		return nil
	}

	return &GossipServer{core, conn}
}

func (s *GossipServer) Run() {
	s.Serve()
}
