package server

import (
	"fmt"
	"log"
	"net"
	"code.google.com/p/goprotobuf/proto"
	"hibera/core"
)

type GossipServer struct {
	*core.Core
	conn *net.UDPConn
}

func (s *GossipServer) sendPing(addr *net.UDPAddr) error {
	// Create our message.
	pkttype := uint32(TYPE_PING)
	version := uint64(0)
	ping := &Gossip{}
	ping.Type = &pkttype
	ping.Version = &version
	data, err := proto.Marshal(ping)
	if err != nil {
		return err
	}

	// Send the packet.
	_, err = s.conn.WriteToUDP(data, addr)
	return err
}

func (s *GossipServer) processPong(addr *net.UDPAddr, gossip *Gossip) {
}

func (s *GossipServer) Serve() {
	packet := make([]byte, 1024)
	for {
		// Pull the next packet.
		n, addr, err := s.conn.ReadFromUDP(packet)
		if err != nil {
			continue
		}
		pong := &Gossip{}
		err = proto.Unmarshal(packet[0:n], pong)
		if err != nil {
			continue
		}

		// Process whenever.
		go s.processPong(addr, pong)
	}
}

func NewGossipServer(core *core.Core, addr string, port uint, seeds []string) *GossipServer {
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
