package server

import (
	"fmt"
	"log"
	"net"
	"math/rand"
	"time"
	"code.google.com/p/goprotobuf/proto"
	"hibera/core"
)

type GossipServer struct {
	*core.Cluster
	conn *net.UDPConn
}

var DefaultSeeds = "255.255.255.255"
var Heartbeat = 100
var DeadServers = 5

func (s *GossipServer) send(addr *net.UDPAddr, m *Message) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	// Send the packet.
	_, err = s.conn.WriteToUDP(data, addr)
	return err
}

func (s *GossipServer) process(addr *net.UDPAddr, m *Message) {
	switch m.GetType() {
	case uint32(TYPE_PING):
		gossip := m.GetGossip()
		s.Cluster.OnPing(addr, m.GetVersion(), *gossip.Id, gossip.Dead)
		// Response to the ping.
		s.sendPing(addr, true)
		break

	case uint32(TYPE_PONG):
		gossip := m.GetGossip()
		s.Cluster.OnPong(addr, m.GetVersion(), *gossip.Id, gossip.Dead)
		break

	case uint32(TYPE_PROPOSE):
		s.Cluster.OnPropose(addr, m.GetVersion())
		break

	case uint32(TYPE_PROMISE):
		s.Cluster.OnPromise(addr, m.GetVersion())
		break

	case uint32(TYPE_NOPROMISE):
		s.Cluster.OnNoPromise(addr, m.GetVersion())
		break

	case uint32(TYPE_ACCEPT):
		s.Cluster.OnAccept(addr, m.GetVersion())
		break

	case uint32(TYPE_ACCEPTED):
		s.Cluster.OnAccepted(addr, m.GetVersion())
		break
	}
}

func (s *GossipServer) blast() {
	// Send out proposals if necessary.
	if s.Cluster.IsProposePhase() {
		nodes := s.Cluster.PaxosNodes()
		version := s.Cluster.ProposeVersion()
		for _, addr := range nodes {
			t := uint32(TYPE_PROPOSE)
			m := &Message{&t, &version, nil, nil}
			s.send(addr, m)
		}

		// Send our accept requests.
	} else if s.Cluster.IsAcceptPhase() {
		nodes := s.Cluster.PaxosNodes()
		version := s.Cluster.ProposeVersion()
		for _, addr := range nodes {
			t := uint32(TYPE_ACCEPT)
			m := &Message{&t, &version, nil, nil}
			s.send(addr, m)
		}
	}

	// Pick a random node and send a ping.
	nodes := s.Cluster.AllNodes()
	addr := nodes[rand.Int()%len(nodes)]
	s.sendPing(addr, false)
}

func (s *GossipServer) sendPing(addr *net.UDPAddr, pong bool) {
	// Construct our list of dead nodes.
	dead := s.Cluster.DeadNodes()
	deadcount := len(dead)
	if deadcount > DeadServers {
		deadcount = DeadServers
	}
	perm := rand.Perm(len(dead))
	perm = perm[0:deadcount]
	gossip := make([]string, len(dead))
	for i, v := range perm {
		gossip[i] = dead[v].String()
	}

	version := s.Cluster.Version()
	id := s.Cluster.Id()
	t := uint32(TYPE_PING)
	if pong {
		t = uint32(TYPE_PONG)
	}
	m := &Message{&t, &version, &Gossip{&id, gossip, nil}, nil}

	// Assume the packet has dropped,
	// we'll fix it when we get the response.
	s.Cluster.OnDrop(addr)
	s.send(addr, m)
}

func (s *GossipServer) Serve() {
	packet := make([]byte, 1024)
	for {
		// Pull the next packet.
		n, addr, err := s.conn.ReadFromUDP(packet)
		if err != nil {
			continue
		}
		m := &Message{}
		err = proto.Unmarshal(packet[0:n], m)
		if err != nil {
			continue
		}

		// Process whenever.
		go s.process(addr, m)
	}
}

func (s *GossipServer) Ping() {
	for {
		time.Sleep(time.Duration(Heartbeat) * time.Millisecond)
		s.blast()
	}
}

func NewGossipServer(cluster *core.Cluster, addr string, port uint, seeds []string) *GossipServer {
	udpaddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		log.Print("Unable to resolve address: ", err)
		return nil
	}

	conn, err := net.ListenUDP("udp", udpaddr)
	if err != nil {
		log.Print("Unable to bind Gossip server: ", err)
		return nil
	}

	return &GossipServer{cluster, conn}
}

func (s *GossipServer) Run() {
	go s.Ping()
	s.Serve()
}
