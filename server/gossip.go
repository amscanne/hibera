package server

import (
	"fmt"
	"log"
	"net"
	"math/rand"
	"time"
	"code.google.com/p/goprotobuf/proto"
	"hibera/core"
	"hibera/client"
)

type GossipServer struct {
	*core.Cluster
	conn  *net.UDPConn
	seeds []string
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
		// Respond to the ping.
		go s.sendPing(addr, true)
		break

	case uint32(TYPE_PONG):
		gossip := m.GetGossip()
		s.Cluster.OnPong(addr, m.GetVersion(), *gossip.Id, gossip.Dead)
		break

	case uint32(TYPE_PROPOSE):
		if s.Cluster.OnPropose(addr, m.GetVersion()) {
			go s.sendPromise(addr, m.GetVersion(), true)
		} else {
			go s.sendPromise(addr, m.GetVersion(), false)
		}
		break

	case uint32(TYPE_PROMISE):
		s.Cluster.OnPromise(addr, m.GetVersion())
		break

	case uint32(TYPE_NOPROMISE):
		s.Cluster.OnNoPromise(addr, m.GetVersion())
		break

	case uint32(TYPE_ACCEPT):
		if s.Cluster.OnAccept(addr, m.GetVersion()) {
			go s.sendAccepted(addr, m.GetVersion())
		}
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
		for _, node := range nodes {
			t := uint32(TYPE_PROPOSE)
			m := &Message{&t, &version, nil, nil}
			s.send(node.Addr(), m)
		}

		// Send our accept requests.
	} else if s.Cluster.IsAcceptPhase() {
		nodes := s.Cluster.PaxosNodes()
		version := s.Cluster.ProposeVersion()
		for _, node := range nodes {
			t := uint32(TYPE_ACCEPT)
			m := &Message{&t, &version, nil, nil}
			s.send(node.Addr(), m)
		}
	}

	// Pick a random node and send a ping.
	nodes := s.Cluster.ActiveNodes()
	var addr *net.UDPAddr
	if len(nodes) == 0 && len(s.seeds) != 0 {
		seedaddr, seedport := client.ParseAddr(s.seeds[rand.Int()%len(s.seeds)])
		addr, _ = net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", seedaddr, seedport))
	} else if len(nodes) != 0 {
		addr = nodes[rand.Int()%len(nodes)].Addr()
	}
	if addr != nil {
		go s.sendPing(addr, false)
	}
}

func (s *GossipServer) sendPing(addr *net.UDPAddr, pong bool) {
	// Construct our list of dead nodes.
	dead := s.Cluster.DeadNodes()
	perm := rand.Perm(len(dead))
	if len(dead) > DeadServers {
		dead = dead[0:DeadServers]
		perm = perm[0:DeadServers]
	}
	gossip := make([]string, len(dead))
	for i, v := range perm {
		gossip[i] = dead[v]
	}

	// Build our ping message.
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

func (s *GossipServer) sendPromise(addr *net.UDPAddr, version uint64, positive bool) {
	// Build our promise message.
	t := uint32(TYPE_PROMISE)
	if !positive {
		t = uint32(TYPE_NOPROMISE)
	}
	m := &Message{&t, &version, nil, nil}
	s.send(addr, m)
}

func (s *GossipServer) sendAccepted(addr *net.UDPAddr, version uint64) {
	// Build our accepted message.
	t := uint32(TYPE_ACCEPTED)
	m := &Message{&t, &version, nil, nil}
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

func (s *GossipServer) Sender() {
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

	return &GossipServer{cluster, conn, seeds}
}

func (s *GossipServer) Run() {
	go s.Sender()
	s.Serve()
}
