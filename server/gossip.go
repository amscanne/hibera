package server

import (
	"fmt"
	"log"
	"net"
	"math/rand"
	"time"
	"sync"
	"code.google.com/p/goprotobuf/proto"
	"hibera/core"
	"hibera/client"
)

type GossipServer struct {
	*core.Cluster
	conn     *net.UDPConn
	seeds    []string
	promised uint64
	proposal uint64
	sync.Mutex
}

// The send addresses when not in a cluster.
var DefaultSeeds = "255.255.255.255"

// The frequency (in ms) of heartbeats.
var Heartbeat = 100

// The number of dead servers to encode in a heartbeat.
var DeadServers = 5

func (s *GossipServer) send(addr *net.UDPAddr, m *Message) error {
	gossip := m.GetGossip()
	if gossip != nil {
		log.Printf("SEND %s (addr=%s,version=%d,id=%s)",
			TYPE_name[int32(m.GetType())], addr, m.GetVersion(), *gossip.Id)
	} else {
		log.Printf("SEND %s (addr=%s,version=%d)",
			TYPE_name[int32(m.GetType())], addr, m.GetVersion())
	}

	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	// Send the packet.
	_, err = s.conn.WriteToUDP(data, addr)
	return err
}

func (s *GossipServer) sendPingPong(addr *net.UDPAddr, pong bool) {
	// Construct our list of dead nodes.
	dead := s.Cluster.Dead()
	perm := rand.Perm(len(dead))
	if len(dead) > DeadServers {
		dead = dead[0:DeadServers]
		perm = perm[0:DeadServers]
	}
	gossip := make([]string, len(dead))
	for i, v := range perm {
		gossip[i] = dead[v].Id()
	}

	// Build our ping message.
	version := s.Cluster.Revision()
	id := s.Cluster.Id()
	t := uint32(TYPE_PING)
	if pong {
		t = uint32(TYPE_PONG)
	}
	m := &Message{&t, &version, &Gossip{&id, gossip, nil}, nil}
	s.send(addr, m)
}

func (s *GossipServer) sendPropose(addr *net.UDPAddr, version uint64) {
	// Build our propose message.
	t := uint32(TYPE_PROPOSE)
	m := &Message{&t, &version, nil, nil}
	s.send(addr, m)
}

func (s *GossipServer) sendPromise(addr *net.UDPAddr, version uint64, positive bool) {
	// Build our promise message.
	t := uint32(TYPE_PROMISE)
	if !positive {
		t = uint32(TYPE_NACK)
	}
	m := &Message{&t, &version, nil, nil}
	s.send(addr, m)
}

func (s *GossipServer) sendAccept(addr *net.UDPAddr, version uint64) {
	// Build our accept message.
	t := uint32(TYPE_ACCEPT)
	m := &Message{&t, &version, nil, nil}
	s.send(addr, m)
}

func (s *GossipServer) heartbeat() {
	var addr *net.UDPAddr

	if s.Cluster.IsActive() {
		// Pick a random node and send a ping.
		nodes := s.Cluster.Active()

		if len(nodes) != 0 {
                        node := nodes[rand.Int()%len(nodes)]
			addr = node.Addr()
	                // Assume the packet has dropped,
	                // we'll fix it when we get the response.
	                node.Dropped()
		}
	} else {
		// Pick a seed and send a ping.
		if len(s.seeds) > 0 {
			seedaddr, seedport := client.ParseAddr(s.seeds[rand.Int()%len(s.seeds)])
			addr, _ = net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", seedaddr, seedport))
		}
	}

	// Send a packet if we've got an address.
	if addr != nil {
		go s.sendPingPong(addr, false)
	}
}

func (s *GossipServer) Sender() {
	for {
		time.Sleep(time.Duration(Heartbeat) * time.Millisecond)
		s.heartbeat()
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

	gs := new(GossipServer)
	gs.Cluster = cluster
	gs.conn = conn
	gs.seeds = seeds
	gs.proposal = 1
	gs.promised = 0
	return gs
}

func (s *GossipServer) process(addr *net.UDPAddr, m *Message) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	gossip := m.GetGossip()
	if gossip != nil && *gossip.Id == s.Cluster.Id() {
		return
	}

	if gossip != nil {
		log.Printf("RECV %s (addr=%s,version=%d,id=%s)",
			TYPE_name[int32(m.GetType())], addr, m.GetVersion(), *gossip.Id)
	} else {
		log.Printf("RECV %s (addr=%s,version=%d)",
			TYPE_name[int32(m.GetType())], addr, m.GetVersion())
	}

	switch m.GetType() {
	case uint32(TYPE_PING):
		s.Cluster.Heartbeat(addr, *gossip.Id, m.GetVersion(), gossip.Dead)

		// Respond to the ping.
		go s.sendPingPong(addr, true)
		break

	case uint32(TYPE_PONG):
		s.Cluster.Heartbeat(addr, *gossip.Id, m.GetVersion(), gossip.Dead)

		// If we've not part of a cluster, send a proposal.
		if !s.Cluster.IsActive() {
			go s.sendPropose(addr, s.proposal)
		}
		break

	case uint32(TYPE_PROPOSE):
		if !s.Cluster.IsActive() {
			if m.GetVersion() >= s.promised {
				// Send a promise.
				s.promised = m.GetVersion()
				go s.sendPromise(addr, m.GetVersion(), true)
			} else {
				// Send a polite no thank you.
				go s.sendPromise(addr, s.promised, false)
			}
		}
		break

	case uint32(TYPE_PROMISE):
		if !s.Cluster.IsActive() {
			go s.sendAccept(addr, m.GetVersion())
		}
		break

	case uint32(TYPE_NACK):
		if !s.Cluster.IsActive() {
			// Our next proposal will beat it.
			s.proposal = m.GetVersion() + 1
		}
		break

	case uint32(TYPE_ACCEPT):
		if !s.Cluster.IsActive() {
			if m.GetVersion() >= s.promised {
				// Accept the offer.
				s.Cluster.Activate(addr, m.GetVersion())
				go s.sendPingPong(addr, true)
			}
		}
		break
	}
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

func (s *GossipServer) Run() {
	go s.Sender()
	s.Serve()
}
