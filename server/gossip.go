package server

import (
    "encoding/json"
    "fmt"
    "github.com/amscanne/hibera/cluster"
    "github.com/amscanne/hibera/core"
    "github.com/amscanne/hibera/utils"
    "math/rand"
    "net"
    "time"
)

type GossipServer struct {
    // The cluster.
    *cluster.Cluster

    // The bound UDP socket.
    conn *net.UDPConn

    // The list of seeds to heartbeat, when
    // no other known nodes are available.
    seeds []string
}

// The frequency (in ms) of heartbeats.
var MinHeartbeat = 100
var MaxHeartbeat = 1000

// The number of dead servers to encode in a heartbeat.
var DeadServers = 5

func (s *GossipServer) send(addr *net.UDPAddr, m *message) error {
    data, err := json.Marshal(m)
    if err != nil {
        return err
    }

    // Send the packet.
    _, err = s.conn.WriteToUDP(data, addr)
    return err
}

func (s *GossipServer) sendPingPong(addr *net.UDPAddr, pong bool) {
    // Construct our list of dead nodes.
    // We randomly select up to DeadServers from
    // the list of all dead servers.
    dead := s.Cluster.Dead()
    perm := rand.Perm(len(dead))
    if len(dead) > DeadServers {
        perm = perm[0:DeadServers]
    }

    gossip := make([]string, len(perm))
    for i, v := range perm {
        gossip[i] = dead[v].Id()
    }

    // Build our message.
    t := pingMessage
    if pong {
        t = pongMessage
    }
    rev := s.Cluster.Revision()
    id := s.Cluster.Id()
    url := s.Cluster.URL()
    m := &message{t, rev, id, url, gossip}
    utils.Print("GOSSIP", "SEND addr=%s type=%d", addr, t)
    s.send(addr, m)
}

func (s *GossipServer) heartbeat() {
    var addr *net.UDPAddr

    // We mix a list of active nodes (favoring suspicious if
    // there are currently some) and seeds. The reason to mix
    // the seeds is to ensure that at cluster creation time, we
    // don't end up with two split clusters.
    var nodes []*core.Node

    if s.Cluster.HasSuspicious() {
        nodes = s.Cluster.Suspicious()
    }
    if nodes == nil || len(nodes) == 0 {
        nodes = s.Cluster.Others()
    }
    if nodes == nil {
        nodes = make([]*core.Node, 0, 0)
    }
    if len(nodes)+len(s.seeds) == 0 {
        return
    }
    index := rand.Int() % (len(nodes) + len(s.seeds))
    if index < len(nodes) {
        node := nodes[index]
        addr, _ = utils.UDPAddr(node.Addr, "", utils.DefaultPort)
        if addr != nil {
            // We're going to send, so assume the packet has dropped
            // and all will be reset when we actually get a response.
            node.IncDropped()
        }
    } else {
        // Pick a seed and send a ping.
        seed := s.seeds[index-len(nodes)]
        addr, _ = utils.UDPAddr(seed, "", utils.DefaultPort)
    }

    // Send a packet if we've got an address.
    if addr != nil {
        go s.sendPingPong(addr, false)
    }
}

func (s *GossipServer) Sender() {
    for {
        if s.Cluster.HasSuspicious() {
            s.heartbeat()
            time.Sleep(time.Duration(MinHeartbeat) * time.Millisecond)
        } else {
            s.heartbeat()
            time.Sleep(time.Duration(MaxHeartbeat) * time.Millisecond)
        }
    }
}

func NewGossipServer(c *cluster.Cluster, addr string, port uint, seeds []string) (*GossipServer, error) {
    udpaddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", addr, port))
    if err != nil {
        return nil, err
    }

    conn, err := net.ListenUDP("udp", udpaddr)
    if err != nil {
        return nil, err
    }

    gs := new(GossipServer)
    gs.Cluster = c
    gs.conn = conn
    gs.seeds = seeds
    return gs, nil
}

func (s *GossipServer) process(addr *net.UDPAddr, m *message) {
    // Check for our own id (prevents broadcast from looping back).
    if m.Id == s.Cluster.Id() {
        return
    }

    // Debug print.
    utils.Print("GOSSIP", "RECV addr=%s type=%d", addr, m.Type)

    // Update the cluster status based on gossip.
    go func() {
        hint := s.Cluster.GossipUpdate(addr, m.Id, m.URL, m.Revision, m.Dead)
        if hint != nil {
            addr, _ = utils.UDPAddr(hint.Addr, "", utils.DefaultPort)
            go s.sendPingPong(addr, true)
        }
    }()

    if m.Type == pingMessage && s.Cluster.Active() {
        // Respond to the ping.
        go s.sendPingPong(addr, true)
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
        var m message
        err = json.Unmarshal(packet[0:n], &m)
        if err != nil {
            continue
        }

        // Process whenever.
        go s.process(addr, &m)
    }
}

func (s *GossipServer) Run() {
    go s.Sender()
    s.Serve()
}
