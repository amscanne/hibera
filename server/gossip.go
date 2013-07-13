package server

import (
    "code.google.com/p/goprotobuf/proto"
    "fmt"
    "hibera/client"
    "hibera/cluster"
    "hibera/core"
    "hibera/utils"
    "log"
    "math/rand"
    "net"
    "time"
)

type GossipServer struct {
    *cluster.Cluster
    conn  *net.UDPConn
    seeds []string
}

// The send addresses when not in a cluster.
var DefaultSeeds = "255.255.255.255"

// The frequency (in ms) of heartbeats.
var MinHeartbeat = 10
var MaxHeartbeat = 1000

// The number of dead servers to encode in a heartbeat.
var DeadServers = 5

func (s *GossipServer) send(addr *net.UDPAddr, m *Message) error {
    gossip := m.GetGossip()
    if gossip != nil {
        utils.Print("GOSSIP", "SEND %s (addr=%s,version=%d,id=%s)",
            TYPE_name[int32(m.GetType())], addr, m.GetVersion(), *gossip.Id)
    } else {
        utils.Print("GOSSIP", "SEND %s (addr=%s,version=%d)",
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
        perm = perm[0:DeadServers]
    }
    gossip := make([]string, len(perm))
    for i, v := range perm {
        gossip[i] = dead[v].Id()
        utils.Print("GOSSIP", "DEAD %s", gossip[i])
    }

    // Build our ping message.
    t := uint32(TYPE_PING)
    version := uint64(s.Cluster.Version())
    nodes := uint64(s.Cluster.Count())
    id := s.Cluster.Id()
    if pong {
        t = uint32(TYPE_PONG)
    }
    m := &Message{&t, &version, &nodes, &Gossip{&id, gossip, nil}, nil}
    s.send(addr, m)
}

func (s *GossipServer) heartbeat() {
    var addr *net.UDPAddr

    // We mix a list of active nodes (favoring suspicious if
    // there are currently some) and seeds. The reason to mix
    // the seeds is to ensure that at cluster creation time, we
    // don't end up with two split clusters.
    nodes := s.Cluster.Suspicious(s.Cluster.Version())
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
        addr, _ = utils.GenerateUDPAddr(node.Addr, "", client.DefaultPort)
        if addr != nil {
            // We're going to send, so assume the packet has dropped
            // and all will be reset when we actually get a response.
            node.IncDropped()
        }
    } else {
        // Pick a seed and send a ping.
        seed := s.seeds[index-len(nodes)]
        addr, _ = utils.GenerateUDPAddr(seed, "", client.DefaultPort)
    }

    // Send a packet if we've got an address.
    if addr != nil {
        go s.sendPingPong(addr, false)
    }
}

func (s *GossipServer) Sender() {
    for {
        if s.Cluster.HasSuspicious(s.Cluster.Version()) {
            s.heartbeat()
            time.Sleep(time.Duration(MinHeartbeat) * time.Millisecond)
        } else {
            s.heartbeat()
            time.Sleep(time.Duration(MaxHeartbeat) * time.Millisecond)
        }
    }
}

func NewGossipServer(c *cluster.Cluster, addr string, port uint, seeds []string) *GossipServer {
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
    gs.Cluster = c
    gs.conn = conn
    gs.seeds = seeds
    return gs
}

func (s *GossipServer) process(addr *net.UDPAddr, m *Message) {
    // Check for our own id (prevents broadcast from looping back).
    gossip := m.GetGossip()
    if gossip != nil && *gossip.Id == s.Cluster.Nodes.Self().Id() {
        return
    }

    // Heartbeat.
    s.Cluster.Heartbeat(*gossip.Id, addr,
        core.Revision(m.GetVersion()), m.GetNodes(), gossip.Dead)

    if m.GetType() == uint32(TYPE_PING) && s.Cluster.Version() > 0 {
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
