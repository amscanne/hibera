package core

import (
	"net"
	"sync"
	"fmt"
	"encoding/json"
	"hibera/storage"
	"hibera/client"
)

type Key string
type Revision uint64
type EphemId uint64
type SubName string

var DefaultKeys = uint(128)
var HiberaKey = Key("hibera")

type Redirect struct {
	URL string
}

func NewRedirect(URL string) *Redirect {
	return &Redirect{URL}
}

func (r Redirect) Error() string {
	return r.URL
}

type Phase uint

var PhaseNone = Phase(0)
var PhasePropose = Phase(1)
var PhaseAccept = Phase(2)

type ClusterUpdate struct {
	// Nodes to be added/activated in the cluster.
	add []string

	// Nodes to be dropped/deactivated in the cluster.
	remove []string
}

type Cluster struct {
	// Our internal id.
	id string

	// The phase for paxos.
	Phase

	// The cluster version.
	version  uint64
	promised uint64
	sync.Mutex

	// Our local database.
	// Some of these functions are
	// exported and used directly by
	// called (HTTPServer).
	*local

	// Our node map.
	// This is computed based on the
	// gossip server and represents
	// consensus of the cluster.
	*Nodes

	// Our ring.
	// This routes requests, etc.
	// We put our domain there and the
	// domain of all nodes in the system.
	// We update it whenever the nodes
	// change.
	*ring
}

func (c *Cluster) Id() string {
	return c.id
}

func NewCluster(backend *storage.Backend, domain string, ids []string) *Cluster {
	uuid, err := storage.Uuid()
	if err != nil {
		return nil
	}

	c := new(Cluster)
	c.id = uuid
	c.Nodes = NewNodes()
	c.local = NewLocal(backend)
	c.ring = NewRing(c.Nodes)

	return c
}

func (c *Cluster) Version() uint64 {
	return 0
}

func (c *Cluster) ProposeVersion() uint64 {
	return 0
}

func (c *Cluster) IsProposePhase() bool {
	return c.doPropose()
}

func (c *Cluster) IsAcceptPhase() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	return (c.Phase == PhaseAccept)
}

func (c *Cluster) PaxosNodes() []*Node {
	return c.ring.NodesFor("")
}

func (c *Cluster) ActiveNodes() []*Node {
	return c.Nodes.Active()
}

func (c *Cluster) DeadNodes() []string {
	return c.Nodes.Dead()
}

func (c *Cluster) OnDrop(addr *net.UDPAddr) {
	c.Nodes.markDrop(addr)
}

func (c *Cluster) OnPing(addr *net.UDPAddr, version uint64, id string, dead []string) {
	c.Nodes.markSeen(addr, version, id)
	c.Nodes.markDead(dead)
}

func (c *Cluster) OnPong(addr *net.UDPAddr, version uint64, id string, dead []string) {
	c.Nodes.markSeen(addr, version, id)
	c.Nodes.markDead(dead)
}

func (c *Cluster) doPropose() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.ring.IsMaster(HiberaKey) {
		if c.Phase != PhaseNone {
			return (c.Phase == PhasePropose)
		}

		// See if there's anything to propose.
		update := c.Nodes.GenerateUpdate()
		if update == nil {
			return false
		}
		c.saveDelta(c.version+1, update)

		// Kick off the propose phase.
		c.Phase = PhasePropose
		return true
	}

	return false
}

func (c *Cluster) OnPropose(addr *net.UDPAddr, version uint64) bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.ring.IsMaster(HiberaKey) {
		// Uh-oh, that's bizarre. We'd better
		// reset our reset and see if the cluster
		// becomes consistent later on.
		c.Phase = PhaseNone
	}

	if version >= c.promised {
		c.promised = version
		return true
	}
	return false
}

func (c *Cluster) OnPromise(addr *net.UDPAddr, version uint64) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.ring.IsMaster(HiberaKey) {
		if version != (c.version + 1) {
			// Stale message? Ignore this and the cluster will
			// sort itself out very shortly.
			return
		}

		if c.Phase == PhasePropose {
			paxosNodes := c.PaxosNodes()
			count := 0
			for _, node := range paxosNodes {
				if node.addr == addr {
					node.promised = version
				}
				if node.promised == version {
					count += 1
				}
			}
			if !(count > (len(paxosNodes) / 2)) {
				// Still a minority, clear looking.
				return
			}

			// We set the phase to accept. The gossip server
			// will send out messages very shortly.
			c.Phase = PhaseAccept
		}
	} else {
		// Ignore. It must be from an old round. The other
		// node will eventually update their cluster.
	}
}

func (c *Cluster) OnNoPromise(addr *net.UDPAddr, version uint64) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Uh-oh. Someone has a newer version than us.
	// We'll rely on the other parts of the system
	// to suck down the latest cluster and we'll stop
	// participating in Paxos.
	c.Phase = PhaseNone
}

func (c *Cluster) readDelta(version uint64) (*ClusterUpdate, error) {
	// Construct a client to read the update.
	api := client.NewHiberaAPI([]string{c.ring.MasterFor(Key(HiberaKey)).String()}, c.id, 0)
	key := fmt.Sprintf("%s.%d", string(HiberaKey), version)
	value, _, err := api.Get(key)
	if err != nil {
		return nil, err
	}

	// Load the update.
	var update ClusterUpdate
	err = json.Unmarshal(value, &update)
	if err != nil {
		return nil, err
	}
	return &update, nil
}

func (c *Cluster) saveDelta(version uint64, update *ClusterUpdate) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Serialize the update.
	value, err := json.Marshal(update)
	if err != nil {
		return nil
	}

	// Construct a client to write the update.
	api := client.NewHiberaAPI([]string{c.ring.MasterFor(HiberaKey).String()}, c.id, 0)
	key := fmt.Sprintf("%s.%d", string(HiberaKey), version)
	_, err = api.Set(key, value, 0)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cluster) OnAccept(addr *net.UDPAddr, version uint64) bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.ring.IsMaster(HiberaKey) {
		// This is unexpected. Should work itself out.
		// In the meantime, abort our current update.
		c.Phase = PhaseNone

	} else {
		// We've got the accept. Let's commit and update
		// our cluster configuration.
		if version >= c.promised {
			// Save the version.
			if version == (c.version + 1) {
				// Update our cluster configuration.
				update, err := c.readDelta(version)
				if err != nil {
					c.version = version
					c.Nodes.ApplyUpdate(update)
					return true
				}
			} else {
				// We are not going to be able to read
				// an increment update, we need to be
				// able to kick off a stop-the-clock
				// reload here.
			}
		}
	}

	return false
}

func (c *Cluster) OnAccepted(addr *net.UDPAddr, version uint64) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.ring.IsMaster(HiberaKey) {
		if version != (c.version + 1) {
			// As per OnPromise, something must be up here.
			// No doubt the cluster will sort itself out soon.
			return
		}

		if c.Phase == PhaseAccept {
			paxosNodes := c.PaxosNodes()
			count := 0
			for _, node := range paxosNodes {
				if node.addr == addr {
					node.accepted = version
				}
				if node.accepted == version {
					count += 1
				}
			}
			if !(count > (len(paxosNodes) / 2)) {
				// Still a minority, clear looking.
				return
			}

			// We've already committed the data,
			// bump our version here and it'll
			// start sending out and people will
			// read it.
			c.version = version
			c.Phase = PhaseNone
		}
	}
}

func (c *Cluster) Check(key Key) error {
	return Redirect{""}
}
