package core

/*
hibera <- full cluster, always updated by master

hibera.0 <- always empty
hibera.1 <- delta for rev 1
hibera.2 <- delta for rev 2
hibera.N <- delta for rev N

import (
    "sort"
)

type Ring struct {
    nodemap map[Key]*Node
    index []Key
}

if len(index) == 0 {
    return nil
}

nodes := make([]*Node)
key := NodeId
start := sort.Search(len(index), func(i int) bool { return index[i] >= key})
cur := i
for {
    // Check if nodemap[index[cur]] satisfies our model.

    // Move on to the next one.
    cur += 1
    if cur >= len(index) {
        cur = 0
    }
    if cur == start {
        break
    }
}

func (r *Ring) Master() *Node {
}

func (r *Ring) IsMaster() bool {
}

func (r *Ring) Slaves() []*Node {
}

func (r *Ring) IsSlave() bool {
}

func (r *Ring) AddNode(*Node) {
}

func (r *Ring) RemoveNode(*Node) {
}

 Client      Servers
   |         |  |  | --- First Request ---
   X-------->|  |  |  Request
   |         X->|->|  Prepare(N)
   |         |<-X--X  Promise(N,I,{Va,Vb,Vc})
   |         X->|->|  Accept!(N,I,Vn)
   |         |<-X--X  Accepted(N,I)
   |<--------X  |  |  Response
   |         |  |  |

 Client      Servers
   X-------->|  |  |  Request
   |         X->|->|  Accept!(N,I+1,W)
   |         |<-X--X  Accepted(N,I+1)
   |<--------X  |  |  Response
   |         |  |  |

OnClusterChange() {
    if Cluster.IsMaster() {
        Slave.Stop()
        Master.Start(Cluster.Slaves())
    } else if Cluster.IsSlave() {
        if Cluster.Master() in dead  &&
           Cluster.IsBackupMaster() {
            // Try to take over as the leader in
            // case the master appears to be down.
            // We know that our proposal will not
            // include the Master (as it's in our
            // dead list). If the master is up, 
            // this may cause a failed round, but
            // the second we hear from the master
            // we will stop running this.
            Master.Start()
        } else {
            Master.Stop()
        }
        Master.Stop()
        Slave.Start(Cluster.Master())
    } else {
        Master.Stop()
        Slave.Stop()
    }
}

OnDeadChange() {
    OnClusterChange()
    if len(dead) != 0 && Cluster.IsMaster() {
        Master.DoProposal()
    }
}

ProposeValue() {
    // Write out our proposed value.
    // This will only get used if we're accepted.
    write("hibera.cluster.%d", newrev)
    write("hibera.cluster.diff.%d", newrev)
}

MasterOnAccepted() {
    // Update our rev (which will go out on gossips).
    // This will trigger fetches of the updated cluster map
    // and resynchronization across the cluster nodes.
    rev += 1
}

ClientOnAccept() {
    // Master will update the cluster value.
    // We don't have any special behavior here.
}

ClientOnNewrev(old, new) {
    // Migrate from the Ring old -> new. Set this up.
    // This will require lazy synchronization of all the state.
    rev = new
}
*/
