package core

// A given namespace.
type Namespace string

// Key for access data.
type Key string

// Revision for a given key.
type Revision uint64

// Internal id for sync connections.
type EphemId uint64

// Map of all nodes in the cluster.
type NodeInfo map[string]*Node

// Individual access token (set of paths and permsissions).
type Token map[string]Perms

// Full set of permissions for a given namespace.
type NamespaceAccess map[string]Token

// Map of all access tokens in the cluster.
type AccessInfo map[Namespace]NamespaceAccess

// Representation of the cluster state.
type Info struct {
    Nodes  NodeInfo
    Access AccessInfo
}

func NewInfo() *Info {
    info := new(Info)
    info.Nodes = make(NodeInfo)
    info.Access = make(AccessInfo)
    return info
}
