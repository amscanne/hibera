package core

import (
    "fmt"
    "math/big"
    "regexp"
)

// A given namespace.
type Namespace string

// Key for data access.
type Key struct {
    Namespace
    Key string
}

func (k Key) String() string {
    return fmt.Sprintf("%s/%s", k.Namespace, k.Key)
}

// Revision for a given key.
type Revision *big.Int

// Internal id for sync connections.
type EphemId uint64

// Map of all nodes in the cluster.
type NodeInfo map[string]*Node

// Permissions.
type Perms struct {
    Read    bool `json:"read"`
    Write   bool `json:"write"`
    Execute bool `json:"execute"`

    // Cached regular expression.
    re  *regexp.Regexp
}

// Individual access token (set of paths and permsissions).
type Token map[string]Perms

// Full set of permissions for a given namespace.
type NamespaceAccess map[string]Token

// Map of all access tokens in the cluster.
type AccessInfo map[Namespace]NamespaceAccess

// Information about a synchronization group.
type SyncInfo struct {
    Index   int      `json:"index"`
    Members []string `json:"members"`
}

// Representation of the cluster state.
type Info struct {
    Nodes  NodeInfo   `json:"nodes"`
    Access AccessInfo `json:"access"`
}

func NewInfo() *Info {
    info := new(Info)
    info.Nodes = make(NodeInfo)
    info.Access = make(AccessInfo)
    return info
}
