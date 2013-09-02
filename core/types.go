package core

import (
    "math/big"
    "regexp"
)

// A given namespace.
type Namespace string

// Key for data access.
type Key string

// Revision for a given key.
type Revision struct {
    *big.Int
}

// Internal id for sync connections.
type EphemId uint64

// Map of all nodes in the cluster.
type NodeInfo map[string]*Node

// Permissions.
type PermissionInfo struct {
    Read    bool `json:"read"`
    Write   bool `json:"write"`
    Execute bool `json:"execute"`

    // Cached regular expression.
    re  *regexp.Regexp
}

// An authentication token.
type Token string

// Individual access token (set of paths and permsissions).
type Permissions map[string]PermissionInfo

// Full set of permissions for a given namespace.
type NamespaceAccess map[Token]Permissions

// Map of all access tokens in the cluster.
type AccessInfo map[Namespace]NamespaceAccess

// Information about a synchronization group.
type SyncInfo struct {
    Index   int      `json:"index"`
    Members []string `json:"members"`
}

var NoSyncInfo = SyncInfo{-1, make([]string, 0, 0)}

// Representation of the cluster state.
type Info struct {
    N      uint       `json:"replication"`
    Nodes  NodeInfo   `json:"nodes"`
    Access AccessInfo `json:"access"`
}

func NewInfo() *Info {
    info := new(Info)
    info.Nodes = make(NodeInfo)
    info.Access = make(AccessInfo)
    return info
}
