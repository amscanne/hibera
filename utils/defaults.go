package utils

import (
    "os"
)

// The default path for the storage backend.
var DefaultPath = "/var/lib/hibera"

// The default replication factor for the cluster.
// Note that we will use 2*N + 1 as the actual copies.
var DefaultN = uint(1)

// The default keys per node assigned in the ring.
// Each node can start with a custom number of keys,
// which may be related to how beefy the server is or
// how much storage capacity is available.
var DefaultKeys = uint(128)

// The default failure domain.
// This controls how nodes are admit and removed
// from the cluster. The default value is guessed
// as the hostname (assuming host-independent failures).
var DefaultDomain = defaultDomain()
func defaultDomain() string {
    hostname, err := os.Hostname()
    if err != nil {
        return ""
    }
    return hostname
}

// The default seed addresses when not in a cluster.
// On a physical network, we are able to send broadcast
// packets to find nearby Hibera nodes.
var DefaultSeeds = "255.255.255.255"

// The default bind address for services.
// By default we bind to INET_ANY (all) addreses.
var DefaultBind = ""

// The default limit for concurrent active connections.
// Due to file descriptor limits, we have admission control
// for client connections. This is prevent starvation where
// we need FDs in order to actually finish outstanding reqs.
var DefaultActive = uint(256)

// The default bind port.
var DefaultPort = uint(2033)

// The default host to connect to.
var DefaultHost = "127.0.0.1"
