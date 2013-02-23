package main

import (
    "flag"
    "log"
    "strings"
    "time"
    "math/rand"
    "hibera/storage"
    "hibera/server"
    "hibera/client"
    "hibera/core"
)

var bind = flag.String("bind", server.DefaultBind, "Bind address for the server.")
var port = flag.Uint("port", client.DefaultPort, "Bind port for the server.")
var path = flag.String("path", storage.DefaultPath, "Backing storage path.")
var domain = flag.String("domain", core.DefaultDomain, "Failure domain for this server.")
var keys = flag.Uint("keys", core.DefaultKeys, "The number of keys for this node (weight).")
var seeds = flag.String("seeds", server.DefaultSeeds, "Seeds for joining the cluster.")

func main() {
    // NOTE: We need the random number generator,
    // as it will be seed with 1 by default (and
    // hence always exhibit the same sequence).
    rand.Seed(time.Now().UTC().UnixNano())

    flag.Parse()

    // Initialize our storage.
    backend := storage.NewBackend(*path)
    if backend == nil {
        return
    }
    go backend.Run()

    // Create our cluster.
    // We load our keys from the persistent storage.
    ids, err := backend.LoadIds(*keys)
    if err != nil {
        log.Fatal("Unable to load keys: ", err)
    }
    cluster := core.NewCluster(backend, *domain, ids)
    if cluster == nil {
        return
    }

    // Initialize our hub.
    hub := core.NewHub(cluster)
    if hub == nil {
        return
    }

    // Startup our server.
    s := server.NewServer(hub, cluster, *bind, *port, strings.Split(*seeds, ","))
    if s == nil {
        return
    }

    // Run our server.
    s.Run()
}
