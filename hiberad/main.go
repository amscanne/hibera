package main

import (
    "flag"
    "log"
    "strings"
    "time"
    "os"
    "runtime/pprof"
    "math/rand"
    "hibera/storage"
    "hibera/cluster"
    "hibera/server"
    "hibera/client"
)

var auth = flag.String("auth", "", "Authorization key.")
var bind = flag.String("bind", server.DefaultBind, "Bind address for the server.")
var port = flag.Uint("port", client.DefaultPort, "Bind port for the server.")
var path = flag.String("path", storage.DefaultPath, "Backing storage path.")
var domain = flag.String("domain", cluster.DefaultDomain, "Failure domain for this server.")
var keys = flag.Uint("keys", cluster.DefaultKeys, "The number of keys for this node (weight).")
var seeds = flag.String("seeds", server.DefaultSeeds, "Seeds for joining the cluster.")
var active = flag.Uint("active", server.DefaultActive, "Maximum active simutaneous clients.")
var profile = flag.String("profile", "", "Enabling profiling and write to file.")

func main() {
    // NOTE: We need the random number generator,
    // as it will be seed with 1 by default (and
    // hence always exhibit the same sequence).
    rand.Seed(time.Now().UTC().UnixNano())

    flag.Parse()

    // Turn on profiling.
    if *profile != "" {
        f, err := os.Create(*profile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

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
    c := cluster.NewCluster(backend, *auth, *domain, ids)
    if c == nil {
        log.Fatal("Unable to create cluster.")
    }

    // Startup our server.
    s := server.NewServer(c, *bind, *port, strings.Split(*seeds, ","), *active)
    if s == nil {
        return
    }

    // Run our server.
    s.Run()
}
