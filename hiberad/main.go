package main

import (
	"flag"
	"strings"
	"hibera/storage"
	"hibera/server"
	"hibera/core"
)

var bind = flag.String("bind", server.DEFAULT_BIND, "Bind address for the server.")
var port = flag.Uint("port", server.DEFAULT_PORT, "Bind port for the server.")
var path = flag.String("path", storage.DEFAULT_PATH, "Backing storage path.")
var domain = flag.String("domain", core.DEFAULT_DOMAIN, "Failure domain for this server.")
var keys = flag.Uint("keys", core.DEFAULT_KEYS, "The number of keys for this node (weight).")
var seeds = flag.String("seeds", "", "Seeds for joining the cluster.")

func main() {
	flag.Parse()

	// Initialize our storage.
	backend := storage.NewBackend(*path)
	if backend == nil {
		return
	}

	// Initialize our core.
	core := core.NewCore(*domain, *keys, backend)
	if core == nil {
		return
	}

	// Startup our server.
	s := server.NewServer(core, *bind, *port, strings.Split(*seeds, ","))
	if s == nil {
		return
	}

	// Run our server.
	go backend.Run()
	s.Run()
}
