package main

import (
	"flag"
	"log"
	"hibera/storage"
	"hibera/server"
)

var bind = flag.String("bind", "", "Bind address for the server.")
var port = flag.Int("port", 2033, "Bind port for the server.")
var path = flag.String("path", "/var/lib/hibera/", "Backing storage path.")

func main() {
	flag.Parse()

	err := storage.Run(*path)
	if err != nil {
		log.Fatal("Error initializing storage: ", err)
		return
	}

	err = server.Run(*bind, *port)
	if err != nil {
		log.Fatal("Error starting server: ", err)
		return
	}
}
