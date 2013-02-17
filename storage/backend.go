package storage

import (
	"os"
        "log"
)

var DEFAULT_PATH = "/var/lib/hibera"

type Backend struct {
}

func NewBackend(path string) *Backend {
        if len(path) == 0 {
            path = DEFAULT_PATH
        }

	err := os.MkdirAll(path, 0644)
        if err != nil {
            log.Fatal("Error initializing storage: ", err)
            return nil
        }

	return new(Backend)
}
