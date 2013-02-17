package storage

import (
	"os"
)

func Run(path string) error {
	err := os.MkdirAll(path, 0644)
	return err
}
