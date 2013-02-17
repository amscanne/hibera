package core

import (
    "hibera/storage"
)

type Core struct {
    *storage.Backend
}

func NewCore(domain string, seeds []string, backend *storage.Backend) *Core {
    return &Core{backend}
}
