package utils

import (
    "io"
    "encoding/hex"
    "crypto/sha1"
)

func Hash(id string) string {
    h := sha1.New()
    io.WriteString(h, id)
    return hex.EncodeToString(h.Sum(nil))
}

func MHash(ids []string) string {
    h := sha1.New()
    for _, id := range ids {
        io.WriteString(h, id)
    }
    return hex.EncodeToString(h.Sum(nil))
}

