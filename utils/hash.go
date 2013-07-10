package utils

import (
    "crypto/sha1"
    "encoding/hex"
    "io"
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
