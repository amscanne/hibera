package core

import (
    "testing"
)

var DefaultSize = uint(128)

func Setup(size uint, keys uint) *ring {
    nodes := NewTestNodes(size+1, keys+1, domains("test"))
    return NewRing(nodes)
}

func TestHash(t *testing.T) {
    if hash("a") == hash("b") {
        t.Fail()
    }
    if hash("a") != hash("a") {
        t.Fail()
    }
}

func BenchmarkRecomputeBySize(b *testing.B) {
    Setup(uint(b.N), DefaultKeys)
}

func BenchmarkRecomputeByKeys(b *testing.B) {
    Setup(DefaultSize, uint(b.N))
}

func BenchmarkCached(b *testing.B) {
    r := Setup(DefaultSize, DefaultKeys)
    for i := 0; i < b.N; i += 1 {
        r.NodesFor(Key(""))
    }
}

func BenchmarkUncached(b *testing.B) {
    r := Setup(DefaultSize, DefaultKeys)
    for i := 0; i < b.N; i += 1 {
        r.lookup(hash(""))
    }
}
