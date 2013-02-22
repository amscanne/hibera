package core

import (
        "testing"
)

var DefaultSize = uint(128)

func Setup(size uint, keys uint) *ring {
    nodes := NewTestNodes(size+1, keys+1)
    self := nodes.Active()[0]
    return NewRing(self, nodes)
}

func TestHash(t *testing.T) {
    r := Setup(0, 0)
    if r.hash("a") == r.hash("b") {
        t.Fail()
    }
    if r.hash("a") != r.hash("a") {
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
        r.lookup(r.hash(""))
    }
}
