package cluster

import (
    "hibera/core"
    "hibera/utils"
    "testing"
)

var DefaultSize = uint(128)

func Setup(size uint, keys uint) *ring {
    nodes := core.NewTestNodes(size+1, keys+1)
    return NewRing(3, nodes)
}

func TestHash(t *testing.T) {
    if utils.Hash("a") == utils.Hash("b") {
        t.Fail()
    }
    if utils.Hash("a") != utils.Hash("a") {
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
        r.NodesFor(core.Key{"", ""})
    }
}

func BenchmarkUncached(b *testing.B) {
    r := Setup(DefaultSize, DefaultKeys)
    for i := 0; i < b.N; i += 1 {
        r.lookup(utils.Hash(""))
    }
}
