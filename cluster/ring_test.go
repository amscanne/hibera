package cluster

import (
    "github.com/amscanne/hibera/core"
    "github.com/amscanne/hibera/utils"
    "testing"
)

var TestSize = uint(128)

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
    Setup(uint(b.N), utils.DefaultKeys)
}

func BenchmarkRecomputeByKeys(b *testing.B) {
    Setup(TestSize, uint(b.N))
}

func BenchmarkCached(b *testing.B) {
    r := Setup(TestSize, utils.DefaultKeys)
    for i := 0; i < b.N; i += 1 {
        r.NodesFor(core.Key(""))
    }
}

func BenchmarkUncached(b *testing.B) {
    r := Setup(TestSize, utils.DefaultKeys)
    for i := 0; i < b.N; i += 1 {
        r.lookup(utils.Hash(""))
    }
}
