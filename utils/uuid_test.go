package utils

import (
    "testing"
)

func TestUuid(t *testing.T) {
    uuid, err := Uuid()
    if err != nil {
        t.Fail()
    }
    if uuid == "" {
        t.Fail()
    }
    uuid2, err := Uuid()
    if err != nil {
        t.Fail()
    }
    if uuid2 == "" {
        t.Fail()
    }
    if uuid == uuid2 {
        t.Fail()
    }
}

func TestUuids(t *testing.T) {
    dotest := func(n uint) {
        uuids, err := Uuids(n)
        if err != nil {
            t.Fail()
        }
        if uuids == nil {
            t.Fail()
        }
        if len(uuids) != int(n) {
            t.Fail()
        }
        for i, uuid1 := range uuids {
            if uuid1 == "" {
                t.Fail()
            }
            for j, uuid2 := range uuids {
                if i == j {
                    continue
                }
                if uuid1 == uuid2 {
                    t.Fail()
                }
            }
        }
    }
    dotest(0)
    dotest(1)
    dotest(2)
    dotest(32)
    dotest(64)
    dotest(128)
}

func BenchmarkUuids(b *testing.B) {
    for i := 0; i < b.N; i += 1 {
        Uuid()
    }
}
