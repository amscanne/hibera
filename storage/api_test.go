package storage

import (
    "fmt"
    "io/ioutil"
    "os"
    "testing"
    "time"
)

func Setup(t *testing.T) *Store {
    log, err := ioutil.TempDir("", "log")
    if err != nil && t != nil {
        t.Fatal(err)
    }

    data, err := ioutil.TempDir("", "data")
    if err != nil && t != nil {
        t.Fatal(err)
    }

    store, err := NewStore(log, data)
    if err != nil && t != nil {
        t.Fatal(err)
    }

    return store
}

func Teardown(store *Store) {
    os.RemoveAll(store.logs.logPath)
    os.RemoveAll(store.logs.dataPath)
}

func doBenchmark(b *testing.B, unique bool) {
    store := Setup(nil)
    if store == nil {
        return
    }
    defer Teardown(store)

    total := uint64(0)
    data := make([]byte, 128, 128)
    metadata := make([]byte, 128, 128)

    // Start the store.
    go store.Run()

    // Fire off transactions, and wait for all to complete.
    done := make(chan time.Duration)
    for i := 0; i < b.N; i += 1 {
        go func(i int) {
            start := time.Now()
            if unique {
                store.Write(fmt.Sprintf("a.%d", i), data, metadata)
            } else {
                store.Write("a", data, metadata)
            }
            done <- time.Since(start)
        }(i)
    }

    // Wait for all to complete.
    for i := 0; i < b.N; i += 1 {
        ns := <-done
        total += uint64(ns / time.Millisecond)
    }

    // Stop the store.
    store.Stop()

    // Print the average latency.
    b.Logf("average latency (N=%d): %.2f ms\n", b.N, float64(total/uint64(b.N)))
}

func BenchmarkWriteUnique(b *testing.B) {
    doBenchmark(b, true)
}
func BenchmarkWriteSame(b *testing.B) {
    doBenchmark(b, false)
}
