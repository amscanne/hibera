package storage

import (
    "bytes"
    "fmt"
    "io/ioutil"
    "math/rand"
    "os"
    "sync/atomic"
    "testing"
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

func generateData(size int, vary_len bool) []byte {
    var length int
    if vary_len {
        length = rand.Int() % (2 * size)
    } else {
        length = size
    }
    start := rand.Int()
    data := make([]byte, length, length)
    for i := 0; i < length; i++ {
        data[i] = byte((start + i) % 256)
    }
    return data
}

func doBenchmark(b *testing.B, workers int, ops int, unique bool, data_len int, vary_len bool) {
    if ops < 10 {
        return
    }
    b.StopTimer()
    store := Setup(nil)
    if store == nil {
        return
    }
    defer Teardown(store)

    count := uint32(0)
    last_data := make(map[int][]byte)
    last_metadata := make(map[int][]byte)

    // Start the store.
    go store.Run()

    // Start timing.
    b.StartTimer()

    // Fire off transactions, and wait for all to complete.
    done := make(chan bool, workers)
    for i := 0; i < workers; i += 1 {
        go func(i int) {
            for {
                data := generateData(data_len/2, vary_len)
                metadata := generateData(data_len/2, vary_len)

                if unique {
                    key := fmt.Sprintf("a.%d", i)
                    store.Write(key, metadata, data)
                } else {
                    store.Write("a", metadata, data)
                }

                last_data[i] = data
                last_metadata[i] = metadata

                // NOTE; This is actually inefficient. But we
                // want to verify that everything is written properly
                // below, so we tolerate at least every worker writing
                // their keys once. Any overread from this specific
                // piece should be dominated in the long run anyways.
                if atomic.AddUint32(&count, 1) > uint32(ops) {
                    break
                }
            }
            done <- true
        }(i)
    }

    // Wait for all to complete.
    for i := 0; i < workers; i += 1 {
        <-done
    }

    // Stop the store.
    store.Stop()
    b.StopTimer()
    store.logs.open()
    defer store.logs.close()

    // Sanity check the results.
    if unique {
        for i := 0; i < workers; i += 1 {
            go func(i int) {
                key := fmt.Sprintf("a.%d", i)
                store_metadata, store_data, err := store.Read(key)
                if err != nil {
                    b.Fail()
                }
                if bytes.Compare(last_data[i], store_data) != 0 {
                    b.Fail()
                }
                if bytes.Compare(last_metadata[i], store_metadata) != 0 {
                    b.Fail()
                }
                done <- true
            }(i)
        }

        // Wait for all to complete.
        for i := 0; i < workers; i += 1 {
            <-done
        }
    } else {
        found := false
        store_metadata, store_data, err := store.Read("a")
        if err != nil {
            b.Fail()
        } else {
            for i := 0; i < workers; i += 1 {
                if bytes.Compare(store_data, last_data[i]) == 0 &&
                    bytes.Compare(store_metadata, last_metadata[i]) == 0 {
                    found = true
                    break
                }
            }
        }
        if !found {
            b.Fail()
        }
    }
}

func Benchmark1Writer4KBytesUniqueFixed(b *testing.B) {
    b.SetBytes(4096)
    doBenchmark(b, 1, b.N, true, 4096, false)
}
func Benchmark1Writer4KBytesSameFixed(b *testing.B) {
    b.SetBytes(4096)
    doBenchmark(b, 1, b.N, false, 4096, false)
}

func Benchmark1Writer4KBytesUniqueRandom(b *testing.B) {
    b.SetBytes(4096)
    doBenchmark(b, 1, b.N, true, 4096, true)
}
func Benchmark1Writer4KBytesSameRandom(b *testing.B) {
    b.SetBytes(4096)
    doBenchmark(b, 1, b.N, false, 4096, true)
}

func BenchmarkNWriters4KBytesUniqueFixed(b *testing.B) {
    b.SetBytes(4096)
    doBenchmark(b, b.N, b.N, true, 4096, false)
}
func BenchmarkNWriters4KBytesSameFixed(b *testing.B) {
    b.SetBytes(4096)
    doBenchmark(b, b.N, b.N, false, 4096, false)
}

func BenchmarkNWriters4KBytesUniqueRandom(b *testing.B) {
    b.SetBytes(4096)
    doBenchmark(b, b.N, b.N, true, 4096, true)
}
func BenchmarkNWriters4KBytesSameRandom(b *testing.B) {
    b.SetBytes(4096)
    doBenchmark(b, b.N, b.N, false, 4096, true)
}
