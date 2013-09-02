package storage

import (
    "fmt"
    "io/ioutil"
    "os"
    "bytes"
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

func doBenchmark(b *testing.B, workers int, ops int, unique bool, data_len int) {
    b.StopTimer()
    store := Setup(nil)
    if store == nil {
        return
    }
    defer Teardown(store)

    data := make([]byte, data_len, data_len)
    unique_data := make(map[string][]byte)
    b.SetBytes(int64(2 * data_len))

    // Generate random data.
    for i := 0; i < len(data); i += 1 {
        data[i] = byte(i % 256)
    }
    for i := 0; i < workers; i += 1 {
        chunk_data := make([]byte, data_len, data_len)
        for j := 0; j < len(chunk_data); j += 1 {
            chunk_data[j] = byte((i+j) % 256)
        }
        unique_data[fmt.Sprintf("a.%d", i)] = chunk_data
    }

    // Start the store.
    go store.Run()

    // Start timing.
    b.StartTimer()

    // Fire off transactions, and wait for all to complete.
    done := make(chan bool, workers)
    for i := 0; i < workers; i += 1 {
        go func(i int) {
            for j := 0; j < ops; j += 1 {
                if unique {
                    key := fmt.Sprintf("a.%d", i)
                    store.Write(key, unique_data[key], unique_data[key])
                } else {
                    store.Write("a", data, data)
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
    store.logs.squashLogs()

    // Sanity check the results.
    for i:= 0; i < workers; i += 1 {
        go func(i int) {
            if unique {
                key := fmt.Sprintf("a.%d", i)
                store_data, store_metadata, err := store.Read(key)
                if err != nil {
                    b.Fail()
                }
                if bytes.Compare(unique_data[key], store_data) != 0 {
                    b.Fail()
                }
                if bytes.Compare(unique_data[key], store_metadata) != 0 {
                    b.Fail()
                }
            } else {
                store_data, store_metadata, err := store.Read("a")
                if err != nil {
                    b.Fail()
                }
                if bytes.Compare(data, store_data) != 0 {
                    b.Fail()
                }
                if bytes.Compare(data, store_metadata) != 0 {
                    b.Fail()
                }
            }
            done <- true
        }(i)
    }

    // Wait for all to complete.
    for i := 0; i < workers; i += 1 {
        <-done
    }
}

func BenchmarkNWriters1Ops4KBytesUnique(b *testing.B) {
    doBenchmark(b, b.N, 1, true, 4096)
}
func BenchmarkNWriters1Ops4KBytesSame(b *testing.B) {
    doBenchmark(b, b.N, 1, false, 4096)
}

func Benchmark100WritersNOps4KBytesUnique(b *testing.B) {
    doBenchmark(b, 100, b.N, true, 4096)
}
func Benchmark100WritersNOps4KBytesSame(b *testing.B) {
    doBenchmark(b, 100, b.N, false, 4096)
}

func Benchmark100Writers1OpsNBytesUnique(b *testing.B) {
    doBenchmark(b, 100, 1, true, b.N)
}
func Benchmark100Writers1OpsNBytesKSame(b *testing.B) {
    doBenchmark(b, 100, 1, false, b.N)
}
