package storage

import (
    "fmt"
    "io/ioutil"
    "os"
    "testing"
    "time"
)

// The storage space required for empty entries.
// Note that this was discovered experimentally,
// there's nothing magical about this value at all.
var BytesPerEntryMin = 96
var BytesPerEntryMax = 100

func Setup(t *testing.T) *Backend {
    log, err := ioutil.TempDir("", "log")
    if err != nil && t != nil {
        t.Fatal(err)
    }

    data, err := ioutil.TempDir("", "data")
    if err != nil && t != nil {
        t.Fatal(err)
    }

    backend, err := NewBackend(log, data)
    if err != nil && t != nil {
        t.Fatal(err)
    }

    return backend
}

func Check(
    t *testing.T,
    backend *Backend,
    waiters []func(),
    key_count int,
    data_size int,
    log_size int,
    handled int) {

    // Synchronize the backend.
    go backend.Run()
    for _, waiter := range waiters {
        waiter()
    }
    backend.Stop()

    // Assert the truth.
    entries, err := backend.List()
    if err != nil {
        t.Error(err)
    }
    if len(entries) != key_count {
        t.Errorf("entries=%d expected=%d",
            len(entries), key_count)
    }
    if backend.data_entries != uint64(data_size) {
        t.Errorf("data_entries=%d expected=%d",
            backend.data_entries, data_size)
    }
    if backend.log_size < int64(log_size*BytesPerEntryMin) ||
        backend.log_size > int64(log_size*BytesPerEntryMax) {
        t.Errorf("log_size=%d expected=%d",
            backend.log_size, log_size)
    }
    if backend.handled != uint64(handled) {
        t.Errorf("handled=%d expected=%d",
            backend.handled, handled)
    }
}

func doWrite(backend *Backend, count int) func() {
    done := make(chan bool)

    // Fire off a goroutine for every write() call.
    for i := 0; i < count; i += 1 {
        go func(i int) {
            backend.Write(fmt.Sprintf("a.%d", i), []byte{}, []byte{})
            done <- true
        }(i)
    }

    // Return a function that allows us to block
    // until all the writes are complete (flushed).
    wait := func() {
        for i := 0; i < count; i += 1 {
            <-done
        }
    }
    return wait
}

func Teardown(backend *Backend) {
    os.RemoveAll(backend.logPath)
    os.RemoveAll(backend.dataPath)
}

func TestNone(t *testing.T) {
    backend := Setup(t)
    defer Teardown(backend)
    Check(t, backend, []func(){}, 0, 0, 0, 0)
    backend.squashLogs()
    Check(t, backend, []func(){}, 0, 0, 0, 0)
}

func TestOne(t *testing.T) {
    backend := Setup(t)
    defer Teardown(backend)
    wait := doWrite(backend, 1)
    Check(t, backend, []func(){wait}, 1, 0, 1, 1)
    backend.squashLogs()
    Check(t, backend, []func(){}, 1, 1, 0, 1)
}

func TestOverwrite(t *testing.T) {
    backend := Setup(t)
    defer Teardown(backend)
    wait1 := doWrite(backend, 1)
    wait2 := doWrite(backend, 1)
    Check(t, backend, []func(){wait1, wait2}, 1, 0, 2, 2)
    backend.squashLogs()
    Check(t, backend, []func(){}, 1, 1, 0, 2)
}

func TestHalfBatch(t *testing.T) {
    backend := Setup(t)
    defer Teardown(backend)
    wait := doWrite(backend, MaximumLogBatch/2)
    Check(t, backend, []func(){wait}, MaximumLogBatch/2, 0, MaximumLogBatch/2, MaximumLogBatch/2)
    backend.squashLogs()
    Check(t, backend, []func(){}, MaximumLogBatch/2, MaximumLogBatch/2, 0, MaximumLogBatch/2)
}

func TestHalfBatchOverwrite(t *testing.T) {
    backend := Setup(t)
    defer Teardown(backend)
    wait1 := doWrite(backend, MaximumLogBatch/2)
    wait2 := doWrite(backend, MaximumLogBatch/2)
    Check(t, backend, []func(){wait1, wait2}, MaximumLogBatch/2, 0, MaximumLogBatch, MaximumLogBatch)
    backend.squashLogs()
    Check(t, backend, []func(){}, MaximumLogBatch/2, MaximumLogBatch/2, 0, MaximumLogBatch)
}

func TestFullBatch(t *testing.T) {
    backend := Setup(t)
    defer Teardown(backend)
    wait := doWrite(backend, MaximumLogBatch)
    Check(t, backend, []func(){wait}, MaximumLogBatch, 0, MaximumLogBatch, MaximumLogBatch)
    backend.squashLogs()
    Check(t, backend, []func(){}, MaximumLogBatch, MaximumLogBatch, 0, MaximumLogBatch)
}

func TestFullBatchPlusHalf(t *testing.T) {
    backend := Setup(t)
    defer Teardown(backend)
    wait1 := doWrite(backend, MaximumLogBatch)
    wait2 := doWrite(backend, MaximumLogBatch/2)
    Check(t, backend, []func(){wait1, wait2}, MaximumLogBatch, 0, 3*MaximumLogBatch/2, MaximumLogBatch*3/2)
    backend.squashLogs()
    Check(t, backend, []func(){}, MaximumLogBatch, MaximumLogBatch, 0, MaximumLogBatch*3/2)
}

func TestFullBatchOverwrite(t *testing.T) {
    backend := Setup(t)
    defer Teardown(backend)
    wait1 := doWrite(backend, MaximumLogBatch)
    wait2 := doWrite(backend, MaximumLogBatch)
    Check(t, backend, []func(){wait1, wait2}, MaximumLogBatch, 0, 2*MaximumLogBatch, MaximumLogBatch*2)
    backend.squashLogs()
    Check(t, backend, []func(){}, MaximumLogBatch, MaximumLogBatch, 0, MaximumLogBatch*2)
}

func doBenchmark(b *testing.B, unique bool) {
    backend := Setup(nil)
    if backend == nil {
        return
    }
    defer Teardown(backend)

    total := uint64(0)
    data := make([]byte, 128, 128)
    metadata := make([]byte, 128, 128)

    // Start the backend.
    go backend.Run()

    // Fire off transactions, and wait for all to complete.
    done := make(chan time.Duration)
    for i := 0; i < b.N; i += 1 {
        go func(i int) {
            start := time.Now()
            if unique {
                backend.Write(fmt.Sprintf("a.%d", i), data, metadata)
            } else {
                backend.Write("a", data, metadata)
            }
            done <- time.Since(start)
        }(i)
    }

    // Wait for all to complete.
    for i := 0; i < b.N; i += 1 {
        ns := <-done
        total += uint64(ns / time.Millisecond)
    }

    // Stop the backend.
    backend.Stop()

    // Print the average latency.
    b.Logf("average latency (N=%d): %.2f ms\n", b.N, float64(total/uint64(b.N)))
}

func BenchmarkWriteUnique(b *testing.B) {
    doBenchmark(b, true)
}
func BenchmarkWriteSame(b *testing.B) {
    doBenchmark(b, false)
}
