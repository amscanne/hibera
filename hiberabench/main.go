package main

import (
    "fmt"
    "hibera/cli"
    "hibera/client"
    "hibera/core"
    "math/rand"
    "sync/atomic"
    "time"
)

var size = cli.Flags.Uint("size", 1024, "Data size.")
var workers = cli.Flags.Uint("workers", 512, "Number of workers.")
var ratio = cli.Flags.Float64("ratio", 0.5, "Write ratio for mixed workload.")
var duration = cli.Flags.Float64("duration", 10.0, "Duration of test (in seconds).")

var cliInfo = cli.Cli{
    "Hibera cluster benchmarking tool.",
    map[string]cli.Command{
        "read": cli.Command{
            "Run a read benchmark (requires write benchmark).",
            "",
            []string{"key"},
            []string{"workers", "duration"},
            true,
        },
        "write": cli.Command{
            "Run a write benchmark.",
            "",
            []string{"key"},
            []string{"size", "workers", "duration"},
            true,
        },
        "mixed": cli.Command{
            "Run a mixed benchmark.",
            "",
            []string{"key"},
            []string{"size", "workers", "ratio", "duration"},
            true,
        },
    },
    cli.Options,
}

func do_worker(
    c *client.HiberaAPI,
    keys []string,
    error_chan chan error,
    ops *uint64,
    data *uint64,
    work func(key string) (uint64, error)) {

    for {
        // Select a random key.
        key := keys[rand.Int()%len(keys)]

        // Do the work.
        n, err := work(key)
        if err != nil {
            error_chan <- err
        }

        // Update the stats.
        atomic.AddUint64(ops, 1)
        atomic.AddUint64(data, n)
    }
}

var dataMap = make(map[string][]byte)

func genData(id string, size uint) []byte {
    data, ok := dataMap[id]
    if !ok {
        id_bytes := []byte(id)
        data = make([]byte, size, size)
        for i := 0; i < int(size); i++ {
            data[i] = id_bytes[i%len(id_bytes)]
        }
        dataMap[id] = data
    }
    return data
}

func do_bench(
    c *client.HiberaAPI,
    duration float64,
    workers uint,
    keys []string,
    size uint,
    work func(key string) (uint64, error)) (float64, float64, error) {

    // Create an error channel.
    error_chan := make(chan error)

    // Our stats.
    // These are atomically updated by the workers.
    ops := uint64(0)
    data := uint64(0)

    // Start our clock.
    duration_ns := time.Duration(uint64(duration * float64(time.Second)))

    // Make sure all data exists.
    for _, id := range keys {
        genData(id, size)
    }

    // Start all the workers.
    for i := 0; i < int(workers); i += 1 {
        go do_worker(c, keys, error_chan, &ops, &data, work)
    }

    // Run the test.
    select {
    case <-time.After(duration_ns):
        break
    case err := <-error_chan:
        // Error during the test, return.
        return float64(0), float64(0), err
    }

    // Give an estimate of the throughput and latency.
    ops_per_second := float64(atomic.LoadUint64(&ops)) / duration
    throughput_mb := float64(atomic.LoadUint64(&data)) / duration / float64(1024*1024)

    return ops_per_second, throughput_mb, nil
}

func cli_write(c *client.HiberaAPI, duration float64, size uint, workers uint, keys []string) error {
    work_fn := func(key string) (uint64, error) {
        _, _, err := c.DataSet(key, core.NoRevision, genData(key, size))
        return uint64(size), err
    }
    ops_per_second, throughput_mb, err := do_bench(c, duration, workers, keys, size, work_fn)
    if err != nil {
        return err
    }
    fmt.Printf(
        "write:  duration=%.2fs, size=%d, workers=%d, ops=%.2f/s, rate=%.2fMb/s\n",
        duration,
        size,
        workers,
        ops_per_second,
        throughput_mb)
    return nil
}

func cli_read(c *client.HiberaAPI, duration float64, workers uint, keys []string) error {
    work_fn := func(key string) (uint64, error) {
        value, _, err := c.DataGet(key, core.NoRevision, 0)
        return uint64(len(value)), err
    }
    ops_per_second, throughput_mb, err := do_bench(c, duration, workers, keys, 0, work_fn)
    if err != nil {
        return err
    }
    fmt.Printf(
        "read:  duration=%.2fs, workers=%d, ops=%.2f/s, rate=%.2fMb/s\n",
        duration,
        workers,
        ops_per_second,
        throughput_mb)
    return nil
}

func cli_mixed(c *client.HiberaAPI, duration float64, size uint, workers uint, ratio float64, keys []string) error {
    work_fn := func(key string) (uint64, error) {
        if rand.Float64() < ratio {
            value, _, err := c.DataGet(key, core.NoRevision, 0)
            return uint64(len(value)), err
        }
        _, _, err := c.DataSet(key, core.NoRevision, genData(key, size))
        return uint64(size), err
    }
    ops_per_second, throughput_mb, err := do_bench(c, duration, workers, keys, size, work_fn)
    if err != nil {
        return err
    }
    fmt.Printf(
        "mixed:  duration=%.2fs, size=%d, workers=%d, ratio=%f ops=%.2f/s, rate=%.2fMb/s\n",
        duration,
        size,
        workers,
        ratio,
        ops_per_second,
        throughput_mb)
    return nil
}

func do_cli(command string, args []string) error {

    client := cli.Client()

    switch command {
    case "read":
        return cli_read(client, *duration, *workers, args)
    case "write":
        return cli_write(client, *duration, *size, *workers, args)
    case "mixed":
        return cli_mixed(client, *duration, *size, *workers, *ratio, args)
    }

    return nil
}

func main() {
    cli.Main(cliInfo, do_cli)
}
