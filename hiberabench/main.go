package main

import (
    "fmt"
    "time"
    "math/rand"
    "hibera/client"
    "hibera/cli"
    "hibera/core"
)

var size = cli.Flags.Uint("size", 1024, "Data size.")
var workers = cli.Flags.Uint("workers", 512, "Number of workers.")
var ratio = cli.Flags.Float64("ratio", 0.5, "Write ratio for mixed workload.")
var duration = cli.Flags.Float64("duration", 10.0, "Duration of test (in seconds).")

var cliInfo = cli.Cli{
    "Hibera cluster benchmarking tool.",
    map[string]cli.Command{
        "read" : cli.Command{
            "Run a read benchmark (requires write benchmark).",
            "",
            []string{"key"},
            []string{"workers", "duration"},
            true,
        },
        "write" : cli.Command{
            "Run a write benchmark.",
            "",
            []string{"key"},
            []string{"size", "workers", "duration"},
            true,
        },
        "mixed" : cli.Command{
            "Run a mixed benchmark.",
            "",
            []string{"key"},
            []string{"size", "workers", "ratio", "duration"},
            true,
        },
    },
    cli.Options,
}

type workResult struct {
    err error
    ops uint64
    data uint64
    waited time.Duration
}

func do_worker(
    c *client.HiberaAPI,
    end_time time.Time,
    keys_chan chan string,
    result_chan chan *workResult,
    work func(key string) (uint64, error)) {

    ops := uint64(0)
    data := uint64(0)
    waited := time.Duration(0)

    for {
        // Grab a key.
        key := <-keys_chan

        // Check our time.
        start := time.Now()
        if start.After(end_time) {
            // This operation does *not* have it's
            // information tracked (wait time, data, etc.).
            // It finished after the deadline, so in fairness
            // it doesn't count either way (good or bad).
            break
        }

        // Do the work.
        n, err := work(key)
        if err != nil {
            result_chan <- &workResult{err, ops, data, waited}
        }

        // Add our stats.
        end := time.Now()
        ops += 1
        data += n
        waited += end.Sub(start)

        // Put the key back.
        keys_chan <- key
    }

    // Send the final results.
    result_chan <- &workResult{nil, ops, data, waited}
}

func do_bench(
    c *client.HiberaAPI,
    duration float64,
    workers uint,
    keys []string,
    work func(key string) (uint64, error)) (float64, float64, error) {

    result_chan := make(chan *workResult, workers)
    key_chan := make(chan string, len(keys) * int(workers))
    ops := uint64(0)
    data := uint64(0)
    waited := time.Duration(0)
    end_time := time.Now().Add(time.Duration(uint64(duration * float64(time.Second))))

    // Fill the key channel.
    for i := 0; i < int(workers); i += 1 {
        for j := 0; j < len(keys); j += 1 {
            key_chan <- keys[j]
        }
    }

    // Start all the workers.
    for i := 0; i < int(workers); i += 1 {
        go do_worker(c, end_time, key_chan, result_chan, work)
    }

    // Wait for all workers to finish.
    for i := 0; i < int(workers); i += 1 {
        result := <-result_chan
        if result.err != nil {
            return float64(0), float64(0), result.err
        }
        ops += result.ops
        data += result.data
        waited += result.waited
    }

    // Give an estimate of the throughput and latency.
    ops_per_second := float64(ops) / duration
    throughput_mb := float64(data) / duration / float64(1024*1024)

    return ops_per_second, throughput_mb, nil
}

func cli_write(c *client.HiberaAPI, duration float64, size uint, workers uint, keys []string) error {
    data_seed := 0
    work_fn := func(key string) (uint64, error) {
        data := make([]byte, size, size)
        for i := 0; i < int(size); i++ {
            data_seed += 1
            data[i] = byte(data_seed % 256)
        }
        _, err := c.DataSet(key, core.NoRevision, data)
        return uint64(size), err
    }
    ops_per_second, throughput_mb, err := do_bench(c, duration, workers, keys, work_fn)
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
    ops_per_second, throughput_mb, err := do_bench(c, duration, workers, keys, work_fn)
    if err != nil {
        return err
    }
    fmt.Printf(
        "write:  duration=%.2fs, workers=%d, ops=%.2f/s, rate=%.2fMb/s\n",
        duration,
        workers,
        ops_per_second,
        throughput_mb)
    return nil
}

func cli_mixed(c *client.HiberaAPI, duration float64, size uint, workers uint, ratio float64, keys []string) error {
    data_seed := 0
    work_fn := func(key string) (uint64, error) {
        if rand.Float64() < ratio {
            value, _, err := c.DataGet(key, core.NoRevision, 0)
            return uint64(len(value)), err
        }
        data := make([]byte, size, size)
        for i := 0; i < int(size); i++ {
            data_seed += 1
            data[i] = byte(data_seed % 256)
        }
        _, err := c.DataSet(key, core.NoRevision, data)
        return uint64(size), err
    }
    ops_per_second, throughput_mb, err := do_bench(c, duration, workers, keys, work_fn)
    if err != nil {
        return err
    }
    fmt.Printf(
        "write:  duration=%.2fs, size=%d, workers=%d, ratio=%f ops=%.2f/s, rate=%.2fMb/s\n",
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
