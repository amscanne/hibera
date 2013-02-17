package main

import (
    "flag"
    "fmt"
    "bytes"
    "io"
    "os"
    "log"
    "strings"
    "hibera/client"
)

var url = flag.String("url", "http://localhost:2033/", "API address.")
var exec = flag.String("exec", "", "Script to execute in context.")
var timeout = flag.Uint("timeout", 0, "Timeout for acquiring a lock.")
var name = flag.String("name", "", "Name to use (other than machine address).")
var count = flag.Uint("count", 1, "Count for services run via the run command.")
var start = flag.String("start", "", "Script to start the given service.")
var stop = flag.String("stop", "", "Script to stop the given service.")
var limit = flag.Uint("limit", 0, "Limit for machines to return.")
var output = flag.String("output", "", "Output file for sync.")

var usage = `usage: hibera <command> <key> [options]
commands:

    [-name <name>]            --- Run a process while holding
    [-exec <run-script>]          the given lock.
    [-timeout <timeout>]
    lock <key>

    [-name <name>]            --- Run a process while joined to
    [-exec <run-script>]          the given group.
    join <key>

    [-count <count>]          --- Conditionally run up-to <count>
    [-start <start-script>]       process across the cluster. 
    [-stop <stop-script>]
    [-name <name>]
    run <key>

    [-limit <number>]         --- Show current members of the
    members <key>                 given group.

    get <key>                 --- Get the contents of the key.

    set <key>                 --- Set the contents of the key.

    [-output <file>]          --- Synchronize a key, either as
    [-exec <run-script>]          stdin to a named script to
    sync <key>                    directly to the named file.

    watch <key>               --- Wait for an update of the key.

    fire <key>                --- Notify all waiters on the key.
`

func do_exec(command string, input string) error {
    return nil
}

func cli_lock(c *client.HiberaClient, key string, name string, exec string, timeout uint) error {
    err := c.Lock(key, timeout, name)
    if err != nil {
        return err
    }
    defer c.Release(key)
    return do_exec(exec, "")
}

func cli_join(c *client.HiberaClient, key string, name string, exec string) error {
    err := c.Join(key, name)
    if err != nil {
        return err
    }
    defer c.Leave(key, name)
    return do_exec(exec, "")
}

func cli_run(c *client.HiberaClient, key string, name string, count uint, start string, stop string) error {
    err := c.Join(key, name)
    if err != nil {
        return err
    }
    defer c.Leave(key, name)

    for {
        // List the current members.
        members, rev, err := c.Members(key, count)
        if err != nil {
            return err
        }

        // Check if we're in the list.
        active := false
        for _, member := range members {
            if member == name {
                active = true
                break
            }
        }

        // Start or stop appropriately.
        if active {
            do_exec(start, "")
        } else {
            do_exec(stop, "")
        }

        // Wait for the next update.
        rev, err = c.Watch(key, rev)
        if err != nil {
            return err
        }
    }
    return nil
}

func cli_members(c *client.HiberaClient, key string, limit uint) error {
    members, _, err := c.Members(key, limit)
    if err != nil {
        return err
    }
    // Output all members.
    fmt.Printf("%s\n", strings.Join(members, "\n"))
    return nil
}

func cli_get(c *client.HiberaClient, key string) error {
    value, _, err := c.Get(key)
    if err != nil {
        return err
    }
    // Output the string.
    fmt.Printf("%s", value)
    return nil
}

func cli_set(c *client.HiberaClient, key string) error {
    buf := bytes.NewBuffer(nil)
    // Fully read input.
    io.Copy(buf, os.Stdin)
    value := string(buf.Bytes())
    _, err := c.Set(key, value, 0)
    return err
}

func cli_sync(c *client.HiberaClient, key string, output string, exec string) error {
    for {
        value, rev, err := c.Get(key)
        if err != nil {
            return err
        }

        if output != "" {
            // Copy the output to the file.
            f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
            if err != nil {
                f.Close()
                return err
            }
            io.WriteString(f, value)
            f.Close()
        }

        if exec != "" {
            // Execute with the given output.
            do_exec(exec, value)
        }

        // Wait for the next update.
        rev, err = c.Watch(key, rev)
        if err != nil {
            return err
        }
    }

    return nil
}

func cli_watch(c *client.HiberaClient, key string) error {
    _, err := c.Watch(key, 0)
    return err
}

func cli_fire(c *client.HiberaClient, key string) error {
    return c.Fire(key, 0)
}

func main() {
	flag.Parse()

        if flag.NArg() != 2 {
            fmt.Printf("%s\n", usage)
            return
        }

        // Pull out our arguments.
        command := flag.Arg(0)
        key := flag.Arg(1)

        // Create our client.
        c := client.NewHiberaClient(*url)
        if c == nil {
            return
        }

        // Do our stuff.
        var err error
        switch command {
            case "lock":
                err = cli_lock(c, key, *name, *exec, *timeout)
                break
            case "join":
                err = cli_join(c, key, *name, *exec)
                break
            case "run":
                err = cli_run(c, key, *name, *count, *start, *stop)
                break
            case "members":
                err = cli_members(c, key, *limit)
                break
            case "get":
                err = cli_get(c, key)
                break
            case "set":
                err = cli_set(c, key)
                break
            case "sync":
                err = cli_sync(c, key, *output, *exec)
                break
            case "watch":
                err = cli_watch(c, key)
                break
            case "fire":
                err = cli_fire(c, key)
                break
        }

    if err != nil {
        log.Fatal(fmt.Sprintf("%s", err))
        os.Exit(1)
    }
}
