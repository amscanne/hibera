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

var api = flag.String("api", "", "API address.")
var exec = flag.String("exec", "", "Script to execute in context.")
var timeout = flag.Uint("timeout", 0, "Timeout (in ms) for acquiring a lock.")
var name = flag.String("name", "", "Name to use (other than machine address).")
var count = flag.Uint("count", 1, "Count for services run via the run command.")
var start = flag.String("start", "", "Script to start the given service.")
var stop = flag.String("stop", "", "Script to stop the given service.")
var limit = flag.Uint("limit", 0, "Limit for machines to return.")
var output = flag.String("output", "", "Output file for sync.")

var usagemsg = `usage: hibera <command> <key> [options]

options for all commands:
    [-api <address:port>]        --- The API address.

commands:

    info                         --- Show cluster info.

    lock <key>                   --- Run a process while holding
         [-name <name>]              the given lock.
         [-exec <run-script>]
         [-timeout <timeout>]

    join <key>                   --- Run a process while joined to
         [-name <name>]              the given group.
         [-exec <run-script>]

    run <key>                    --- Conditionally run up-to <count>
        [-count <count>]             process across the cluster.
        [-start <start-script>]
        [-stop <stop-script>]
        [-name <name>]

    members <key>                --- Show current members of the
            [-limit <number>]        given group. NOTE: members that
            [-name <name>]           match this node's name will be
                                     prefixed with a '*'.

    get <key>                    --- Get the contents of the key.

    set <key>                    --- Set the contents of the key.

    rm <key>                     --- Remove the given key.

    list                         --- List all keys.

    clear                        --- Clear all data.

    sync <key>                   --- Synchronize a key, either as
         [-output <file>]            stdin to a named script to
         [-exec <run-script>]        directly to the named file.

    watch <key>                  --- Wait for an update of the key.

    fire <key>                   --- Notify all waiters on the key.
`

func do_exec(command string, input string) error {
	return nil
}

func cli_info(c *client.HiberaClient) error {
	_, err := c.Info(0)
	if err != nil {
		return err
	}
	return nil
}

func cli_lock(c *client.HiberaClient, key string, name string, exec string, timeout uint) error {
	_, err := c.Lock(key, timeout, name)
	if err != nil {
		return err
	}
	defer c.Unlock(key)
	return do_exec(exec, "")
}

func cli_join(c *client.HiberaClient, key string, name string, exec string) error {
	_, err := c.Join(key, name)
	if err != nil {
		return err
	}
	defer c.Leave(key, name)
	return do_exec(exec, "")
}

func cli_run(c *client.HiberaClient, key string, name string, count uint, start string, stop string) error {
	_, err := c.Join(key, name)
	if err != nil {
		return err
	}
	defer c.Leave(key, name)

	for {
		// List the current members.
		members, rev, err := c.Members(key, name, count)
		if err != nil {
			return err
		}

		// Check if we're in the list.
		active := false
		for _, member := range members {
			if member[0] == '*' {
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

func cli_members(c *client.HiberaClient, key string, name string, limit uint) error {
	members, _, err := c.Members(key, name, limit)
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

func cli_rm(c *client.HiberaClient, key string) error {
	_, err := c.Remove(key, 0)
	return err
}

func cli_list(c *client.HiberaClient) error {
	items, err := c.List()
	if err != nil {
		return err
	}
	for _, item := range items {
		fmt.Printf("%s\n", string(item))
	}
	return nil
}

func cli_clear(c *client.HiberaClient) error {
	return c.Clear()
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
	_, err := c.Fire(key, 0)
	return err
}

func usage() {
	fmt.Printf("%s\n", usagemsg)
	os.Exit(1)
}

func main() {
	// Pull out our arguments.
	if len(os.Args) == 1 {
		usage()
	}
	command := os.Args[1]
	os.Args = os.Args[1:]

	key := ""
	if command == "info" ||
           command == "list" ||
           command == "clear" {
	} else {
		if len(os.Args) == 1 {
			usage()
		}
		key = os.Args[1]
		os.Args = os.Args[1:]
	}
	flag.Parse()

	// Create our client.
	c := client.NewHiberaClient(*api)
	if c == nil {
		return
	}

	// Do our stuff.
	var err error
	switch command {
        case "info":
                err = cli_info(c)
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
		err = cli_members(c, key, *name, *limit)
		break
	case "get":
		err = cli_get(c, key)
		break
	case "set":
		err = cli_set(c, key)
		break
	case "rm":
		err = cli_rm(c, key)
		break
	case "list":
		err = cli_list(c)
		break
	case "clear":
		err = cli_clear(c)
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
	default:
		usage()
	}

	if err != nil {
		log.Fatal("Error: ", err)
		os.Exit(1)
	}
}
