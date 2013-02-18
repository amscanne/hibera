package main

import (
	"flag"
	"fmt"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"log"
	"strings"
        "syscall"
	"hibera/client"
)

var api = flag.String("api", "", "API address.")
var cmd = flag.String("exec", "", "Script to execute in context.")
var timeout = flag.Uint("timeout", 0, "Timeout (in ms) for acquiring a lock.")
var name = flag.String("name", "", "Name to use (other than machine address).")
var count = flag.Uint("count", 1, "Count for services run via the run command.")
var start = flag.String("start", "", "Script to start the given service.")
var stop = flag.String("stop", "", "Script to stop the given service.")
var limit = flag.Uint("limit", 1, "Limit for machines to run or simultanous locks.")
var output = flag.String("output", "", "Output file for sync.")

var usagemsg = `usage: hibera <command> <key> [options]

options for all commands:
    [-api <address:port>]        --- The API address.

commands:

    info                         --- Show cluster info.

    lock <key>                   --- Run a process while holding
         [-name <name>]              the given lock.
         [-limit <number>]
         [-timeout <timeout>]
         [-exec <run-script>]
         [<run-script> ...]

    owners <key>                 --- Show current lock owners.
         [-name <name>]

    join <key>                   --- Run a process while joined to
         [-name <name>]              the given group.
         [-exec <run-script>]
         [<run-script> ...]

    run <key>                    --- Conditionally run up-to <count>
        [-count <count>]             process across the cluster.
        [-start <start-script>]
        [-stop <stop-script>]
        [-exec <run-script>]
        [-name <name>]
        [<run-script> ...]

    members <key>                --- Show current members of the
            [-limit <number>]        given group. NOTE: members that
            [-name <name>]           match this node's name will be
                                     prefixed with a '*'.

    get <key>                    --- Get the contents of the key.

    set <key> [value]            --- Set the contents of the key.

    rm <key>                     --- Remove the given key.

    ls                           --- List all keys.

    clear                        --- Clear all data.

    sync <key>                   --- Synchronize a key, either as
         [-output <file>]            stdin to a named script to
         [-exec <run-script>]        directly to the named file.

    watch <key>                  --- Wait for an update of the key.

    fire <key>                   --- Notify all waiters on the key.
`

func do_exec(command string, input []byte) error {
	cmd := exec.Command("sh", "-c", command)
	cmd.Stdin = bytes.NewBuffer(input)
	return cmd.Run()
}

func cli_info(c *client.HiberaClient) error {
	_, err := c.Info(0)
	if err != nil {
		return err
	}
	return nil
}

func cli_lock(c *client.HiberaClient, key string, name string, cmd string, timeout uint, limit uint) error {
	_, err := c.Lock(key, timeout, name, limit)
	if err != nil {
		return err
	}
	defer c.Unlock(key)
	return do_exec(cmd, nil)
}

func cli_owners(c *client.HiberaClient, key string, name string) error {
	owners, _, err := c.Owners(key, name)
	if err != nil {
		return err
	}
        if len(owners) > 0 {
	    // Output all owners.
	    fmt.Printf("%s\n", strings.Join(owners, "\n"))
        }
	return nil
}

func cli_join(c *client.HiberaClient, key string, name string, cmd string) error {
	_, err := c.Join(key, name)
	if err != nil {
		return err
	}
	defer c.Leave(key, name)
	return do_exec(cmd, nil)
}

func cli_run(c *client.HiberaClient, key string, name string, count uint, start string, stop string, cmd string) error {
	_, err := c.Join(key, name)
	if err != nil {
		return err
	}
	defer c.Leave(key, name)

        var proc *exec.Cmd
        wasactive := false

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
		if active && !wasactive {
			do_exec(start, nil)
                        if proc == nil && cmd != "" {
                            // Start this process on startup.
                            proc = exec.Command("sh", "-c", cmd)
                            proc.Start()
                        }
                        wasactive = true;

		} else if !active && wasactive {
                        if proc != nil {
                            // Kill this process on stop.
                            // Nothing can be done here to handle errors.
                            syscall.Kill(proc.Process.Pid, syscall.SIGTERM)
                            proc.Wait()
                            proc = nil
                        }
			do_exec(stop, nil)
                        wasactive = false;
		}

		// Wait for the next update.
		rev, err = c.Watch(key, rev, 0)
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
        if len(members) > 0 {
	    // Output all members.
	    fmt.Printf("%s\n", strings.Join(members, "\n"))
        }
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

func cli_set(c *client.HiberaClient, key string, value *string) error {
	buf := new(bytes.Buffer)
        var err error
        if value == nil {
	    // Fully read input.
	    io.Copy(buf, os.Stdin)
	    _, err = c.Set(key, buf.Bytes(), 0)
        } else {
            // Use the given string.
	    _, err = c.Set(key, []byte(*value), 0)
        }
	return err
}

func cli_rm(c *client.HiberaClient, key string) error {
	_, err := c.Remove(key, 0)
	return err
}

func cli_ls(c *client.HiberaClient) error {
	items, err := c.List()
	if err != nil {
		return err
	}
	for _, item := range items {
		fmt.Printf("%s\n", item)
	}
	return nil
}

func cli_clear(c *client.HiberaClient) error {
	return c.Clear()
}

func cli_sync(c *client.HiberaClient, key string, output string, cmd string) error {
	for {
		value, rev, err := c.Get(key)
		if err != nil {
			return err
		}

		if output != "" {
			// Copy the output to the file.
			err = ioutil.WriteFile(output, value, 0644)
			if err != nil {
				return err
			}
		}

		if cmd != "" {
			// Execute with the given output.
			do_exec(cmd, value)
		}

		// Wait for the next update.
		rev, err = c.Watch(key, rev, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

func cli_watch(c *client.HiberaClient, key string) error {
	_, err := c.Watch(key, 0, 0)
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
		command == "ls" ||
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
                if flag.NArg() > 0 {
                    script := strings.Join(flag.Args(), " ")
		    err = cli_lock(c, key, *name, script, *timeout, *limit)
                } else {
		    err = cli_lock(c, key, *name, *cmd, *timeout, *limit)
                }
		break
	case "owners":
		err = cli_owners(c, key, *name)
		break
	case "join":
                if flag.NArg() > 0 {
                    script := strings.Join(flag.Args(), " ")
		    err = cli_join(c, key, *name, script)
                } else {
		    err = cli_join(c, key, *name, *cmd)
                }
		break
	case "run":
                if flag.NArg() > 0 {
                    script := strings.Join(flag.Args(), " ")
		    err = cli_run(c, key, *name, *count, *start, *stop, script)
                } else {
		    err = cli_run(c, key, *name, *count, *start, *stop, *cmd)
                }
		break
	case "members":
		err = cli_members(c, key, *name, *limit)
		break
	case "get":
		err = cli_get(c, key)
		break
	case "set":
                if flag.NArg() > 0 {
                    value := strings.Join(flag.Args(), " ")
		    err = cli_set(c, key, &value)
                } else {
		    err = cli_set(c, key, nil)
                }
		break
	case "rm":
		err = cli_rm(c, key)
		break
	case "ls":
		err = cli_ls(c)
		break
	case "clear":
		err = cli_clear(c)
		break
	case "sync":
		err = cli_sync(c, key, *output, *cmd)
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
	}
}
