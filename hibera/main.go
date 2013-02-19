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
var timeout = flag.Uint("timeout", 0, "Timeout (in ms) for acquiring a lock.")
var name = flag.String("name", "", "Name to use (other than machine address).")
var start = flag.String("start", "", "Script to start the given service.")
var stop = flag.String("stop", "", "Script to stop the given service.")
var limit = flag.Uint("limit", 1, "Limit for machines to run or simultanous locks.")
var output = flag.String("output", "", "Output file for sync.")
var delay = flag.Uint("delay", 1000, "Delay and retry failed requests.")
var data = flag.Bool("data", false, "Use the synchronization group data mapping.")

var usagemsg = `usage: hibera <command> <key> [options]

options for all commands:

    [-api <address:port>]        --- The API address.

    [-delay <delay>]             --- Delay in milliseconds to

cluster commands:

    info                         --- Show cluster info.

synchronization commands:

    lock <key>                   --- Run a process while holding
         [-name <name>]              the given lock.
         [-limit <number>]
         [-timeout <timeout>]
         [-data]
         [<run-script> ...]

    join <key>                   --- Run a process while joined to
         [-name <name>]              the given synchronization group.
         [-data]
         [<run-script> ...]

    run <key>                    --- Conditionally run up-to <limit>
        [-name <name>]               process across the cluster.
        [-limit <limit>]
        [-start <start-script>]
        [-stop <stop-script>]
        [-data]
        [<run-script> ...]

    members <key>                --- Show current members of the
            [-name <name>]           given group. The first line will
            [-limit <number>]        be this connections index in the
                                     list.

    in <key>                     --- If this process is part of
         [-name <name>]              the synchronization group named,
         [-limit <limit>]            fetch the associated data. If
                                     not in the set, exit with in error.

data commands:

    get <key>                    --- Get the contents of the key.

    set <key> [value]            --- Set the contents of the key.

    rm <key>                     --- Remove the given key.

    ls                           --- List all keys.

    clear                        --- Clear all data.

    sync <key>                   --- Synchronize a key, either as
         [-output <file>]            stdin to a named script to
         [<run-script> ...]          directly to the named file.

event commands:

    watch <key>                  --- Wait for an update of the key.

    fire <key>                   --- Notify all waiters on the key.
`

func do_exec(command string, input []byte) error {
	cmd := exec.Command("sh", "-c", command)
	cmd.Stdin = bytes.NewBuffer(input)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func cli_info(c *client.HiberaAPI) error {
	_, err := c.Info(0)
	if err != nil {
		return err
	}
	return nil
}

func cli_lock(c *client.HiberaAPI, key string, name string, limit uint, timeout uint, cmd string, dodata bool) error {
	index, _, err := c.Join(key, name, limit, timeout)
	if err != nil {
		return err
	}
	defer c.Leave(key, name)
	var value []byte
	if dodata {
		value, _, err = c.Get(fmt.Sprintf("%s.%d", key, index))
		if err != nil {
			return err
		}
	}
	return do_exec(cmd, value)
}

func cli_join(c *client.HiberaAPI, key string, name string, cmd string, dodata bool) error {
	index, _, err := c.Join(key, name, 0, 0)
	if err != nil {
		return err
	}
	defer c.Leave(key, name)
	var value []byte
	if dodata {
		value, _, err = c.Get(fmt.Sprintf("%s.%d", key, index))
		if err != nil {
			return err
		}
	}
	return do_exec(cmd, value)
}

func cli_run(c *client.HiberaAPI, key string, name string, limit uint, start string, stop string, cmd string, dodata bool) error {
	oldindex, _, err := c.Join(key, name, 0, 0)
	if err != nil {
		return err
	}
	defer c.Leave(key, name)

	var proc *exec.Cmd
	var value []byte
	for {
		// List the current members.
		newindex, _, rev, err := c.Members(key, name, limit)
		if err != nil {
			return err
		}

		// Update the mapped data.
		if newindex >= 0 && dodata {
			// Opportunistic.
			value, _, err = c.Get(fmt.Sprintf("%s.%d", key, newindex))
			if err != nil {
				value = nil
			}
		} else {
			// No input.
			value = nil
		}

		// If something has changed, stop the running process.
		if newindex < 0 || (newindex != oldindex && dodata) {
			if proc != nil {
				// Kill this process on stop.
				// Nothing can be done here to handle errors.
				syscall.Kill(proc.Process.Pid, syscall.SIGTERM)
				proc.Wait()
				proc = nil
			}
		}
		if newindex < 0 && (oldindex != newindex) {
			if stop != "" {
				do_exec(stop, value)
			}
		}

		// Start or stop appropriately.
		if newindex >= 0 && (newindex != oldindex) {
			if start != "" {
				do_exec(start, value)
			}
		}
		if newindex >= 0 || (newindex != oldindex && dodata) {
			if proc == nil && cmd != "" {
				// Ensure our process is running.
				proc = exec.Command("sh", "-c", cmd)
				proc.Stdin = bytes.NewBuffer(value)
				proc.Stdout = os.Stdout
				proc.Stderr = os.Stderr
				proc.Start()
			}
		}

		// Wait for the next update.
		rev, err = c.Wait(key, rev, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func cli_members(c *client.HiberaAPI, key string, name string, limit uint) error {
	index, members, _, err := c.Members(key, name, limit)
	if err != nil {
		return err
	}
	if len(members) > 0 {
		// Output all members.
		fmt.Printf("%d\n%s\n", index, strings.Join(members, "\n"))
	}
	return nil
}

func cli_in(c *client.HiberaAPI, key string, name string, limit uint) (bool, error) {
	index, _, _, err := c.Members(key, name, limit)
	if err != nil {
		return false, err
	}
	if index >= 0 {
		value, _, err := c.Get(fmt.Sprintf("%s.%d", key, index))
		if err == nil {
			os.Stdout.Write(value)
		}
		return true, nil
	}
	return false, nil
}

func cli_get(c *client.HiberaAPI, key string) error {
	value, _, err := c.Get(key)
	if err != nil {
		return err
	}
	// Output the string.
	fmt.Printf("%s", value)
	return nil
}

func cli_set(c *client.HiberaAPI, key string, value *string) error {
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

func cli_rm(c *client.HiberaAPI, key string) error {
	_, err := c.Remove(key, 0)
	return err
}

func cli_ls(c *client.HiberaAPI) error {
	items, err := c.List()
	if err != nil {
		return err
	}
	for _, item := range items {
		fmt.Printf("%s\n", item)
	}
	return nil
}

func cli_clear(c *client.HiberaAPI) error {
	return c.Clear()
}

func cli_sync(c *client.HiberaAPI, key string, output string, cmd string) error {
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
		rev, err = c.Wait(key, rev, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

func cli_watch(c *client.HiberaAPI, key string) error {
	_, err := c.Wait(key, 0, 0)
	return err
}

func cli_fire(c *client.HiberaAPI, key string) error {
	_, err := c.Fire(key, 0)
	return err
}

func usage() {
	fmt.Printf("%s\n", usagemsg)
	os.Exit(1)
}

func main() {
	var err error

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
	c := client.NewHiberaClient(*api, *delay)
	if c == nil {
		return
	}

	// Do our stuff.
	switch command {
	case "info":
		err = cli_info(c)
	case "lock":
		script := strings.Join(flag.Args(), " ")
		err = cli_lock(c, key, *name, *timeout, *limit, script, *data)
		break
	case "join":
		script := strings.Join(flag.Args(), " ")
		err = cli_join(c, key, *name, script, *data)
		break
	case "run":
		script := strings.Join(flag.Args(), " ")
		err = cli_run(c, key, *name, *limit, *start, *stop, script, *data)
		break
	case "members":
		err = cli_members(c, key, *name, *limit)
		break
	case "in":
		var in bool
		in, err = cli_in(c, key, *name, *limit)
		if err == nil && !in {
			os.Exit(1)
		}
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
		script := strings.Join(flag.Args(), " ")
		err = cli_sync(c, key, *output, script)
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
