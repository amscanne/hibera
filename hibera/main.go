package main

import (
    "bufio"
    "bytes"
    "flag"
    "fmt"
    "hibera/client"
    "hibera/core"
    "hibera/utils"
    "hibera/cli"
    "io"
    "io/ioutil"
    "os"
    "os/exec"
    "strings"
    "syscall"
    "errors"
)

var timeout = flag.Uint("timeout", 0, "Timeout (in ms) for acquiring a lock.")
var name = flag.String("name", "", "Name to use (other than machine address).")
var start = flag.String("start", "", "Script to start the given service.")
var stop = flag.String("stop", "", "Script to stop the given service.")
var limit = flag.Uint("limit", 0, "Limit for machines to run or simultanous locks.")
var output = flag.String("output", "", "Output file for sync.")
var data = flag.Bool("data", false, "Use the synchronization group data mapping.")
var path = flag.String("path", ".*", "The path for a given token (regular expression).")
var perms = flag.String("perms", "rwx", "Permissions (combination of r,w,x).")

var cliInfo = cli.Cli{
    "Hibera command line client.",
    map[string]cli.Command{
        "run" : cli.Command{
            "Run up-to <limit> process across the cluster.",
`      For example, to join a group while a script runs:
        run foo.bar script.sh

       To run something exclusively (locked):
        run foo.bar -limit 1 script.sh

       To add a timeout to the lock:
        run foo.bar -limit 1 -timeout 1000 script.sh

       To start N instances of myapp at all times:
        run foo.bar -limit N
                    -start '/etc/init.d/myapp start'
                    -stop '/etc/init.d/myapp stop'`,
            []string{"key"},
            []string{"name", "limit", "timeout", "start", "stop", "data"},
            true,
        },
        "members" : cli.Command{ "Show current members of the given group.",
            "",
            []string{"key"},
            []string{"name", "limit"},
            false,
        },
        "in" : cli.Command{
            "Show associated data (if joined with given group).",
            "",
            []string{"key"},
            []string{"name", "limit"},
            false,
        },
        "out" : cli.Command{ "Set associated data (if joined with given group).",
            "",
            []string{"key"},
            []string{"name", "limit"},
            false,
        },
        "list" : cli.Command{
            "List all keys.",
            "",
            []string{},
            []string{},
            false,
        },
        "get" : cli.Command{
            "Get the contents of the key.",
            "",
            []string{"key"},
            []string{},
            false,
        },
        "set" : cli.Command{ "Set the contents of the key.",
            "",
            []string{"key"},
            []string{},
            true,
        },
        "push" : cli.Command{
            "Stream up the contents of the key.",
            "",
            []string{"key"},
            []string{},
            false,
        },
        "pull" : cli.Command{ "Stream down the contents of the key.",
            "",
            []string{"key"},
            []string{},
            false,
        },
        "remove" : cli.Command{
            "Remove the given key.",
            "",
            []string{"key"},
            []string{},
            false,
        },
        "sync" : cli.Command{ "Synchronize a key.",
            "",
            []string{"key"},
            []string{"output", "timeout"},
            true,
        },
        "watch" : cli.Command{
            "Wait for an update of the key.",
            "",
            []string{"key"},
            []string{"timeout"},
            false,
        },
        "fire" : cli.Command{
            "Notify all waiters on a key.",
            "",
            []string{"key"},
            []string{},
            false,
        },
    },
    cli.Options,
}

func do_exec(command []string, input []byte) error {
    utils.Print("CLIENT", "Executing '%s'...", strings.Join(command, " "))
    cmd := exec.Command(command[0], command[1:]...)
    cmd.Stdin = bytes.NewBuffer(input)
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    return cmd.Run()
}

func cli_run(
    c *client.HiberaAPI,
    raw_key string,
    name string,
    limit uint,
    timeout uint,
    start []string,
    stop []string,
    cmd []string,
    dodata bool) error {

    var value []byte
    var proc *exec.Cmd
    key := cli.Key(raw_key)
    procchan := make(chan error)
    watchchan := make(chan error)

    oldindex := -1
    oldrev := core.ZeroRevision
    olddatarev := core.ZeroRevision

    defer c.SyncLeave(key, name)

    for {
        // Ensure that we're in the group.
        // This is necessary to run each time because the watch
        // may have fired due to an underlying node change. This
        // means that the new node may have lost all ephemeral
        // information and membership may be completely new.
        utils.Print("CLIENT", "JOINING key=%s name=%s", key.String(), name)
        newindex, newrev, err := c.SyncJoin(key, name, limit, 1)
        if err != nil {
            return err
        }
        utils.Print("CLIENT", "INDEX=%d REV=%d", newindex, newrev)

        if newindex != oldindex || (*newrev).Cmp(oldrev) != 0 {

            datachanged := false

            // Update the mapped data.
            if newindex >= 0 && dodata {

                // Opportunistic read of the data.
                // If there's nothing, we provide an empty set.
                var datarev core.Revision
                value, datarev, err = c.DataGet(
                    core.Key{
                        cli.Namespace(),
                        fmt.Sprintf("%s.%d", key.Key, newindex)},
                    core.ZeroRevision, 0)

                if err != nil {
                    datarev = core.ZeroRevision
                }
                if value == nil {
                    value = make([]byte, 0, 0)
                }

                // Save whether changed.
                datachanged = (newindex != oldindex || olddatarev != datarev)
                olddatarev = datarev

            } else {
                // No input.
                value = nil
            }
            if value != nil {
                utils.Print("CLIENT", "DATA len=%d", len(value))
            } else {
                utils.Print("CLIENT", "NODATA")
            }

            // If something has changed, stop the running process.
            if newindex < 0 || datachanged {
                if proc != nil {
                    utils.Print("CLIENT", "PROC-STOP")
                    // Kill this process on stop.
                    // Nothing can be done here to handle errors.
                    syscall.Kill(proc.Process.Pid, syscall.SIGTERM)
                    // Pull the exit status.
                    // NOTE: This doesn't count as a natural exit
                    // of the running process as per below. So we
                    // won't return but rather we will wait until
                    // we own this node again and we will restart
                    // the process.
                    <-procchan
                    proc = nil
                }
            }
            if newindex < 0 && (oldindex != newindex) {
                if stop != nil {
                    utils.Print("CLIENT", "EXEC-STOP")
                    do_exec(stop, value)
                }
            }

            // Start or stop appropriately.
            if newindex >= 0 && (newindex != oldindex) {
                if start != nil {
                    utils.Print("CLIENT", "EXEC-START")
                    do_exec(start, value)
                }
            }
            if newindex >= 0 {
                if proc == nil && cmd != nil {
                    // Ensure our process is running.
                    utils.Print("CLIENT", "PROC-START")
                    proc = exec.Command(cmd[0], cmd[1:]...)
                    proc.SysProcAttr = &syscall.SysProcAttr{
                        Chroot:    "",
                        Pdeathsig: syscall.SIGTERM}
                    if value != nil {
                        utils.Print("CLIENT", "PROC-DATA")
                        proc.Stdin = bytes.NewBuffer(value)
                    } else {
                        utils.Print("CLIENT", "PROC-STDIN")
                        proc.Stdin = os.Stdin
                    }
                    proc.Stdout = os.Stdout
                    proc.Stderr = os.Stderr
                    proc.Start()

                    // Start waiting for the process.
                    dowait := func() {
                        utils.Print("CLIENT", "PROC-WAIT")
                        procchan <- proc.Wait()
                        utils.Print("CLIENT", "PROC-EXIT")
                    }
                    go dowait()
                }
            }

            oldindex = newindex
            oldrev = newrev
        }

        // Wait for the next update.
        // NOTE: We always start a goroutine here.
        // If there is an active process, then we will
        // block for both the watch and the process. If
        // the process exits, then we will leave anyways.
        dowatch := func() {
            utils.Print("CLIENT", "WATCH-START")
            rev, err := c.EventWait(key, newrev, timeout)
            utils.Print("CLIENT", "WATCH-FIRED rev=%d", rev)
            watchchan <- err
        }
        go dowatch()

        select {
        case err := <-procchan:
            // The process has exited, so we do.
            // We still have an active watch, but that
            // will be killed and cleaned up shortly.
            return err

        case <-watchchan:
            break
        }
    }

    return nil
}

func cli_members(c *client.HiberaAPI, raw_key string, name string, limit uint) error {
    key := cli.Key(raw_key)
    index, members, _, err := c.SyncMembers(key, name, limit)
    if err != nil {
        return err
    }
    if len(members) > 0 {
        // Output all members.
        fmt.Printf("%d\n%s\n", index, strings.Join(members, "\n"))
    }
    return nil
}

func cli_in(c *client.HiberaAPI, raw_key string, name string, limit uint) (bool, error) {
    key := cli.Key(raw_key)
    index, _, _, err := c.SyncMembers(key, name, limit)
    if err != nil {
        return false, err
    }
    if index >= 0 {
        value, _, err := c.DataGet(
            cli.Key(fmt.Sprintf("%s.%d", raw_key, index)),
            core.ZeroRevision, 0)
        if err == nil {
            os.Stdout.Write(value)
        }
        return true, nil
    }
    return false, nil
}

func cli_out(c *client.HiberaAPI, raw_key string, value *string, name string, limit uint) (bool, error) {
    key := cli.Key(raw_key)
    index, _, _, err := c.SyncMembers(key, name, limit)
    if err != nil {
        return false, err
    }
    if index >= 0 {
        var err error
        if value == nil {
            // Fully read input.
            buf := new(bytes.Buffer)
            io.Copy(buf, os.Stdin)
            _, err = c.DataSet(
                cli.Key(fmt.Sprintf("%s.%d", raw_key, index)),
                core.ZeroRevision, buf.Bytes())
        } else {
            // Use the given string.
            _, err = c.DataSet(
                cli.Key(fmt.Sprintf("%s.%d", raw_key, index)),
                core.ZeroRevision, []byte(*value))
        }
        return true, err
    }
    return false, err
}

func cli_get(c *client.HiberaAPI, raw_key string) error {
    key := cli.Key(raw_key)
    value, _, err := c.DataGet(key, core.ZeroRevision, 0)
    if err != nil {
        return err
    }
    // Output the string.
    fmt.Printf("%s", value)
    return nil
}

func cli_push(c *client.HiberaAPI, raw_key string) error {
    key := cli.Key(raw_key)
    stdin := bufio.NewReader(os.Stdin)
    rev := core.ZeroRevision

    for {
        // Read the next line.
        line, err := stdin.ReadSlice('\n')
        if err == io.EOF {
            return nil
        } else if err != nil {
            return err
        }

        // Post the update.
        rev, err = c.DataSet(key, core.IncRevision(rev), line)
        if err != nil {
            return err
        }
    }
}

func cli_pull(c *client.HiberaAPI, raw_key string) error {
    key := cli.Key(raw_key)
    rev := core.ZeroRevision

    for {
        // Read the next entry.
        var err error
        var value []byte
        value, rev, err = c.DataGet(key, rev, 0)
        if err != nil {
            return err
        }

        // Write the data.
        _, err = os.Stdout.Write(value)
        if err == io.EOF {
            return nil
        } else if err != nil {
            return err
        }
    }
}

func cli_set(c *client.HiberaAPI, raw_key string, value *string) error {
    key := cli.Key(raw_key)
    var err error
    if value == nil {
        // Fully read input.
        buf := new(bytes.Buffer)
        io.Copy(buf, os.Stdin)
        _, err = c.DataSet(key, core.ZeroRevision, buf.Bytes())
    } else {
        // Use the given string.
        _, err = c.DataSet(key, core.ZeroRevision, []byte(*value))
    }
    return err
}

func cli_remove(c *client.HiberaAPI, raw_key string) error {
    key := cli.Key(raw_key)
    _, err := c.DataRemove(key, core.ZeroRevision)
    return err
}

func cli_list(c *client.HiberaAPI) error {
    items, err := c.DataList(cli.Namespace())
    if err != nil {
        return err
    }
    for _, item := range items {
        fmt.Printf("%s\n", item)
    }
    return nil
}

func cli_sync(c *client.HiberaAPI, raw_key string, output string, cmd []string, timeout uint) error {
    key := cli.Key(raw_key)
    rev := core.ZeroRevision
    var value []byte
    var err error

    for {
        // Get the current value.
        utils.Print("CLIENT", "GET rev=%d", rev)
        value, rev, err = c.DataGet(key, rev, timeout)
        utils.Print("CLIENT", "GOT rev=%d", rev)
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

        if cmd != nil {
            // Execute with the given output.
            do_exec(cmd, value)
        }
    }

    return nil
}

func cli_watch(c *client.HiberaAPI, raw_key string, timeout uint) error {
    key := cli.Key(raw_key)
    _, err := c.EventWait(key, core.ZeroRevision, timeout)
    return err
}

func cli_fire(c *client.HiberaAPI, raw_key string) error {
    key := cli.Key(raw_key)
    _, err := c.EventFire(key, core.ZeroRevision)
    return err
}

func make_command(args ...string) []string {
    // Skip empty commands.
    if len(args) == 0 || (len(args) == 1 && len(args[0]) == 0) {
        return nil
    }

    // Build up the appropriate shell execution.
    var sh_arg string
    if len(args) == 1 {
        sh_arg = args[0]
    } else {
        formatted_args := make([]string, len(args), len(args))
        for i, arg := range args {
            formatted_args[i] = fmt.Sprintf("'%s'", strings.Replace(arg, "'", "\\'", -1))
        }
        sh_arg = strings.Join(formatted_args, " ")
    }
    cmd := make([]string, 3, 3)
    cmd[0] = "sh"
    cmd[1] = "-c"
    cmd[2] = sh_arg
    return cmd
}

func do_cli(command string, args []string) error {

    client := cli.Client()

    switch command {
    case "run":
        cmd := make_command(args...)
        start_cmd := make_command(*start)
        stop_cmd := make_command(*stop)
        return cli_run(client, args[0], *name, *limit, *timeout, start_cmd, stop_cmd, cmd, *data)
    case "members":
        return cli_members(client, args[0], *name, *limit)
    case "in":
        in, err := cli_in(client, args[0], *name, *limit)
        if err == nil && !in {
            return errors.New("")
        }
        return err
    case "out":
        var in bool
        var err error
        if len(args) > 0 {
            value := strings.Join(args, " ")
            in, err = cli_out(client, args[0], &value, *name, *limit)
        } else {
            in, err = cli_out(client, args[0], nil, *name, *limit)
        }
        if err == nil && !in {
            return errors.New("")
        }
        return err
    case "get":
        return cli_get(client, args[0])
    case "push":
        return cli_push(client, args[0])
    case "pull":
        return cli_pull(client, args[0])
    case "set":
        if len(args) > 0 {
            value := strings.Join(args, " ")
            return cli_set(client, args[0], &value)
        }
        return cli_set(client, args[0], nil)
    case "remove":
        return cli_remove(client, args[0])
    case "list":
        return cli_list(client)
    case "sync":
        cmd := make_command(flag.Args()...)
        return cli_sync(client, args[0], *output, cmd, *timeout)
    case "watch":
        return cli_watch(client, args[0], *timeout)
    case "fire":
        return cli_fire(client, args[0])
    }

    return nil
}

func main() {
    cli.Main(cliInfo, do_cli)
}
