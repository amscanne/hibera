package main

import (
    "bufio"
    "bytes"
    "fmt"
    "hibera/cli"
    "hibera/client"
    "hibera/core"
    "hibera/utils"
    "io"
    "io/ioutil"
    "os"
    "os/exec"
    "strconv"
    "strings"
    "syscall"
)

var timeout = cli.Flags.Uint("timeout", 0, "Timeout (in ms) for acquiring a lock.")
var start = cli.Flags.String("start", "", "Script to start the given service.")
var stop = cli.Flags.String("stop", "", "Script to stop the given service.")
var limit = cli.Flags.Uint("limit", 0, "Limit for machines to run or simultanous locks.")
var output = cli.Flags.String("output", "", "Output file for sync.")
var data = cli.Flags.String("data", "", "Synchronization group data.")
var path = cli.Flags.String("path", ".*", "The path for a given token (regular expression).")
var perms = cli.Flags.String("perms", "rwx", "Permissions (combination of r,w,x).")
var rev = cli.Flags.String("rev", "0", "Revision (for data, watching, etc.).")

var cliInfo = cli.Cli{
    "Hibera command line client.",
    map[string]cli.Command{
        "run": cli.Command{
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
            []string{"data", "limit", "timeout", "start", "stop"},
            true,
        },
        "members": cli.Command{"Show current members of the given group.",
            "",
            []string{"key"},
            []string{"data", "limit"},
            false,
        },
        "list": cli.Command{
            "List all keys.",
            "",
            []string{},
            []string{},
            false,
        },
        "get": cli.Command{
            "Get the contents of the key.",
            "",
            []string{"key"},
            []string{"rev"},
            false,
        },
        "set": cli.Command{"Set the contents of the key.",
            "",
            []string{"key"},
            []string{"rev"},
            true,
        },
        "push": cli.Command{
            "Stream up the contents of the key.",
            "",
            []string{"key"},
            []string{},
            false,
        },
        "pull": cli.Command{"Stream down the contents of the key.",
            "",
            []string{"key"},
            []string{},
            false,
        },
        "remove": cli.Command{
            "Remove the given key.",
            "",
            []string{"key"},
            []string{"rev"},
            false,
        },
        "sync": cli.Command{"Synchronize a key.",
            "",
            []string{"key"},
            []string{"output", "timeout"},
            true,
        },
        "watch": cli.Command{
            "Wait for an update of the key.",
            "",
            []string{"key"},
            []string{"timeout", "rev"},
            false,
        },
        "fire": cli.Command{
            "Notify all waiters on a key.",
            "",
            []string{"key"},
            []string{"rev"},
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
    key string,
    data string,
    limit uint,
    timeout uint,
    start []string,
    stop []string,
    cmd []string) error {

    var proc *exec.Cmd
    procchan := make(chan error)
    watchchan := make(chan error)

    oldindex := -1
    oldrev := core.NoRevision

    defer c.SyncLeave(key, data)

    for {
        // Ensure that we're in the group.
        // This is necessary to run each time because the watch
        // may have fired due to an underlying node change. This
        // means that the new node may have lost all ephemeral
        // information and membership may be completely new.
        utils.Print("CLIENT", "JOINING key=%s data=%s", key, data)
        newindex, newrev, err := c.SyncJoin(key, data, limit, 1)
        if err != nil {
            return err
        }
        utils.Print("CLIENT", "INDEX=%d REV=%d", newindex, newrev)

        if newindex != oldindex || !newrev.Equals(oldrev) {

            // Our value is our newindex.
            value := strconv.FormatInt(int64(newindex), 10)

            // If something has changed, stop the running process.
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

            // Start or stop appropriately.
            if newindex < 0 && (oldindex != newindex) {
                if stop != nil {
                    utils.Print("CLIENT", "EXEC-STOP")
                    do_exec(stop, []byte(value))
                }
            }
            if newindex >= 0 && (newindex != oldindex) {
                if start != nil {
                    utils.Print("CLIENT", "EXEC-START")
                    do_exec(start, []byte(value))
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
                    proc.Stdin = bytes.NewBuffer([]byte(value))
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

func cli_members(c *client.HiberaAPI, key string, data string, limit uint) error {
    index, members, rev, err := c.SyncMembers(key, data, limit)
    if err != nil {
        return err
    }
    // Output all members.
    os.Stdout.Write([]byte(fmt.Sprintf("%d\n%s\n", index, strings.Join(members, "\n"))))
    os.Stderr.Write([]byte(fmt.Sprintf("%s\n", rev.String())))
    return nil
}

func cli_get(c *client.HiberaAPI, key string, rev core.Revision) error {
    value, rev, err := c.DataGet(key, rev, 0)
    if err != nil {
        return err
    }
    // Output the string.
    os.Stdout.Write([]byte(fmt.Sprintf("%s\n", value)))
    os.Stderr.Write([]byte(fmt.Sprintf("%s\n", rev.String())))
    return nil
}

func cli_push(c *client.HiberaAPI, key string) error {
    stdin := bufio.NewReader(os.Stdin)
    rev := core.NoRevision

    for {
        // Read the next line.
        line, err := stdin.ReadSlice('\n')
        if err == io.EOF {
            return nil
        } else if err != nil {
            return err
        }

        // Post the update.
        rev, err = c.DataSet(key, rev.Next(), line)
        if err != nil {
            return err
        }
        os.Stderr.Write([]byte(fmt.Sprintf("%s\n", rev.String())))
    }

    return nil
}

func cli_pull(c *client.HiberaAPI, key string) error {
    rev := core.NoRevision

    for {
        // Read the next entry.
        var err error
        var value []byte
        value, rev, err = c.DataGet(key, rev, 0)
        if err != nil {
            return err
        }
        os.Stderr.Write([]byte(fmt.Sprintf("%s\n", rev.String())))

        // Write the data.
        _, err = os.Stdout.Write(value)
        if err == io.EOF {
            return nil
        } else if err != nil {
            return err
        }
    }

    return nil
}

func cli_set(c *client.HiberaAPI, key string, value *string, rev core.Revision) error {
    var err error
    if value == nil {
        // Fully read input.
        buf := new(bytes.Buffer)
        io.Copy(buf, os.Stdin)
        rev, err = c.DataSet(key, rev, buf.Bytes())
    } else {
        // Use the given string.
        rev, err = c.DataSet(key, rev, []byte(*value))
    }
    if err == nil {
        os.Stderr.Write([]byte(fmt.Sprintf("%s\n", rev.String())))
    }
    return err
}

func cli_remove(c *client.HiberaAPI, key string, rev core.Revision) error {
    rev, err := c.DataRemove(key, rev)
    if err == nil {
        os.Stderr.Write([]byte(fmt.Sprintf("%s\n", rev.String())))
    }
    return err
}

func cli_list(c *client.HiberaAPI) error {
    items, err := c.DataList()
    if err != nil {
        return err
    }
    for item, _ := range items {
        os.Stdout.Write([]byte(fmt.Sprintf("%s\n", item)))
    }
    return nil
}

func cli_sync(c *client.HiberaAPI, key string, output string, cmd []string, timeout uint) error {
    rev := core.NoRevision
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
        os.Stderr.Write([]byte(fmt.Sprintf("%s\n", rev.String())))

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

func cli_watch(c *client.HiberaAPI, key string, timeout uint, rev core.Revision) error {
    rev, err := c.EventWait(key, rev, timeout)
    if err == nil {
        os.Stdout.Write([]byte(fmt.Sprintf("%s\n", rev.String())))
        os.Stderr.Write([]byte(fmt.Sprintf("%s\n", rev.String())))
    }
    return err
}

func cli_fire(c *client.HiberaAPI, key string, rev core.Revision) error {
    rev, err := c.EventFire(key, rev)
    if err == nil {
        os.Stderr.Write([]byte(fmt.Sprintf("%s\n", rev.String())))
    }
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
    crev, err := core.RevisionFromString(*rev)
    if err != nil {
        return err
    }

    switch command {
    case "run":
        cmd := make_command(args[1:]...)
        start_cmd := make_command(*start)
        stop_cmd := make_command(*stop)
        return cli_run(client, args[0], *data, *limit, *timeout, start_cmd, stop_cmd, cmd)
    case "members":
        return cli_members(client, args[0], *data, *limit)
    case "get":
        return cli_get(client, args[0], crev)
    case "push":
        return cli_push(client, args[0])
    case "pull":
        return cli_pull(client, args[0])
    case "set":
        if len(args) > 1 {
            value := strings.Join(args[1:], " ")
            return cli_set(client, args[0], &value, crev)
        }
        return cli_set(client, args[0], nil, crev)
    case "remove":
        return cli_remove(client, args[0], crev)
    case "list":
        return cli_list(client)
    case "sync":
        cmd := make_command(args[1:]...)
        return cli_sync(client, args[0], *output, cmd, *timeout)
    case "watch":
        return cli_watch(client, args[0], *timeout, crev)
    case "fire":
        return cli_fire(client, args[0], crev)
    }

    return nil
}

func main() {
    cli.Main(cliInfo, do_cli)
}
