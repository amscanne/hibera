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
    "strconv"
    "syscall"
    "time"
    "math/rand"
    "hibera/client"
    "hibera/utils"
)

var api = flag.String("api", "", "API address.")
var auth = flag.String("auth", "", "Authorization key.")
var timeout = flag.Uint("timeout", 0, "Timeout (in ms) for acquiring a lock.")
var name = flag.String("name", "", "Name to use (other than machine address).")
var start = flag.String("start", "", "Script to start the given service.")
var stop = flag.String("stop", "", "Script to stop the given service.")
var limit = flag.Uint("limit", 0, "Limit for machines to run or simultanous locks.")
var output = flag.String("output", "", "Output file for sync.")
var delay = flag.Uint("delay", 1000, "Delay and retry failed requests.")
var data = flag.Bool("data", false, "Use the synchronization group data mapping.")

var usagemsg = `usage: hibera <command> <key> [options]

options for all commands:

    [-api <address:port>         --- The API address.
         [,<address:port>...]]

    [-auth <authorization-key>]  --- The authorization key.

    [-delay <delay>]             --- Delay in milliseconds to
                                     reconnect to API servers.

cluster commands:

    info                         --- Show cluster info.

synchronization commands:

    run <key>                    --- Conditionally run up-to <limit>
        [-name <name>]               process across the cluster.
        [-limit <limit>]             If you use the run-script element,
        [-timeout <timeout>]         then the run command will exit when
        [-start <start-script>]      your command finishes. If the script
        [-stop <stop-script>]        doesn't exit, then it will be killed
        [-data]                      when this node loses membership, and
        [<run-script> ...]           restarted when it gains membership.

       For example, to join a group while a script runs:
        run foo.bar script.sh

       To run something exclusively (locked):
        run foo.bar -limit 1 script.sh

       To add a timeout to the lock:
        run foo.bar -limit 1 -timeout 1000 script.sh

       To start N instances of myapp at all times:
        run foo.bar -limit N
                    -start '/etc/init.d/myapp start'
                    -stop '/etc/init.d/myapp stop'

    members <key>                --- Show current members of the
            [-name <name>]           given group. The first line will
            [-limit <number>]        be this connections index in the
                                     list.

    in <key>                     --- If this process is part of
         [-name <name>]              the synchronization group named,
         [-limit <limit>]            fetch the associated data. If
                                     not in the set, exit with in error.

    out <key>                    --- If this process is part of
         [-name <name>]              the synchronization group named,
         [-limit <limit>]            set the associated data.

data commands:

    get <key>                    --- Get the contents of the key.

    set <key> [value]            --- Set the contents of the key.

    rm <key>                     --- Remove the given key.

    ls                           --- List all keys.

    clear                        --- Clear all data.

    sync <key>                   --- Synchronize a key, either as
         [-output <file>]            stdin to a named script to
         [-timeout <timeout>]        directly to the named file.
         [<run-script> ...]

event commands:

    watch <key>                  --- Wait for an update of the key.
         [-timeout <timeout>]

    fire <key>                   --- Notify all waiters on the key.

helper commands:

    generate <config> <out-dir>  --- Generate an upstart configuration
                                     for the given config file.

    update <config> [files...]   --- Update the given files in relevant
                                     services. If no files are specified,
                                     then all service files are updated.
`

func do_exec(command []string, input []byte) error {
    utils.Print("CLIENT", "Executing '%s'...", strings.Join(command, " "))
    cmd := exec.Command(command[0], command[1:]...)
    cmd.Stdin = bytes.NewBuffer(input)
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    return cmd.Run()
}

func cli_info(c *client.HiberaAPI, rev uint64) error {
    value, _, err := c.Info(rev)
    if err != nil {
        return err
    }
    os.Stdout.Write(value)
    return nil
}

func cli_run(c *client.HiberaAPI, key string, name string, limit uint, timeout uint, start []string, stop []string, cmd []string, dodata bool) error {

    var value []byte
    var proc *exec.Cmd
    procchan := make(chan error)
    watchchan := make(chan error)

    oldindex := -1
    oldrev := uint64(0)
    olddatarev := uint64(0)

    defer c.Leave(key, name)

    for {
        // Ensure that we're in the group.
        // This is necessary to run each time because the watch
        // may have fired due to an underlying node change. This
        // means that the new node may have lost all ephemeral
        // information and membership may be completely new.
        utils.Print("CLIENT", "JOINING key=%s name=%s", string(key), name)
        newindex, newrev, err := c.Join(key, name, limit, 1)
        if err != nil {
            return err
        }
        utils.Print("CLIENT", "INDEX=%d REV=%d", newindex, newrev)

        if newindex != oldindex || newrev != oldrev {

            datachanged := false

            // Update the mapped data.
            if newindex >= 0 && dodata {

                // Opportunistic read of the data.
                // If there's nothing, we provide an empty set.
                var datarev uint64
                value, datarev, err = c.Get(fmt.Sprintf("%s.%d", key, newindex), 0, 0)
                if err != nil {
                    datarev = uint64(0)
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
                        Chroot: "",
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
            rev, err := c.Wait(key, newrev, timeout)
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
        value, _, err := c.Get(fmt.Sprintf("%s.%d", key, index), 0, 0)
        if err == nil {
            os.Stdout.Write(value)
        }
        return true, nil
    }
    return false, nil
}

func cli_out(c *client.HiberaAPI, key string, value *string, name string, limit uint) (bool, error) {
    index, _, _, err := c.Members(key, name, limit)
    if err != nil {
        return false, err
    }
    if index >= 0 {
        var err error
        if value == nil {
            // Fully read input.
            buf := new(bytes.Buffer)
            io.Copy(buf, os.Stdin)
            _, err = c.Set(fmt.Sprintf("%s.%d", key, index), 0, buf.Bytes())
        } else {
            // Use the given string.
            _, err = c.Set(fmt.Sprintf("%s.%d", key, index), 0, []byte(*value))
        }
        return true, err
    }
    return false, err
}

func cli_get(c *client.HiberaAPI, key string) error {
    value, _, err := c.Get(key, 0, 0)
    if err != nil {
        return err
    }
    // Output the string.
    fmt.Printf("%s", value)
    return nil
}

func cli_set(c *client.HiberaAPI, key string, value *string) error {
    var err error
    if value == nil {
        // Fully read input.
        buf := new(bytes.Buffer)
        io.Copy(buf, os.Stdin)
        _, err = c.Set(key, 0, buf.Bytes())
    } else {
        // Use the given string.
        _, err = c.Set(key, 0, []byte(*value))
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

func cli_sync(c *client.HiberaAPI, key string, output string, cmd []string, timeout uint) error {

    rev := uint64(0)
    var value []byte
    var err error

    for {
        // Get the current value.
        utils.Print("CLIENT", "GET rev=%d", rev)
        value, rev, err = c.Get(key, rev, timeout)
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

func cli_watch(c *client.HiberaAPI, key string, timeout uint) error {
    _, err := c.Wait(key, 0, timeout)
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

func main() {
    var err error

    // NOTE: We need the random number generator,
    // as it will be seed with 1 by default (and
    // hence always exhibit the same sequence).
    rand.Seed(time.Now().UTC().UnixNano())

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

    client := func() *client.HiberaAPI {
        // Create our client.
        return client.NewHiberaClient(*api, *auth, *delay)
    }

    // Do our stuff.
    switch command {
    case "info":
        if flag.NArg() > 0 {
            var rev uint64
            rev, err = strconv.ParseUint(flag.Args()[0], 0, 64)
            if err == nil {
                err = cli_info(client(), rev)
            }
        } else {
            err = cli_info(client(), 0)
        }
    case "run":
        cmd := make_command(flag.Args()...)
        start_cmd := make_command(*start)
        stop_cmd := make_command(*stop)
        err = cli_run(client(), key, *name, *limit, *timeout, start_cmd, stop_cmd, cmd, *data)
        break
    case "members":
        err = cli_members(client(), key, *name, *limit)
        break
    case "in":
        var in bool
        in, err = cli_in(client(), key, *name, *limit)
        if err == nil && !in {
            os.Exit(1)
        }
        break
    case "out":
        var in bool
        if flag.NArg() > 0 {
            value := strings.Join(flag.Args(), " ")
            in, err = cli_out(client(), key, &value, *name, *limit)
        } else {
            in, err = cli_out(client(), key, nil, *name, *limit)
        }
        if err == nil && !in {
            os.Exit(1)
        }
        break
    case "get":
        err = cli_get(client(), key)
        break
    case "set":
        if flag.NArg() > 0 {
            value := strings.Join(flag.Args(), " ")
            err = cli_set(client(), key, &value)
        } else {
            err = cli_set(client(), key, nil)
        }
        break
    case "rm":
        err = cli_rm(client(), key)
        break
    case "ls":
        err = cli_ls(client())
        break
    case "clear":
        err = cli_clear(client())
        break
    case "sync":
        cmd := make_command(flag.Args()...)
        err = cli_sync(client(), key, *output, cmd, *timeout)
        break
    case "watch":
        err = cli_watch(client(), key, *timeout)
        break
    case "fire":
        err = cli_fire(client(), key)
        break
    case "generate":
        if flag.NArg() == 1 {
            err = cli_generate(key, flag.Args()[0], *api)
        } else {
            usage()
        }
        break
    case "update":
        err = cli_update(client(), key, flag.Args()...)
        break
    default:
        usage()
    }

    if err != nil {
        log.Fatal("Error: ", err)
    }
}
