package main

import (
    "bufio"
    "bytes"
    "flag"
    "fmt"
    "hibera/client"
    "hibera/core"
    "hibera/utils"
    "io"
    "io/ioutil"
    "log"
    "math/rand"
    "os"
    "os/exec"
    "strings"
    "syscall"
    "time"
)

var api = flag.String("api", "", "API address (comma-separated list).")
var auth = flag.String("auth", "", "Authorization token.")
var host = flag.String("host", "", "Override for the host header (namespace).")
var timeout = flag.Uint("timeout", 0, "Timeout (in ms) for acquiring a lock.")
var name = flag.String("name", "", "Name to use (other than machine address).")
var start = flag.String("start", "", "Script to start the given service.")
var stop = flag.String("stop", "", "Script to stop the given service.")
var limit = flag.Uint("limit", 0, "Limit for machines to run or simultanous locks.")
var output = flag.String("output", "", "Output file for sync.")
var delay = flag.Uint("delay", 1000, "Delay and retry failed requests.")
var data = flag.Bool("data", false, "Use the synchronization group data mapping.")
var path = flag.String("path", ".*", "The path for a given token.")
var perms = flag.String("perms", "rwx", "Permissions (combination of r,w,x).")
var debug = flag.Bool("debug", false, "Enable all debugging.")

var mainmsg = `usage: hibera <command> ... [options]

options for all commands:

    [-api <address:port>         --- The API address.
         [,<address:port>...]]

    [-auth <authorization-key>]  --- The authorization key.

    [-host <namespace>]          --- The namespace to use.

    [-delay <delay>]             --- Delay in milliseconds to

cluster commands:

    nodes                        --- List all nodes.
    active                       --- List all active nodes.
    info <id>                    --- Show node info.
    activate                     --- Activate the API target.
    deactivate                   --- Deactivate the API target.

access commands:

    tokens                       --- Show access tokens.
    access <token>               --- Show the given token.
    permissions <token>          --- Grant given permissions.
        [-path <path>]
        [-perms <perms>]

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

    list                         --- List all keys.

    get <key>                    --- Get the contents of the key.
    set <key> [value]            --- Set the contents of the key.

    push <key>                   --- Stream up contents of the key.
    pull <key>                   --- Stream down contents of the key.

    remove <key>                 --- Remove the given key.
    sync <key>                   --- Synchronize a key, either as
         [-output <file>]            stdin to a named script to
         [-timeout <timeout>]        directly to the named file.
         [<run-script> ...]

    bench <key>                  --- Run a benchmark.

event commands:

    watch <key>                  --- Wait for an update of the key.
         [-timeout <timeout>]
    fire <key>                   --- Notify all waiters on the key.
`

func do_exec(command []string, input []byte) error {
    utils.Print("CLIENT", "Executing '%s'...", strings.Join(command, " "))
    cmd := exec.Command(command[0], command[1:]...)
    cmd.Stdin = bytes.NewBuffer(input)
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    return cmd.Run()
}

func cli_activate(c *client.HiberaAPI) error {
    return c.Activate()
}

func cli_deactivate(c *client.HiberaAPI) error {
    return c.Deactivate()
}

func cli_nodes(c *client.HiberaAPI) error {
    nodes, _, err := c.NodeList(false)
    if err != nil {
        return err
    }
    for _, id := range nodes {
        fmt.Printf("%s\n", id)
    }
    return nil
}

func cli_active(c *client.HiberaAPI) error {
    nodes, _, err := c.NodeList(true)
    if err != nil {
        return err
    }
    for _, id := range nodes {
        fmt.Printf("%s\n", id)
    }
    return nil
}

func cli_info(c *client.HiberaAPI, key core.Key) error {
    node, _, err := c.NodeGet(key.Key)
    if err != nil {
        return err
    }

    fmt.Printf("addr: %s\n", node.Addr)
    fmt.Printf("domain: %s\n", node.Domain)
    fmt.Printf("active: %t\n", node.Active)
    fmt.Printf("modified: %d\n", node.Modified)
    fmt.Printf("current: %d\n", node.Current)
    fmt.Printf("dropped: %d\n", node.Dropped)
    fmt.Printf("keys:\n")
    for _, key := range node.Keys {
        fmt.Printf("- %s\n", key)
    }
    return nil
}

func cli_tokens(c *client.HiberaAPI) error {
    tokens, _, err := c.AccessList(core.Namespace(*host))
    if err != nil {
        return err
    }
    for _, token := range tokens {
        fmt.Printf("%s\n", token)
    }
    return nil
}

func cli_access(c *client.HiberaAPI, auth core.Key) error {
    val, _, err := c.AccessGet(auth)
    if err != nil {
        return err
    }

    for path, perms := range *val {
        asstr := func(val bool, t string) string {
            if val {
                return t
            }
            return "-"
        }
        rs := asstr(perms.Read, "r")
        ws := asstr(perms.Write, "w")
        xs := asstr(perms.Execute, "x")
        fmt.Printf("%s%s%s %s\n", rs, ws, xs, path)
    }
    return nil
}

func cli_permissions(c *client.HiberaAPI, auth core.Key, path string, perms string) error {
    read := strings.Index(perms, "r") >= 0
    write := strings.Index(perms, "w") >= 0
    execute := strings.Index(perms, "x") >= 0
    _, err := c.AccessUpdate(auth, path, read, write, execute)
    return err
}

func cli_run(
    c *client.HiberaAPI,
    key core.Key,
    name string,
    limit uint,
    timeout uint,
    start []string,
    stop []string,
    cmd []string,
    dodata bool) error {

    var value []byte
    var proc *exec.Cmd
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
                        core.Namespace(*host),
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

func cli_members(c *client.HiberaAPI, key core.Key, name string, limit uint) error {
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

func cli_in(c *client.HiberaAPI, key core.Key, name string, limit uint) (bool, error) {
    index, _, _, err := c.SyncMembers(key, name, limit)
    if err != nil {
        return false, err
    }
    if index >= 0 {
        value, _, err := c.DataGet(
            core.Key{key.Namespace, fmt.Sprintf("%s.%d", key.Key, index)},
            core.ZeroRevision, 0)
        if err == nil {
            os.Stdout.Write(value)
        }
        return true, nil
    }
    return false, nil
}

func cli_out(c *client.HiberaAPI, key core.Key, value *string, name string, limit uint) (bool, error) {
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
                core.Key{key.Namespace, fmt.Sprintf("%s.%d", key.Key, index)},
                core.ZeroRevision, buf.Bytes())
        } else {
            // Use the given string.
            _, err = c.DataSet(
                core.Key{key.Namespace, fmt.Sprintf("%s.%d", key, index)},
                core.ZeroRevision, []byte(*value))
        }
        return true, err
    }
    return false, err
}

func cli_get(c *client.HiberaAPI, key core.Key) error {
    value, _, err := c.DataGet(key, core.ZeroRevision, 0)
    if err != nil {
        return err
    }
    // Output the string.
    fmt.Printf("%s", value)
    return nil
}

func cli_push(c *client.HiberaAPI, key core.Key) error {
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

func cli_pull(c *client.HiberaAPI, key core.Key) error {
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

func cli_set(c *client.HiberaAPI, key core.Key, value *string) error {
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

func cli_remove(c *client.HiberaAPI, key core.Key) error {
    _, err := c.DataRemove(key, core.ZeroRevision)
    return err
}

func cli_list(c *client.HiberaAPI, namespace string) error {
    items, err := c.DataList(core.Namespace(namespace))
    if err != nil {
        return err
    }
    for _, item := range items {
        fmt.Printf("%s\n", item)
    }
    return nil
}

func cli_sync(c *client.HiberaAPI, key core.Key, output string, cmd []string, timeout uint) error {

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

func cli_watch(c *client.HiberaAPI, key core.Key, timeout uint) error {
    _, err := c.EventWait(key, core.ZeroRevision, timeout)
    return err
}

func cli_fire(c *client.HiberaAPI, key core.Key) error {
    _, err := c.EventFire(key, core.ZeroRevision)
    return err
}

func do_worker(c *client.HiberaAPI, key core.Key, data_size int, count int, err_chan chan error) {
    data := make([]byte, data_size, data_size)
    for i := 0; i < count; i += 1 {
        _, err := c.DataSet(key, core.ZeroRevision, data)
        if err != nil {
            err_chan <- err
        }
    }
    err_chan <- nil
}

func do_bench(c *client.HiberaAPI, key core.Key, data_size int, workers int) error {
    err_chan := make(chan error, workers)
    count := 512 / workers

    start := time.Now()
    for i := 0; i < workers; i += 1 {
        go do_worker(
            c,
            core.Key{key.Namespace, fmt.Sprintf("%s.%d", key.Key, i)},
            data_size, count, err_chan)
    }
    for i := 0; i < workers; i += 1 {
        err := <-err_chan
        if err != nil {
            return err
        }
    }
    duration := time.Since(start)

    fmt.Printf(
        "workers=%d, data_size=%d, ops=%.2f/s, rate=%.2fMb/s\n",
        workers,
        data_size,
        float64(count*workers)/duration.Seconds(),
        float64(count*data_size*workers/1024)/duration.Seconds())
    return nil
}

func cli_bench(c *client.HiberaAPI, key core.Key) error {
    data_sizes := []int{128, 1024, 4096, 8192}
    n_workers := []int{1, 4, 16, 64, 256, 512}

    for _, data_size := range data_sizes {
        for _, n_worker := range n_workers {
            err := do_bench(c, key, data_size, n_worker)
            if err != nil {
                return err
            }
        }
    }

    return nil
}

func usage(msg string) {
    fmt.Printf("%s\n", msg)
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
    if len(os.Args) < 2 {
        usage(mainmsg)
    }
    command := os.Args[1]
    os.Args = os.Args[1:]

    key := core.Key{core.Namespace(*host), ""}
    if command == "nodes" ||
        command == "active" ||
        command == "tokens" ||
        command == "activate" ||
        command == "deactivate" ||
        command == "list" {
    } else {
        if len(os.Args) < 2 {
            usage(mainmsg)
        }
        key = core.Key{core.Namespace(*host), os.Args[1]}
        os.Args = os.Args[1:]
    }
    flag.Parse()
    if *debug {
        utils.EnableDebugging()
    }

    client := func() *client.HiberaAPI {
        // Create our client.
        return client.NewHiberaClient(*api, *auth, *delay)
    }

    switch command {
    case "activate":
        err = cli_activate(client())
        break
    case "deactivate":
        err = cli_deactivate(client())
        break
    case "nodes":
        err = cli_nodes(client())
        break
    case "active":
        err = cli_active(client())
        break
    case "info":
        err = cli_info(client(), key)
        break
    case "tokens":
        err = cli_tokens(client())
        break
    case "access":
        err = cli_access(client(), key)
        break
    case "permissions":
        err = cli_permissions(client(), key, *path, *perms)
        break
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
    case "push":
        err = cli_push(client(), key)
        break
    case "pull":
        err = cli_pull(client(), key)
        break
    case "set":
        if flag.NArg() > 0 {
            value := strings.Join(flag.Args(), " ")
            err = cli_set(client(), key, &value)
        } else {
            err = cli_set(client(), key, nil)
        }
        break
    case "remove":
        err = cli_remove(client(), key)
        break
    case "list":
        err = cli_list(client(), *host)
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
    case "bench":
        err = cli_bench(client(), key)
        break
    default:
        usage(mainmsg)
    }

    if err != nil {
        log.Fatal("Error: ", err)
    }
}
