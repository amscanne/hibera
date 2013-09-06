package main

import (
    "errors"
    "fmt"
    "hibera/cli"
    "hibera/cluster"
    "hibera/core"
    "hibera/server"
    "hibera/storage"
    "hibera/utils"
    "io/ioutil"
    "net"
    "os"
    "os/signal"
    "path"
    "strings"
    "syscall"
)

var root = cli.Flags.String("root", utils.DefaultBind, "The root authorization token.")
var bind = cli.Flags.String("bind", utils.DefaultBind, "Bind address for the server.")
var port = cli.Flags.Uint("port", utils.DefaultPort, "Bind port for the server.")
var logPath = cli.Flags.String("log", utils.DefaultLogPath, "Backing storage log path.")
var dataPath = cli.Flags.String("data", utils.DefaultDataPath, "Backing storage data path.")
var domain = cli.Flags.String("domain", utils.DefaultDomain, "Failure domain for this server.")
var keys = cli.Flags.Uint("keys", utils.DefaultKeys, "The number of keys for this node (weight).")
var seeds = cli.Flags.String("seeds", utils.DefaultSeeds, "Seeds for joining the cluster.")
var active = cli.Flags.Uint("active", utils.DefaultActive, "Maximum active simutaneous clients.")

var rootfd = cli.Flags.Int("rootfd", -1, "File descriptor for reading root token.")
var serverfd = cli.Flags.Int("serverfd", -1, "File descriptor used for server restarts.")

func discoverAddress() string {
    addrs, _ := net.InterfaceAddrs()
    for _, addr := range addrs {
        if ipnet, ok := addr.(*net.IPNet); ok {
            if ipnet.IP.IsGlobalUnicast() {
                return ipnet.IP.String()
            }
        }
    }

    return ""
}

var cliInfo = cli.Cli{
    "Run a Hibera server.",
    map[string]cli.Command{
        "run": cli.Command{
            "Start a hibera server.",
            "",
            []string{},
            []string{
                "root",
                "bind",
                "port",
                "log",
                "data",
                "domain",
                "keys",
                "seeds",
                "active",
            },
            false,
        },
    },
    []string{},
}

func setupRootFd(root string) (*os.File, error) {

    // Create the pipe the will pass the data.
    r, w, err := os.Pipe()
    if err != nil {
        return nil, err
    }

    // Write the password into our end of the pipe.
    n, err := w.Write([]byte(root))
    if n != len([]byte(root)) || err != nil {
        r.Close()
        w.Close()
        return nil, err
    }

    // Clear the password.
    // NOTE: It is the callers responsibility to set rootfd.
    cli.Flags.Set("root", "******")

    // Read our file.
    w.Close()
    return r, nil
}

func cli_run() error {

    // Check if the root value has been passed in directly.
    var root_passwd string
    if *rootfd >= 0 {
        // Read the root password.
        root_file := os.NewFile(uintptr(*rootfd), "-rootfd-")
        root_bytes, err := ioutil.ReadAll(root_file)
        if err != nil {
            return err
        }
        root_passwd = string(root_bytes)

        // Close the pipe.
        root_file.Close()
    } else {
        // Write the password over the pipe.
        rootpipe, err := setupRootFd(*root)
        if err != nil {
            return err
        }

        // Does not return.
        cli.Flags.Set("rootfd", "3")
        return cli.Restart([]*os.File{os.Stdin, os.Stdout, os.Stderr, rootpipe})
    }

    // Initialize our storage.
    backend, err := storage.NewStore(*logPath, *dataPath)
    if err != nil {
        return err
    }
    go backend.Run()

    // Sanity check addresses, ports, etc.
    var addr string
    if *bind == "" {
        addr = fmt.Sprintf("%s:%d", discoverAddress(), *port)
    } else {
        addr = fmt.Sprintf("%s:%d", *bind, *port)
    }
    if *port == 0 {
        return errors.New("Sorry, port can't be 0.")
    }

    // Create our cluster.
    // We load our keys from the persistent storage.
    ids, err := utils.LoadIds(path.Join(*dataPath, "ids"), *keys)
    if err != nil {
        return err
    }
    c, err := cluster.NewCluster(backend, addr, core.Token(root_passwd), *domain, ids)
    if err != nil {
        return err
    }

    // Startup our server.
    s, err := server.NewServer(c, *serverfd, *bind, *port, strings.Split(*seeds, ","), *active)
    if err != nil {
        return err
    }

    // Enable clean exit.
    terminate := make(chan os.Signal, 2)
    restart := make(chan os.Signal, 1)
    signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)
    signal.Notify(restart, syscall.SIGHUP)

    // Run our server.
    go s.Run()

    // Wait for a signal.
    for {
        select {
        case <-terminate:
            s.Stop()
            return nil

        case <-restart:
            // Setup our root password again.
            rootpipe, err := setupRootFd(root_passwd)
            if err != nil {
                // Just keep running.
                break
            }

            // Grab a restart Fd.
            restart := s.Restart()
            if err != nil {
                // Keep running.
                rootpipe.Close()
                break
            }

            // Save our restartfd.
            cli.Flags.Set("rootfd", "3")
            cli.Flags.Set("serverfd", "4")

            // Will not return.
            s.Stop()
            return cli.Restart([]*os.File{os.Stdin, os.Stdout, os.Stderr, rootpipe, restart})
        }
    }

    return nil
}

func do_cli(command string, args []string) error {
    switch command {
    case "run":
        return cli_run()
    }

    return nil
}

func main() {
    cli.Main(cliInfo, do_cli)
}
