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
    "net"
    "os"
    "path"
    "syscall"
    "os/signal"
    "runtime"
    "runtime/pprof"
    "strings"
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
var profile = cli.Flags.String("profile", "", "Enabling profiling and write to file.")

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
                "profile"},
            false,
        },
    },
    []string{},
}

func cli_run() error {

    // Sanity check addresses, ports, etc.
    var addr string
    if *bind == "" {
        addr = fmt.Sprintf("%s:%d", discoverAddress(), *port)
    } else {
        addr = fmt.Sprintf("%s:%d", *bind, *port)
    }
    if *port == 0 {
        errors.New("Sorry, port can't be 0.")
    }

    // Crank up processors.
    runtime.GOMAXPROCS(4)

    // Turn on profiling.
    if *profile != "" {
        f, err := os.OpenFile(*profile, os.O_RDWR|os.O_CREATE, 0644)
        if err != nil {
            return err
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

    // Initialize our storage.
    backend, err := storage.NewStore(*logPath, *dataPath)
    if err != nil {
        return err
    }
    go backend.Run()

    // Create our cluster.
    // We load our keys from the persistent storage.
    ids, err := utils.LoadIds(path.Join(*dataPath, "ids"), *keys)
    if err != nil {
        return err
    }
    c, err := cluster.NewCluster(backend, addr, core.Token(*root), *domain, ids)
    if err != nil {
        return err
    }

    // Startup our server.
    s, err := server.NewServer(c, *bind, *port, strings.Split(*seeds, ","), *active)
    if err != nil {
        return err
    }

    // Enable clean exit.
    terminate := make(chan os.Signal, 2)
    signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)

    // Run our server.
    go s.Run()

    // Wait for a signal.
    <-terminate
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
