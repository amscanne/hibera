package main

import (
    "flag"
    "fmt"
    "hibera/cluster"
    "hibera/server"
    "hibera/storage"
    "hibera/cli"
    "hibera/utils"
    "net"
    "os"
    "runtime"
    "runtime/pprof"
    "strings"
    "errors"
)

var auth = flag.String("root", utils.DefaultBind, "The root authorization token.")
var bind = flag.String("bind", utils.DefaultBind, "Bind address for the server.")
var port = flag.Uint("port", utils.DefaultPort, "Bind port for the server.")
var path = flag.String("path", utils.DefaultPath, "Backing storage path.")
var domain = flag.String("domain", utils.DefaultDomain, "Failure domain for this server.")
var keys = flag.Uint("keys", utils.DefaultKeys, "The number of keys for this node (weight).")
var seeds = flag.String("seeds", utils.DefaultSeeds, "Seeds for joining the cluster.")
var active = flag.Uint("active", utils.DefaultActive, "Maximum active simutaneous clients.")
var profile = flag.String("profile", "", "Enabling profiling and write to file.")

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
        "run" : cli.Command{
            "Start a hibera server.",
            "",
            []string{},
            []string{
                "root",
                "bind",
                "port",
                "path",
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
        f, err := os.Create(*profile)
        if err != nil {
            return err
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

    // Initialize our storage.
    backend, err := storage.NewBackend(*path)
    if err != nil {
        return err
    }
    go backend.Run()

    // Create our cluster.
    // We load our keys from the persistent storage.
    ids, err := backend.LoadIds(*keys)
    if err != nil {
        return err
    }
    c, err := cluster.NewCluster(backend, addr, *auth, *domain, ids)
    if err != nil {
        return err
    }

    // Startup our server.
    s, err := server.NewServer(c, *bind, *port, strings.Split(*seeds, ","), *active)
    if err != nil {
        return err
    }

    // Run our server.
    s.Run()
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
