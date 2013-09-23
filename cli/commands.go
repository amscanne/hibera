package cli

import (
    "bufio"
    "flag"
    "fmt"
    "github.com/amscanne/hibera/utils"
    "io/ioutil"
    "log"
    "math/rand"
    "os"
    "regexp"
    "runtime"
    "runtime/pprof"
    "sort"
    "strconv"
    "strings"
    "syscall"
    "time"
)

var Flags = flag.NewFlagSet("github.com/amscanne/hibera", flag.ExitOnError)

type Command struct {
    Help     string
    LongHelp string
    Args     []string
    Options  []string
    Extra    bool
}

type Cli struct {
    Help     string
    Commands map[string]Command
    Options  []string
}

// Universal debug flag.
// This is available to all programs.
var debug = Flags.Bool("debug", false, "Enable all debugging.")
var cpuprofile = Flags.String("cpuprofile", "", "Enabling CPU profiling and write to file.")
var memprofile = Flags.String("memprofile", "", "Enabling memory profiling and write to file.")

// Universal help command.
// This is available to all programs.
// (And it is handled specially by the CLI code).
var HelpCommand = Command{"Display help on a given command.", "", []string{"command"}, []string{}, false}

func top_usage(cli Cli, arg0 string) {
    fmt.Printf("usage: %s <command> [<options>...] <arguments...>\n\n", arg0)
    fmt.Printf("    %s\n\n", cli.Help)
}

func command_list(cli Cli, arg0 string) {
    fmt.Printf("available commands:\n\n")
    command_list := make([]string, len(cli.Commands))
    i := 0
    for command, _ := range cli.Commands {
        command_list[i] = command
        i += 1
    }
    sort.Strings(command_list)
    for _, command := range command_list {
        spec := cli.Commands[command]
        fmt.Printf("    %-20s    %s\n", command, spec.Help)
    }
    fmt.Printf("\n")
    fmt.Printf("   You can use '%s help <command>' for detailed help.\n", arg0)
    fmt.Printf("\n")
}

func command_help(cli Cli, arg0 string, command string, spec Command) {
    fmt.Printf("usage: %s [options...] %s", arg0, command)
    for _, arg := range spec.Args {
        fmt.Printf(" <%s>", arg)
    }
    if spec.Extra {
        fmt.Printf(" [...]")
    }
    fmt.Printf("\n\n    %s\n\n", spec.Help)
    if spec.LongHelp != "" {
        fmt.Printf("%s\n\n", spec.LongHelp)
    }
    fmt.Printf("options:\n")
    print_flag := func(flag *flag.Flag) {
        fmt.Printf("    %-20s    %s\n",
            fmt.Sprintf("-%s=%s", flag.Name, flag.DefValue),
            flag.Usage)
    }
    for _, option := range cli.Options {
        print_flag(Flags.Lookup(option))
    }
    for _, option := range spec.Options {
        print_flag(Flags.Lookup(option))
    }
    fmt.Printf("\n")
}

func crankProcessors() error {
    cpu_info, err := os.OpenFile("/proc/cpuinfo", os.O_RDONLY, 0)
    if err != nil {
        return err
    }
    defer cpu_info.Close()
    reader := bufio.NewReader(cpu_info)
    count := 0
    re, err := regexp.Compile("^processor\t:")
    if err != nil {
        return err
    }
    for {
        line, _, err := reader.ReadLine()
        if err != nil {
            break
        }
        if re.Match(line) {
            count += 1
        }
    }

    runtime.GOMAXPROCS(count + 1)
    return nil
}

func crankFileDescriptors() error {
    // First set our soft limit.
    var rlim syscall.Rlimit
    err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim)
    if err != nil {
        return err
    }
    rlim.Cur = rlim.Max
    err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim)
    if err != nil {
        return err
    }

    // Then, read the system max.
    nr_open, err := os.OpenFile("/proc/sys/fs/nr_open", os.O_RDONLY, 0)
    if err != nil {
        return err
    }
    defer nr_open.Close()
    bytes, err := ioutil.ReadAll(nr_open)
    if err != nil {
        return err
    }
    max, err := strconv.ParseUint(strings.TrimSpace(string(bytes)), 10, 64)
    if err != nil {
        return err
    }

    // Try to adjust our maximum.
    rlim.Cur = max
    rlim.Max = max
    return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim)
}

var arg0 string
var command string
var args []string

func Main(cli Cli, run func(command string, args []string) error) {
    // Seed the random number generator.
    rand.Seed(time.Now().UTC().UnixNano())

    // Parse flags.
    arg0 = os.Args[0]
    usage := func() {
        top_usage(cli, arg0)
        command_list(cli, arg0)
    }
    Flags.Usage = usage
    Flags.Parse(os.Args[1:])
    args = Flags.Args()

    // Pull out our arguments.
    if len(args) == 0 {
        top_usage(cli, arg0)
        command_list(cli, arg0)
        os.Exit(1)
    }

    // Pull out the command.
    // (and reparse, we support options before and after).
    command = args[0]
    args = args[1:]
    Flags.Parse(args)
    args = Flags.Args()

    // Grab the spec.
    spec, ok := cli.Commands[command]
    if !ok {
        if command == "help" {
            spec = HelpCommand
        } else {
            top_usage(cli, arg0)
            command_list(cli, arg0)
            os.Exit(1)
        }
    }

    // Check the arguments match.
    if len(args) < len(spec.Args) ||
        (!spec.Extra && len(args) != len(spec.Args)) {
        if command == "help" && len(args) == 0 {
            command_list(cli, arg0)
            os.Exit(0)
        } else {
            command_help(cli, arg0, command, spec)
            os.Exit(1)
        }
    }

    // Enable debugging, if it's been specified.
    if *debug {
        utils.EnableDebugging()
    }

    // Crank up processors.
    err := crankProcessors()
    if err != nil {
        // Ignore.
    }

    // Crank up file descriptors.
    err = crankFileDescriptors()
    if err != nil {
        // Ignore.
    }

    // Turn on CPU profiling.
    if *cpuprofile != "" {
        f, err := os.OpenFile(*cpuprofile, os.O_RDWR|os.O_CREATE|syscall.O_CLOEXEC, 0644)
        if err != nil {
            log.Fatal("CPU profiling: ", err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

    // Turn on memory profiling.
    if *memprofile != "" {
        f, err := os.OpenFile(*memprofile, os.O_RDWR|os.O_CREATE|syscall.O_CLOEXEC, 0644)
        if err != nil {
            log.Fatal("Memory profiling: ", err)
        }
        defer pprof.WriteHeapProfile(f)
    }

    // Run the given function.
    if command == "help" {
        if args[0] == "help" {
            spec = HelpCommand
            ok = true
        } else {
            spec, ok = cli.Commands[args[0]]
        }

        if !ok {
            command_list(cli, arg0)
            os.Exit(1)
        } else {
            command_help(cli, arg0, args[0], spec)
            os.Exit(0)
        }
    } else {
        err := run(command, args)
        if err != nil {
            log.Fatal("Error: ", err)
        }
    }
}

func Restart(files []*os.File) error {

    // Make our new arguments.
    new_args := make([]string, 0)
    new_args = append(new_args, arg0)
    new_args = append(new_args, command)
    Flags.Visit(func(flag *flag.Flag) {
        new_args = append(new_args, fmt.Sprintf("-%s=%s", flag.Name, flag.Value))
    })
    for _, arg := range args {
        new_args = append(new_args, arg)
    }

    // Clear the close-on-exec flag for the files.
    for _, file := range files {
        syscall.RawSyscall(syscall.SYS_FCNTL, file.Fd(), syscall.F_SETFD, 0)
    }

    // Re-exec our process.
    err := syscall.Exec(arg0, new_args, nil)
    if err != nil {
        return err
    }

    // Ummm, success?
    // We will never get here.
    return nil
}
