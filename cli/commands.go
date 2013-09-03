package cli

import (
    "flag"
    "fmt"
    "hibera/utils"
    "log"
    "math/rand"
    "os"
    "sort"
    "time"
)

var Flags = flag.NewFlagSet("hibera", flag.ExitOnError)

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

func Main(cli Cli, run func(command string, args []string) error) {
    // Seed the random number generator.
    rand.Seed(time.Now().UTC().UnixNano())

    // Parse flags.
    arg0 := os.Args[0]
    usage := func() {
        top_usage(cli, arg0)
        command_list(cli, arg0)
    }
    Flags.Usage = usage
    Flags.Parse(os.Args[1:])
    args := Flags.Args()

    // Pull out our arguments.
    if len(args) == 0 {
        top_usage(cli, arg0)
        command_list(cli, arg0)
        os.Exit(1)
    }

    // Pull out the command.
    command := args[0]
    args = args[1:]

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
