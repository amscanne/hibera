package main

import (
    "fmt"
    "hibera/cli"
    "hibera/client"
    "strings"
)

var path = cli.Flags.String("path", ".*", "The path for a given token (regular expression).")
var perms = cli.Flags.String("perms", "rwx", "Permissions (combination of r,w,x).")

var cliInfo = cli.Cli{
    "Hibera permissions client.",
    map[string]cli.Command{
        "list": cli.Command{
            "List access tokens.",
            "",
            []string{},
            []string{},
            false,
        },
        "show": cli.Command{
            "Show the given access token.",
            "",
            []string{"token"},
            []string{},
            false,
        },
        "set": cli.Command{
            "Set given permissions.",
            "",
            []string{"token"},
            []string{"path", "perms"},
            false,
        },
    },
    cli.Options,
}

func cli_list(c *client.HiberaAPI) error {
    tokens, _, err := c.AccessList()
    if err != nil {
        return err
    }
    for _, token := range tokens {
        fmt.Printf("%s\n", token)
    }
    return nil
}

func cli_show(c *client.HiberaAPI, auth string) error {
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

func cli_set(c *client.HiberaAPI, auth string, path string, perms string) error {
    read := strings.Index(perms, "r") >= 0
    write := strings.Index(perms, "w") >= 0
    execute := strings.Index(perms, "x") >= 0
    _, err := c.AccessUpdate(auth, path, read, write, execute)
    return err
}

func do_cli(command string, args []string) error {

    client := cli.Client()

    switch command {
    case "list":
        return cli_list(client)
    case "show":
        return cli_show(client, args[0])
    case "set":
        return cli_set(client, args[0], *path, *perms)
    }

    return nil
}

func main() {
    cli.Main(cliInfo, do_cli)
}
