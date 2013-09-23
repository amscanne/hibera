package main

import (
    "fmt"
    "github.com/amscanne/hibera/cli"
    "github.com/amscanne/hibera/client"
    "github.com/amscanne/hibera/utils"
    "strings"
)

var replication = cli.Flags.Uint("replication", utils.DefaultN, "The replication factor.")
var path = cli.Flags.String("path", ".*", "The path for a given token (regular expression).")
var perms = cli.Flags.String("perms", "rwx", "Permissions (combination of r,w,x).")

var cliInfo = cli.Cli{
    "Hibera cluster control tool.",
    map[string]cli.Command{
        "activate": cli.Command{
            "Activate the API target.",
            "",
            []string{},
            []string{"replication"},
            false,
        },
        "deactivate": cli.Command{
            "Deactivate the API target.",
            "",
            []string{},
            []string{},
            false,
        },
        "nodes": cli.Command{
            "List all nodes.",
            "",
            []string{},
            []string{},
            false,
        },
        "data": cli.Command{
            "List all data and associated replication.",
            "",
            []string{},
            []string{},
            false,
        },
        "list-namespaces": cli.Command{
            "List all namespaces.",
            "",
            []string{},
            []string{},
            false,
        },
        "list-tokens": cli.Command{
            "List access tokens.",
            "",
            []string{},
            []string{},
            false,
        },
        "show-token": cli.Command{
            "Show the given access token.",
            "",
            []string{"token"},
            []string{},
            false,
        },
        "set-token": cli.Command{
            "Set given permissions.",
            "",
            []string{"token"},
            []string{"path", "perms"},
            false,
        },
    },
    cli.Options,
}

func cli_activate(c *client.HiberaAPI) error {
    return c.Activate(*replication)
}

func cli_deactivate(c *client.HiberaAPI) error {
    return c.Deactivate()
}

func cli_nodes(c *client.HiberaAPI) error {
    info, _, err := c.Info(true)
    if err != nil {
        return err
    }

    for id, node := range info.Nodes {
        url := fmt.Sprintf("url=%s", node.API())
        domain := fmt.Sprintf("domain=%s", node.Domain)
        keys := fmt.Sprintf("keys=%d", len(node.Keys))
        active := fmt.Sprintf("active=%t", node.Active)
        modified := fmt.Sprintf("modified=%s", node.Modified.String())
        current := fmt.Sprintf("current=%s", node.Current.String())
        dropped := fmt.Sprintf("dropped=%d", node.Dropped)
        fmt.Printf("%s %s %s %s %s %s %s %s\n",
            id, url, domain, keys, active, modified, current, dropped)
    }

    return nil
}

func cli_data(c *client.HiberaAPI) error {
    items, err := c.DataList()
    if err != nil {
        return err
    }

    for key, count := range items {
        fmt.Printf("%s %d\n", key, count)
    }

    return nil
}

func cli_list_namespaces(c *client.HiberaAPI) error {
    info, _, err := c.Info(true)
    if err != nil {
        return err
    }

    for ns, _ := range info.Access {
        fmt.Printf("%s\n", ns)
    }

    return nil
}

func cli_list_tokens(c *client.HiberaAPI) error {
    tokens, _, err := c.AccessList()
    if err != nil {
        return err
    }
    for _, token := range tokens {
        fmt.Printf("%s\n", token)
    }
    return nil
}

func cli_show_token(c *client.HiberaAPI, auth string) error {
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

func cli_set_token(c *client.HiberaAPI, auth string, path string, perms string) error {
    read := strings.Index(perms, "r") >= 0
    write := strings.Index(perms, "w") >= 0
    execute := strings.Index(perms, "x") >= 0
    _, err := c.AccessUpdate(auth, path, read, write, execute)
    return err
}

func do_cli(command string, args []string) error {

    client := cli.Client()

    switch command {
    case "activate":
        return cli_activate(client)
    case "deactivate":
        return cli_deactivate(client)
    case "nodes":
        return cli_nodes(client)
    case "data":
        return cli_data(client)
    case "list-namespaces":
        return cli_list_namespaces(client)
    case "list-tokens":
        return cli_list_tokens(client)
    case "show-token":
        return cli_show_token(client, args[0])
    case "set-token":
        return cli_set_token(client, args[0], *path, *perms)
    }

    return nil
}

func main() {
    cli.Main(cliInfo, do_cli)
}
