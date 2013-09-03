package main

import (
    "fmt"
    "hibera/cli"
    "hibera/client"
    "hibera/utils"
    "strings"
)

var replication = cli.Flags.Uint("replication", utils.DefaultN, "The replication factor.")
var path = cli.Flags.String("path", ".*", "The path for a given token (regular expression).")
var perms = cli.Flags.String("perms", "rwx", "Permissions (combination of r,w,x).")

var cliInfo = cli.Cli{
    "Hibera cluster control tool.",
    map[string]cli.Command{
        "list-nodes": cli.Command{
            "List all nodes.",
            "",
            []string{},
            []string{},
            false,
        },
        "list-active": cli.Command{
            "List all active nodes.",
            "",
            []string{},
            []string{},
            false,
        },
        "node-info": cli.Command{
            "Show node info.",
            "",
            []string{"id"},
            []string{},
            false,
        },
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

func cli_list_nodes(c *client.HiberaAPI) error {
    nodes, _, err := c.NodeList(false)
    if err != nil {
        return err
    }
    for _, id := range nodes {
        fmt.Printf("%s\n", id)
    }
    return nil
}

func cli_list_active(c *client.HiberaAPI) error {
    nodes, _, err := c.NodeList(true)
    if err != nil {
        return err
    }
    for _, id := range nodes {
        fmt.Printf("%s\n", id)
    }
    return nil
}

func cli_node_info(c *client.HiberaAPI, id string) error {
    node, _, err := c.NodeGet(id)
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
    case "list-nodes":
        return cli_list_nodes(client)
    case "list-active":
        return cli_list_active(client)
    case "node-info":
        return cli_node_info(client, args[0])
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
