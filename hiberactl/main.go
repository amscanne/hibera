package main

import (
    "fmt"
    "hibera/cli"
    "hibera/client"
    "hibera/utils"
)

var replication = cli.Flags.Uint("replication", utils.DefaultN, "The replication factor.")

var cliInfo = cli.Cli{
    "Hibera cluster control tool.",
    map[string]cli.Command{
        "nodes": cli.Command{
            "List all nodes.",
            "",
            []string{},
            []string{},
            false,
        },
        "active": cli.Command{
            "List all active nodes.",
            "",
            []string{},
            []string{},
            false,
        },
        "info": cli.Command{
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
            []string{"N"},
            false,
        },
    },
    []string{"api", "auth", "delay"},
}

func cli_activate(c *client.HiberaAPI) error {
    return c.Activate(*replication)
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

func cli_info(c *client.HiberaAPI, id string) error {
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

func do_cli(command string, args []string) error {

    client := cli.Client()

    switch command {
    case "activate":
        return cli_activate(client)
    case "deactivate":
        return cli_deactivate(client)
    case "nodes":
        return cli_nodes(client)
    case "active":
        return cli_active(client)
    case "info":
        return cli_info(client, args[0])
    }

    return nil
}

func main() {
    cli.Main(cliInfo, do_cli)
}
