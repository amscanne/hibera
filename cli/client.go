package cli

import (
    "hibera/client"
    "hibera/core"
)

var api = Flags.String("api", "", "API address (comma-separated list).")
var auth = Flags.String("auth", "", "Authorization token.")
var host = Flags.String("host", "", "Override for the host header (namespace).")
var delay = Flags.Uint("delay", 1000, "Delay and retry failed requests.")

var Options = []string{"api", "auth", "host", "delay"}

func Client() *client.HiberaAPI {
    return client.NewHiberaClient(*api, *auth, *delay, *host)
}

func Namespace() core.Namespace {
    return core.Namespace(*host)
}
