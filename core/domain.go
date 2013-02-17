package core

import (
    "os"
)

func defaultDomain() string {
    domain, err := os.Hostname()
    if err != nil {
        return ""
    }
    return domain
}

var DEFAULT_DOMAIN = defaultDomain()
