package utils

import (
    "os"
    "log"
)

func Print(module string, fmt string, v ...interface{}) {
    all_on := os.Getenv("HIBERA_LOG_ALL") == "true"
    module_on := os.Getenv("HIBERA_LOG_" + module) == "true"
    module_off := os.Getenv("HIBERA_LOG_" + module) == "false"
    if module_on || (all_on && !module_off) {
        log.Printf(module + " " + fmt, v...)
    }
}
