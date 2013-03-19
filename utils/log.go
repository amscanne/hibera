package utils

import (
    "os"
    "log"
)

var cache = make(map[string]bool)

func isLogging(module string) bool {
    val, present := cache[module]
    if !present {
        all_on := os.Getenv("HIBERA_LOG_ALL") == "true"
        module_on := os.Getenv("HIBERA_LOG_" + module) == "true"
        module_off := os.Getenv("HIBERA_LOG_" + module) == "false"
        val = module_on || (all_on && !module_off)
        cache[module] = val
    }
    return val
}

func Print(module string, fmt string, v ...interface{}) {
    if isLogging(module) {
        log.Printf(module + " " + fmt, v...)
    }
}
