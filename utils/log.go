package utils

import (
    "log"
    "os"
    "strconv"
)

var cache = make(map[string]bool)
var pid = strconv.FormatInt(int64(os.Getpid()), 10)
var enabled = false

func isLogging(module string) bool {
    val, present := cache[module]
    if !present {
        all_on := enabled || os.Getenv("HIBERA_LOG_ALL") == "true"
        module_on := os.Getenv("HIBERA_LOG_"+module) == "true"
        module_off := os.Getenv("HIBERA_LOG_"+module) == "false"
        val = module_on || (all_on && !module_off)
        cache[module] = val
    }
    return val
}

func Print(module string, fmt string, v ...interface{}) {
    if isLogging(module) {
        log.Printf(pid+" "+module+" "+fmt, v...)
    }
}

func Debug(fmt string, v ...interface{}) {
    if enabled {
        log.Printf(pid+" "+fmt, v...)
    }
}

func EnableDebugging() {
    enabled = true
    cache = make(map[string]bool)
}
