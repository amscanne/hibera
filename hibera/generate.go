package main

import (
    "os"
    "fmt"
    "log"
    "bytes"
    "io/ioutil"
    "strings"
    "encoding/json"
    "path/filepath"
    "hibera/client"
)

type Service struct {
    Start string
    Stop string
    Pre []string
    Post []string
    Limit uint
    Timeout uint
    Files []string
    Sync string
}

func make_upstart(output string, name string, config *Service, api string) error {
    // Open the file.
    fullpath := filepath.Join(output, fmt.Sprintf("%s.conf", name))
    log.Printf("Generating %s...\n", fullpath)
    fo, err := os.Create(fullpath)
    if err != nil {
        return err
    }
    defer fo.Close()

    // Write it.
    header := fmt.Sprintf("description \"%s\"\nstart on runlevel [2345]\nstop on runlevel [016]\nrespawn\n", name)
    n, err := fo.WriteString(header)
    if n != len(header) || err != nil {
        return err
    }
    pre_start := fmt.Sprintf("pre-start script\n    %s\nend script\n", strings.Join(config.Pre, "\n    "))
    n, err = fo.WriteString(pre_start)
    if n != len(pre_start) || err != nil {
        return err
    }
    post_stop := fmt.Sprintf("post-stop script\n    %s\nend script\n", strings.Join(config.Post, "\n    "))
    n, err = fo.WriteString(post_stop)
    if n != len(post_stop) || err != nil {
        return err
    }
    syncstr := fmt.Sprintf("for file in $(hibera get %s); do hibera get %s:$file > $file; done; %s",
                            name, name, config.Sync)
    execstr := fmt.Sprintf("exec hibera run %s -api '%s' -limit %d -timeout %d -start '%s' -stop '%s' hibera sync %s '%s'\n",
                            name, api, config.Limit, config.Timeout, config.Start, config.Stop, name, syncstr)
    n, err = fo.WriteString(execstr)
    if n != len(execstr) || err != nil {
        return err
    }

    return nil
}

func read_spec(input string) (map[string]*Service, error) {
    // Read the input file.
    data, err := ioutil.ReadFile(input)
    if err != nil {
        return nil, err
    }

    // Decode our JSON specification.
    spec := make(map[string]*Service)
    buf := bytes.NewBuffer(data)
    dec := json.NewDecoder(buf)
    err = dec.Decode(&spec)
    if err != nil {
        return nil, err
    }

    // Expand globs.
    for _, config := range spec {
        new_files := make([]string, 0, 0)
        for _, file := range config.Files {
            found, err := filepath.Glob(file)
            if err == nil {
                log.Printf("Expanding %s...\n", file)
                new_files = append(new_files, found...)
                for _, new_file := range new_files {
                    log.Printf(" -> %s\n", new_file)
                }
            } else {
                log.Printf("Error matching %s: %s.\n", file, err.Error())
            }
        }

        // Update the spec.
        config.Files = new_files
    }

    return spec, nil
}

func cli_generate(input string, output string, api string) error {
    spec, err := read_spec(input)
    if err != nil {
        return err
    }

    // Generate upstart files.
    log.Printf("Found %d services.\n", len(spec))
    for service, config := range spec {
        err = make_upstart(output, service, config, api)
        if err != nil {
            return err
        }
    }

    log.Printf("Finished.")
    return nil
}

func cli_update(client *client.HiberaAPI, input string, files ...string) error {
    spec, err := read_spec(input)
    if err != nil {
        return err
    }

    if len(files) == 0 {
        for _, config := range spec {
            files = append(files, config.Files...)
        }
    }

    for service, config := range spec {
        changed := false
        for _, file := range config.Files {
            for _, localfile := range files {
                if file == localfile {
                    data, err := ioutil.ReadFile(localfile)
                    if err != nil {
                        log.Printf("Error reading %s: %s.", localfile, err.Error())
                        continue
                    }
                    _, err = client.Set(fmt.Sprintf("%s:%s", service, file), 0, data)
                    if err != nil {
                        log.Printf("Error setting %s: %s.", localfile, err.Error())
                        continue
                    }
                    changed = true
                }
            }
        }
        if changed {
            // Reset the base key.
            _, err = client.Set(service, 0, []byte(strings.Join(config.Files, "\n") + "\n"))
            if err != nil {
                log.Printf("Error setting %s: %s.", service, err.Error())
                continue
            }
        }
    }

    return nil
}
