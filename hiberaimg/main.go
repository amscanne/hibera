package main

import (
    "bytes"
    "crypto/sha1"
    "encoding/json"
    "errors"
    "fmt"
    "hibera/cli"
    "hibera/core"
    "hibera/utils"
    "io"
    "log"
    "os"
    "path"
    "strconv"
    "syscall"
)

var size = cli.Flags.Uint("size", 1024*1024, "Chunk size.")
var workers = cli.Flags.Uint("workers", 16, "Simultaneous workers.")
var cacheSize = cli.Flags.Int64("cacheSize", 10*1024*1024*1024, "Cache size.")
var cacheDir = cli.Flags.String("cacheDir", path.Join(os.Getenv("HOME"), ".hiberaimg"), "Cache location.")

var cliInfo = cli.Cli{
    "Hibera cluster image storage tool.",
    map[string]cli.Command{
        "upload": cli.Command{
            "Upload a given file.",
            "",
            []string{"key", "file"},
            []string{"size"},
            false,
        },
        "download": cli.Command{
            "Download the given file.",
            "",
            []string{"key", "file"},
            []string{"workers", "cacheSize", "cacheDir"},
            false,
        },
        "remove": cli.Command{
            "Remove the given file.",
            "",
            []string{"key"},
            []string{"workers"},
            false,
        },
    },
    cli.Options,
}

type chunk struct {
    Hash   string `json:"hash"`
    Offset int64  `json:"offset"`
    Size   int64  `json:"size"`
    res    chan error
}

const (
    o_upload = iota
    o_download
    o_remove
)

func setData(file *os.File, c *chunk, data []byte) error {
    if c.Size != int64(len(data)) {
        return errors.New("mismatched size")
    }
    for n := 0; n < int(c.Size); {
        cur, err := syscall.Pwrite(int(file.Fd()), data[n:], c.Offset+int64(n))
        if int64(n+cur) == c.Size {
            // We're set.
            break
        } else if err != nil {
            // Not so okay.
            return err
        }
        n += cur
    }
    return nil
}

func getData(file *os.File, c *chunk) ([]byte, error) {
    buffer := make([]byte, c.Size, c.Size)
    for n := 0; n < int(c.Size); {
        cur, err := syscall.Pread(int(file.Fd()), buffer[n:], c.Offset+int64(n))
        if (err == io.EOF || err == io.ErrUnexpectedEOF) && (n+cur) == int(c.Size) {
            // We're still okay, so long as we've got what we need.
            break
        } else if err != nil {
            // Not so okay.
            return nil, err
        } else if cur == 0 {
            // A genuine EOF!
            return nil, io.ErrUnexpectedEOF
        }
        n += cur
    }
    return buffer, nil
}

func worker(c *cache, num int, file *os.File, op int, chunks chan *chunk) {

    // Each worker gets their own client.
    client := cli.Client()
    name := fmt.Sprintf("worker-%d", num)

    var err error
    var work *chunk
    for {
        if work == nil {
            work = <-chunks
            if work == nil {
                break
            }
        }

        // Join the sync group.
        var refs uint64
        rev := core.NoRevision
        refs_key := fmt.Sprintf("img/refs/%s", work.Hash)
        data_key := fmt.Sprintf("img/data/%s", work.Hash)

        if op == o_upload || op == o_remove {
            _, _, err = client.SyncJoin(refs_key, name, 1, 0)
            if err != nil {
                // Retry.
                log.Printf("unable to join %s: %s\n", refs_key, err.Error())
                continue
            }

            // Get the current ref count.
            var value []byte
            value, rev, err = client.DataGet(refs_key, core.NoRevision, 1)
            if err != nil {
                // Retry.
                client.SyncLeave(refs_key, name)
                log.Printf("unable to get revision for %s: %s\n", refs_key, err.Error())
                continue
            }
            if !rev.IsZero() {
                refs, err = strconv.ParseUint(string(value), 10, 64)
                if err != nil {
                    // No coming back from this one.
                    client.SyncLeave(refs_key, name)
                    log.Printf("unable to parse revision for %s: %s\n", refs_key, err.Error())
                    break
                }
            }
        }

        utils.Debug("chunk %s %d %d -> revision %s, refs %d\n",
            work.Hash, work.Offset, work.Size, rev.String(), refs)

        if op == o_upload {
            if refs == 0 {
                // Upload the contents (if necessary).
                utils.Debug("reading %s @ %d (%d bytes)...\n", file.Name(), work.Offset, work.Size)
                data, err := getData(file, work)
                if err != nil {
                    // No coming back from this one.
                    client.SyncLeave(refs_key, name)
                    log.Printf("unable to read %s: %s\n", file.Name(), err.Error())
                    break
                }
                ok, _, err := client.DataSet(data_key, core.NoRevision, data)
                if !ok || err != nil {
                    // Retry.
                    client.SyncLeave(refs_key, name)
                    if err != nil {
                        log.Printf("unable to upload %s: %s\n", data_key, err.Error())
                    }
                    continue
                }
            }

            // Bump the ref count.
            ok, new_rev, err := client.DataSet(refs_key, rev.Next(), []byte(strconv.FormatUint(refs+1, 10)))
            if !ok || err != nil || !new_rev.Equals(rev.Next()) {
                // Whoops. Retry.
                client.SyncLeave(refs_key, name)
                if err != nil {
                    log.Printf("unable to bump %s: %s\n", refs_key, err.Error())
                }
                continue
            }

        } else if op == o_download {

            data, err := c.start(work.Hash)
            if err != nil {
                log.Printf("error fetching from cache: %s\n", err.Error())
            }

            if data == nil {
                // Grab the data.
                utils.Debug("downloading %s @ %d (%d bytes)...\n", data_key, work.Offset, work.Size)
                data, rev, err := client.DataGet(data_key, core.NoRevision, 0)
                if err != nil {
                    // Retry.
                    log.Printf("unable to download %s: %s\n", data_key, err.Error())
                    c.cancel(work.Hash)
                    continue
                }
                if rev == core.NoRevision {
                    // No file. No recovery here.
                    err = errors.New("not available")
                    c.cancel(work.Hash)
                    break
                }

                // Set in the cache.
                err = c.save(work.Hash, data)
                if err != nil {
                    log.Printf("error saving to cache: %s\n", err.Error())
                }
            } else {
                utils.Debug("found %s @ %d (%d bytes) in cache.\n", data_key, work.Offset, work.Size)
            }

            utils.Debug("writing %s @ %d (%d bytes)...\n", file.Name(), work.Offset, work.Size)
            err = setData(file, work, data)
            if err != nil {
                // No coming back from this one.
                log.Printf("unable to write %s: %s\n", file.Name(), err.Error())
                break
            }

        } else if op == o_remove {

            // Drop the ref count.
            ok, new_rev, err := client.DataSet(refs_key, rev.Next(), []byte(strconv.FormatUint(refs-1, 10)))
            if !ok || err != nil || !new_rev.Equals(rev.Next()) {
                // Whoops. Retry.
                client.SyncLeave(refs_key, name)
                if err != nil {
                    log.Printf("unable to drop %s: %s\n", refs_key, err.Error())
                }
                continue
            }
            // Update our revision (for the scrub below).
            rev = rev.Next()

            // If we're at 0, remove the data and key.
            if (refs - 1) == 0 {
                ok, _, err := client.DataRemove(data_key, core.NoRevision)

                if !ok || err != nil {
                    // Ack. We've already changed the ref count.
                    // We can't really do that again. We need to
                    // abdanon this here for now (and maybe we
                    // could have a separate cleanup later).
                    client.SyncLeave(refs_key, name)
                    if err != nil {
                        log.Printf("unable to scrub %s: %s\n", data_key, err.Error())
                    }
                    work.res <- err
                    work = nil
                    continue
                }

                // Remove the refs key too.
                ok, new_rev, err := client.DataRemove(refs_key, rev.Next())
                if !ok || err != nil || !new_rev.Equals(rev.Next()) {
                    // Crap. Oh well. It's still zero, so someone
                    // will be able to just clean it up later.
                    client.SyncLeave(refs_key, name)
                    if err != nil {
                        log.Printf("unable to scrub %s: %s\n", refs_key, err.Error())
                    }
                    work.res <- err
                    work = nil
                    continue
                }
            }
        }

        // We're done.
        if op == o_upload || op == o_remove {
            _, err = client.SyncLeave(refs_key, name)
            if err != nil {
                log.Printf("unable to leave  %s: %s\n", refs_key, err.Error())
            }
        }
        work.res <- nil
        work = nil
    }
}

func swapChunks(key string, write bool, data []byte) (func() *chunk, int64, error) {
    chunks := make([]*chunk, 0, 0)
    chunkno := 0
    total := int64(0)
    iter := func() *chunk {
        if chunkno >= len(chunks) {
            return nil
        }
        chunkno += 1
        return chunks[chunkno-1]
    }

    // Get via the client.
    client := cli.Client()

    var rev core.Revision
    for {
        var value []byte
        var err error
        if data == nil || !rev.IsZero() {
            utils.Debug("fetching info for %s...", key)

            // If we're just getting, or there was something there on a set / delete.
            value, rev, err = client.DataGet(fmt.Sprintf("img/info/%s", key), core.NoRevision, 0)
            if err != nil {
                log.Printf("unable to fetch info for %s: %s\n", key, err.Error())
                return iter, total, err
            }
            utils.Debug("info %s -> revision %s\n", key, rev.String())

            // Decode our nodes and tokens.
            buf := bytes.NewBuffer(value)
            dec := json.NewDecoder(buf)
            err = dec.Decode(&chunks)
            if err != nil {
                log.Printf("unable to decode info for %s: %s\n", key, err.Error())
                return iter, total, err
            }

            // Ensure that the channels are available.
            for _, chunk := range chunks {
                chunk.res = make(chan error, 1)
            }

            // Ensure the total is correct.
            if len(chunks) > 0 {
                total = chunks[len(chunks)-1].Offset + chunks[len(chunks)-1].Size
            }
        }

        if write {
            if data == nil {
                utils.Debug("deleting %s...", key)

                // If we're deleting...
                ok, new_rev, err := client.DataRemove(fmt.Sprintf("img/info/%s", key), rev.Next())
                if err != nil {
                    log.Printf("unable to remove %s: %s\n", key, err.Error())
                    return iter, total, err
                }
                if !ok || !new_rev.Equals(rev.Next()) {
                    // Retry.
                    rev = new_rev
                    continue
                }

                utils.Debug("done -> revision %s\n", new_rev.String())

            } else {
                utils.Debug("swapping %s...", key)

                // Or just swapping...
                ok, new_rev, err := client.DataSet(fmt.Sprintf("img/info/%s", key), rev.Next(), data)
                if err != nil {
                    log.Printf("unable to set %s: %s\n", key, err.Error())
                    return iter, total, err
                }
                if !ok || !new_rev.Equals(rev.Next()) {
                    // Retry.
                    rev = new_rev
                    continue
                }

                utils.Debug("done -> revision %s\n", new_rev.String())
            }
        }

        break
    }

    return iter, total, nil
}

func getChunks(key string) (func() *chunk, int64, error) {
    return swapChunks(key, false, nil)
}

func setChunks(key string, chunks []*chunk) (func() *chunk, int64, error) {
    // Encode our info object as JSON.
    buf := new(bytes.Buffer)
    enc := json.NewEncoder(buf)
    err := enc.Encode(&chunks)
    if err != nil {
        noop := func() *chunk {
            return nil
        }
        return noop, int64(0), err
    }

    return swapChunks(key, true, buf.Bytes())
}

func delChunks(key string) (func() *chunk, int64, error) {
    return swapChunks(key, true, nil)
}

func do_op(c *cache, file *os.File, total int64, op int, next func() *chunk, workers uint) error {
    var err error
    chunks := make(chan *chunk, workers)

    var status string
    if op == o_upload {
        status = "upload"
    } else if op == o_download {
        status = "download"
    } else if op == o_remove {
        status = "remove"
    }
    utils.Debug("--- starting %s ---\n", status)

    // Start workers.
    for i := 0; i < int(workers); i += 1 {
        go worker(c, i, file, op, chunks)
    }

    // Setup for generating chunks.
    ui_updates := make(chan int64)
    ui_stop := make(chan bool)
    ui_done := make(chan bool)
    count := 0
    all_res := make(chan error)

    // Our 'UI' thread.
    current := int64(0)
    go func() {
        for {
            select {
            case value := <-ui_updates:
                current += value
                percent := float64(current) * 100.0 / float64(total)
                output := fmt.Sprintf("\r%s [% 6.2f%%] % 10d/%d bytes", status, percent, current, total)
                os.Stderr.Write([]byte(output))
                break
            case <-ui_stop:
                os.Stderr.Write([]byte("\n"))
                ui_done <- true
                return
            }
        }
    }()

    // Generate all chunks.
    for {
        chunk := next()
        if chunk == nil {
            break
        }
        chunks <- chunk
        count += 1

        go func() {
            err := <-chunk.res
            ui_updates <- chunk.Size
            all_res <- err
        }()
    }

    // Wait for all results.
    for i := 0; i < count; i += 1 {
        this_err := <-all_res
        if err == nil && this_err != nil {
            err = this_err
        }
    }

    // Put the sentinels.
    for i := 0; i < int(workers); i += 1 {
        chunks <- nil
    }

    // Wait for the 'UI'.
    ui_stop <- true
    <-ui_done
    utils.Debug("--- finished %s ---\n", status)
    return err
}

func cli_upload(c *cache, key string, file string, size uint, workers uint) error {
    // Open the file.
    open_file, err := os.OpenFile(file, os.O_RDONLY, 0)
    if err != nil {
        return err
    }
    defer open_file.Close()

    // Generate all of our chunks.
    offset := int64(0)
    chunks := make([]*chunk, 0, 0)
    piece := make([]byte, size, size)
    next := func() *chunk {
        // Compute the next rolling hash chunk.
        n, err := io.ReadFull(open_file, piece)
        if err == io.EOF {
            return nil
        }
        if err != nil && err != io.ErrUnexpectedEOF {
        }

        // Compute the hash.
        hasher := sha1.New()
        _, err = hasher.Write(piece[:n])
        if err != nil {
            return nil
        }

        // Save the chunk.
        cur := &chunk{fmt.Sprintf("%x", hasher.Sum(nil)), offset, int64(n), make(chan error, 1)}
        chunks = append(chunks, cur)

        // Emit the chunk.
        offset += int64(n)
        return cur
    }

    // Get the total size.
    info, err := open_file.Stat()
    if err != nil {
        return err
    }

    // Do the heavy lifting.
    err = do_op(c, open_file, info.Size(), o_upload, next, workers)
    if err != nil {
        return err
    }

    // Replace the hashes.
    next, total, err := setChunks(key, chunks)

    // Remove the old key, if there was one.
    return do_op(nil, nil, total, o_remove, next, workers)
}

func cli_download(c *cache, key string, file string, workers uint) error {
    // Open the file.
    open_file, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
    if err != nil {
        return err
    }
    defer open_file.Close()

    // Grab our hashes.
    next, total, err := getChunks(key)
    if err != nil {
        return err
    }

    return do_op(c, open_file, total, o_download, next, workers)
}

func cli_remove(key string, workers uint) error {
    // Grab (and remove) our hashes.
    next, total, err := delChunks(key)
    if err != nil {
        return err
    }

    // Do the heavy lifting (no file necessary).
    return do_op(nil, nil, total, o_remove, next, workers)
}

func do_cli(command string, args []string) error {

    cache, err := newCache(*cacheSize, *cacheDir)
    if err != nil {
        return err
    }

    switch command {
    case "upload":
        return cli_upload(cache, args[0], args[1], *size, *workers)
    case "download":
        return cli_download(cache, args[0], args[1], *workers)
    case "remove":
        return cli_remove(args[0], *workers)
    }

    return nil
}

func main() {
    cli.Main(cliInfo, do_cli)
}
