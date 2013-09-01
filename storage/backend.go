package storage

import (
    "bytes"
    "encoding/binary"
    "encoding/gob"
    "fmt"
    "hibera/utils"
    "io"
    "io/ioutil"
    "log"
    "os"
    "path"
    "path/filepath"
    "sort"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
)

// The number of writes to batch prior to flushing.
// Note that this batching only happens when there is
// an active queue of writes. If one write comes along
// and no others are pending, then we will flush to disk.
var MaximumLogBatch = 1024

// Allow this maximum simultaneous Sync() calls.
var MaximumFlushes = 2

// The maximum log size.
// If this log size is hit (1Mb default) then we will do
// a full pivot of the logs and synchronize the data file.
var MaximumLogSize = int64(1024 * 1024)

type key string

type value struct {
    Data     []byte
    Metadata []byte
}

type entry struct {
    Key   key
    Value value
}

type update struct {
    entry
    result chan error
}

type Backend struct {
    logPath  string
    dataPath string

    accepted map[key]value

    data         *os.File
    data_entries uint64
    log_size     int64
    log_number   uint64

    pending chan *update
    handled uint64

    notify chan bool

    sync.Mutex
}

func (b *Backend) loadLogs() error {

    // Create the log path if it doesn't exist.
    err := os.MkdirAll(b.logPath, 0644)
    if err != nil {
        log.Print("Error initializing log: ", err)
        return err
    }

    // List all entries.
    logfiles, err := filepath.Glob(path.Join(b.logPath, "log.*"))
    if err != nil {
        log.Print("Error listing logs: ", err)
        return err
    }

    lognumbers := make([]int, 0, len(logfiles))
    for _, logfile := range logfiles {
        // Extract suffix (after log.).
        basename := filepath.Base(logfile)[4:]

        // Extract the number.
        // NOTE: This is currently limited to 32 bits just
        // to support the IntSlice below. This is somewhat
        // annoying, but if necessary it could be expanded
        // without breaking compatibilit.
        basenum, err := strconv.ParseInt(basename, 10, 32)
        if err != nil {
            // Skip unknown files.
            continue
        }

        // Save the number.
        lognumbers = append(lognumbers, int(basenum))
    }

    // Sort the logs.
    sort.Sort(sort.IntSlice(lognumbers))

    for _, basenum := range lognumbers {
        // Open the file.
        file, err := os.OpenFile(path.Join(b.logPath, fmt.Sprintf("log.%d", basenum)), os.O_RDONLY, 0)

        // Get the filesize.
        fi, err := os.Stat(file.Name())
        if err != nil {
            file.Close()
            continue
        }

        // Read all the entries.
        _, err = b.loadFile(file)
        if err != nil {
            file.Close()
            continue
        }

        // Save the highest log number.
        if uint64(basenum) > b.log_number {
            b.log_number = uint64(basenum)
        }

        // Keep going.
        b.log_size += fi.Size()
        file.Close()
    }

    return nil
}

func (b *Backend) removeLogs(limit uint64) error {

    // List all log files.
    logfiles, err := filepath.Glob(path.Join(b.logPath, "log.*"))
    if err != nil {
        log.Print("Error listing log files: ", err)
        return err
    }

    // Delete them all.
    // Unfortunately a stale logfile can have unintended
    // consequences so we have to bail. At the higher level,
    // we ensure that the data is written out before bailling,
    // because *some* log file may have been erased.
    for _, logfile := range logfiles {
        // Extract suffix (after log.).
        basename := filepath.Base(logfile)[4:]

        // Extract the number.
        basenum, err := strconv.ParseUint(basename, 10, 64)
        if err != nil {
            // Skip unknown files.
            continue
        }

        // Don't erase past where we're given.
        if basenum > limit {
            continue
        }

        // Get the filesize.
        fi, err := os.Stat(logfile)
        if err != nil {
            continue
        }

        // Try the actual remove.
        err = os.Remove(logfile)
        if err != nil {
            log.Print("Error removing log file: ", err)
            return err
        }

        // Take off the entries.
        atomic.AddInt64(&b.log_size, -fi.Size())
    }

    // Reset the log number if it's still limit.
    atomic.CompareAndSwapUint64(&b.log_number, limit, 0)

    return nil
}

func NewBackend(logPath string, dataPath string) (*Backend, error) {
    // Create our backend.
    b := new(Backend)
    b.logPath = logPath
    b.dataPath = dataPath
    b.pending = make(chan *update, MaximumLogBatch)
    b.notify = make(chan bool)
    err := b.init()
    if err != nil {
        return nil, err
    }
    return b, nil
}

func (b *Backend) init() error {
    b.Mutex.Lock()
    defer b.Mutex.Unlock()

    // Reset in-memory database.
    b.accepted = make(map[key]value)
    b.handled = uint64(0)

    // Close existing files.
    if b.data != nil {
        b.data.Close()
        b.data = nil
    }

    // Create the directories.
    err := os.MkdirAll(b.dataPath, 0644)
    if err != nil {
        log.Print("Error initializing data: ", err)
        return err
    }

    // Open our files.
    b.data, err = os.OpenFile(path.Join(b.dataPath, "data"), os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        log.Print("Error initializing data file: ", err)
        return err
    }
    b.data_entries, err = b.loadFile(b.data)
    if err != nil {
        b.data.Close()
        b.data = nil
        log.Print("Error loading data file: ", err)
        return err
    }

    // Load log files.
    err = b.loadLogs()
    if err != nil {
        b.data.Close()
        b.data = nil
        log.Print("Unable to load initial log files: ", err)
        return err
    }

    // Clean old log files.
    err = b.squashLogs()
    if err != nil {
        b.data.Close()
        b.data = nil
        log.Print("Unable to clean squash log files: ", err)
        return err
    }

    return nil
}

func serialize(output *os.File, ent *entry) error {

    // Do the encoding.
    encoded := bytes.NewBuffer(make([]byte, 0))
    enc := gob.NewEncoder(encoded)
    err := enc.Encode(ent)
    if err != nil {
        log.Print("Error encoding an entry: ", err)
        return err
    }

    // Finish our header.
    err = binary.Write(output, binary.LittleEndian, uint32(encoded.Len()))
    if err != nil {
        log.Print("Error writing entry length: ", err)
        return err
    }

    // Write the full buffer.
    _, err = output.Write(encoded.Bytes())
    if err != nil {
        log.Print("Error writing full entry: ", err)
        return err
    }

    return nil
}

func deserialize(input *os.File, ent *entry) error {

    // Read the header.
    length := uint32(0)
    err := binary.Read(input, binary.LittleEndian, &length)
    if err == io.ErrUnexpectedEOF {
        return io.EOF
    } else if err != nil {
        if err != io.EOF {
            log.Print("Error reading entry header: ", err)
        }
        return err
    }

    // Read the object.
    encoded := make([]byte, length, length)
    n, err := io.ReadFull(input, encoded)
    if (err == io.EOF || err == io.ErrUnexpectedEOF) && n == int(length) {
        // Perfect. We got exactly this object.
    } else if err != nil {
        log.Print("Error reading full entry: ", err)
        return err
    }

    // Do the decoding.
    dec := gob.NewDecoder(bytes.NewBuffer(encoded))
    err = dec.Decode(ent)
    if err != nil {
        log.Print("Error decoding an entry: ", err)
        return err
    }

    return nil
}

func (b *Backend) safeSquash(limit uint64) error {
    b.Mutex.Lock()
    defer b.Mutex.Unlock()

    if atomic.LoadInt64(&b.log_size) > MaximumLogSize {
        return b.squashLogsUntil(limit)
    }

    return nil
}

func (b *Backend) squashLogsUntil(limit uint64) error {

    // Serialize our current database.
    data, err := ioutil.TempFile(b.dataPath, "data")
    if err != nil {
        log.Print("Error opening new data: ", err)
        return err
    }
    data_entries := uint64(0)
    for id, val := range b.accepted {
        err = serialize(data, &entry{id, val})
        if err != nil {
            log.Print("Error serializing data: ", err)
            return err
        }
        data_entries += 1
    }
    data.Sync()
    err = os.Rename(data.Name(), path.Join(b.dataPath, "data"))
    if err != nil {
        log.Print("Error renaming data: ", err)
        return err
    }
    b.data.Close()
    b.data = data
    b.data_entries = data_entries

    // Remove old log files.
    return b.removeLogs(limit)
}

func (b *Backend) squashLogs() error {
    // Grab the current highest log.
    limit := atomic.LoadUint64(&b.log_number)
    return b.squashLogsUntil(limit)
}

func (b *Backend) loadFile(file *os.File) (uint64, error) {

    // Count entries.
    entries := uint64(0)

    // Reset the pointer.
    file.Seek(0, 0)

    for {
        var ent entry
        err := deserialize(file, &ent)
        if err == io.EOF {
            break
        } else if err != nil {
            return entries, err
        }

        entries += 1

        // Atomic set on our map.
        if ent.Value.Data == nil && ent.Value.Metadata == nil {
            delete(b.accepted, ent.Key)
        } else {
            b.accepted[ent.Key] = ent.Value
        }
    }

    return entries, nil
}

func (b *Backend) logWriter() error {

    var log_number uint64
    var finished []*update
    var logfile *os.File

    // Allow a limited number of flushers.
    flush_chan := make(chan bool, MaximumFlushes)
    for i := 0; i < MaximumFlushes; i += 1 {
        flush_chan <- true
    }

    // Allow a single squash at any moment.
    squash_chan := make(chan error, 1)
    squash_chan <- nil

    reset := func() error {
        var err error
        log_number = atomic.AddUint64(&b.log_number, 1)
        finished = make([]*update, 0, MaximumLogBatch)
        logfile, err = os.OpenFile(
            path.Join(b.logPath, fmt.Sprintf("log.%d", log_number)),
            os.O_WRONLY|os.O_CREATE, 0644)
        return err
    }

    complete := func() {

        flush := func(log_number uint64, logfile *os.File, finished []*update) {
            // Ensure we're synced.
            logfile.Sync()
            logfile.Close()

            // Allow other flushes.
            flush_chan <- true

            // Notify waiters.
            for _, upd := range finished {
                upd.result <- nil
            }

            // Kick off a pivot if we've exceed our size.
            fi, err := os.Stat(logfile.Name())
            if err == nil {
                atomic.AddInt64(&b.log_size, fi.Size())
            }

            // Allow only one squash at a time.
            last_err := <-squash_chan
            if last_err == nil {
                squash_chan<- b.safeSquash(log_number)
            } else {
                squash_chan<- last_err
            }
        }

        // Send this batch to be flushed.
        go flush(log_number, logfile, finished)

        // Reset our buffers.
        logfile = nil
        finished = nil
    }

    process := func(upd *update) bool {
        // Check for terminal.
        if upd == nil {
            return true
        }

        if upd.entry.Value.Data == nil && upd.entry.Value.Metadata == nil {
            // It's a promise, try to set.
            delete(b.accepted, upd.entry.Key)
        } else {
            // Update the in-memory database.
            // NOTE: This should be atomic.
            b.accepted[upd.entry.Key] = upd.entry.Value
        }

        // Serialize the entry to the log.
        serialize(logfile, &upd.entry)

        // Add it to our list of done.
        finished = append(finished, upd)

        // Keep going.
        atomic.AddUint64(&b.handled, 1)
        return false
    }

    for {

        if finished == nil {

            // Nothing in the queue?
            // Block indefinitely.
            select {
            case upd := <-b.pending:

                // Initialize the log.
                err := reset()
                if err != nil {
                    log.Print("Error opening log: ", err)
                }

                // Process normally.
                if process(upd) {
                    return <-squash_chan
                }
                break
            }

        } else {

            if len(finished) < MaximumLogBatch {
                // Something in the queue?
                // Do a non-blocking call. If we don't
                // find anything right now, then we do
                // a flush log to complete this batch.
                select {
                case upd := <-b.pending:
                    if process(upd) {
                        return <-squash_chan
                    }
                    break
                case <-flush_chan:
                    complete()
                    break
                }

            } else {
                // We're at a full batch.
                // We have to do a flush of the log.
                <-flush_chan
                complete()
            }
        }
    }
}

func (b *Backend) Write(id string, data []byte, metadata []byte) error {
    ent := entry{key(id), value{data, metadata}}
    upd := &update{ent, make(chan error, 1)}
    b.pending <- upd
    return <-upd.result
}

func (b *Backend) Read(id string) ([]byte, []byte, error) {
    ent := b.accepted[key(id)]
    return ent.Data, ent.Metadata, nil
}

func (b *Backend) Delete(id string) error {
    return b.Write(id, nil, nil)
}

func (b *Backend) List() ([]string, error) {
    keys := make([]string, 0, 0)
    for id, _ := range b.accepted {
        keys = append(keys, string(id))
    }
    return keys, nil
}

func (b *Backend) Clear() error {
    // Remove all files.
    os.Remove(path.Join(b.logPath, "log.2"))
    os.Remove(path.Join(b.logPath, "log.1"))
    os.Remove(path.Join(b.dataPath, "data"))
    os.Remove(path.Join(b.dataPath, "id"))
    return b.init()
}

func (b *Backend) LoadIds(number uint) ([]string, error) {
    ids := make([]string, 0)

    // Read our current set of ids.
    iddata, err := ioutil.ReadFile(path.Join(b.dataPath, "id"))
    if err != nil &&
        iddata != nil ||
        len(iddata) > 0 {
        ids = strings.Split(string(iddata), "\n")
    }

    // Supplement.
    for {
        if len(ids) >= int(number) {
            break
        }
        uuid, err := utils.Uuid()
        if err != nil {
            return nil, err
        }
        ids = append(ids, uuid)
    }

    // Write out the result.
    buf := new(bytes.Buffer)
    for _, id := range ids {
        buf.WriteString(id)
        buf.WriteString("\n")
    }
    err = ioutil.WriteFile(path.Join(b.dataPath, "id"), buf.Bytes(), 0644)
    if err != nil {
        return nil, err
    }

    // Return our ids.
    return ids[0:number], nil
}

func (b *Backend) Run() error {
    err := b.logWriter()
    b.notify <- true
    return err
}

func (b *Backend) Stop() {
    b.pending <- nil
    <-b.notify
}
