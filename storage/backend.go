package storage

import (
    "bytes"
    "encoding/binary"
    "encoding/gob"
    "hibera/utils"
    "io"
    "io/ioutil"
    "log"
    "os"
    "path"
    "strings"
    "syscall"
)

var DefaultPath = "/var/lib/hibera"
var MaximumLogBatch = 256
var MaximumLogSize = int64(1024 * 1024)

type Val struct {
    Rev   uint64
    Value []byte
}

type Entry struct {
    Key string
    Val
}

type Update struct {
    Entry
    result chan error
}

type Backend struct {
    path string

    accepted map[string]Val
    promised map[string]uint64

    cluster *os.File
    data    *os.File
    log1    *os.File
    log2    *os.File
    cs      chan *Update
}

func OpenLocked(filename string) (*os.File, error) {
    file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return nil, err
    }
    fd := file.Fd()
    err = syscall.Flock(int(fd), syscall.LOCK_EX)
    if err != nil {
        file.Close()
        return nil, err
    }
    return file, nil
}

func NewBackend(p string) *Backend {
    if len(p) == 0 {
        p = DefaultPath
    }

    // Create the directory.
    err := os.MkdirAll(p, 0644)
    if err != nil {
        log.Print("Error initializing storage: ", err)
        return nil
    }

    // Create our backend.
    b := new(Backend)
    b.path = p
    b.cs = make(chan *Update, MaximumLogBatch)
    err = b.init()
    if err != nil {
        return nil
    }
    return b
}

func (b *Backend) init() error {
    b.accepted = make(map[string]Val)
    b.promised = make(map[string]uint64)

    // Open our files.
    cluster, err := OpenLocked(path.Join(b.path, "cluster"))
    if err != nil {
        log.Print("Error initializing cluster file: ", err)
        return err
    }
    data, err := OpenLocked(path.Join(b.path, "data"))
    if err != nil {
        log.Print("Error initializing data file: ", err)
        return err
    }
    err = b.loadFile(data)
    if err != nil {
        log.Print("Error loading data file: ", err)
        return err
    }
    log1, err := OpenLocked(path.Join(b.path, "log.1"))
    if err != nil {
        data.Close()
        log.Print("Error initializing first log file: ", err)
        return err
    }
    err = b.loadFile(log1)
    if err != nil {
        data.Close()
        log.Print("Error loading first log file: ", err)
        return err
    }
    log2, err := OpenLocked(path.Join(b.path, "log.2"))
    if err != nil {
        log1.Close()
        data.Close()
        log.Print("Error initializing second log file: ", err)
        return err
    }
    err = b.loadFile(log2)
    if err != nil {
        data.Close()
        log.Print("Error loading second log file: ", err)
        return err
    }

    // Save our files.
    b.cluster = cluster
    b.data = data
    b.log1 = log1
    b.log2 = log2

    // After our initial load do a pivot.
    b.pivotLogs()
    b.pivotLogs()

    return nil
}

func (b *Backend) SetCluster(id string) error {
    err := b.cluster.Truncate(0)
    if err != nil {
        return err
    }
    _, err = b.cluster.WriteAt([]byte(id), 0)
    if err != nil {
        return err
    }
    return nil
}

func (b *Backend) GetCluster() (string, error) {
    idbytes := make([]byte, 256, 256)
    n, err := b.cluster.ReadAt(idbytes, 0)
    if err != nil && err != io.EOF {
        return "", err
    }
    idbytes = idbytes[0:n]
    return string(idbytes[0:n]), nil
}

func serialize(output *os.File, entry *Entry) error {
    // Do the encoding.
    encoded := bytes.NewBuffer(make([]byte, 0))
    enc := gob.NewEncoder(encoded)
    err := enc.Encode(entry)
    if err != nil {
        return err
    }

    // Finish our header.
    err = binary.Write(output, binary.LittleEndian, uint32(encoded.Len()))
    if err != nil {
        return err
    }

    // Write the full buffer.
    _, err = output.Write(encoded.Bytes())
    if err != nil {
        return err
    }

    return nil
}

func deserialize(input *os.File, entry *Entry) error {
    // Read the header.
    length := uint32(0)
    err := binary.Read(input, binary.LittleEndian, &length)
    if err == io.ErrUnexpectedEOF {
        return io.EOF
    } else if err != nil {
        return err
    }

    // Read the object.
    encoded := make([]byte, length, length)
    n, err := io.ReadFull(input, encoded)
    if (err == io.EOF || err == io.ErrUnexpectedEOF) && n == int(length) {
        // Perfect. We got exactly this object.
    } else if err != nil {
        return err
    }

    // Do the decoding.
    dec := gob.NewDecoder(bytes.NewBuffer(encoded))
    err = dec.Decode(entry)
    if err != nil {
        return err
    }

    return nil
}

func (b *Backend) pivotLogs() error {

    // Remove the first log.
    err := os.Remove(path.Join(b.path, "log.1"))
    if err != nil {
        return err
    }

    // Pivot the second one to the first by name.
    err = os.Rename(path.Join(b.path, "log.2"), path.Join(b.path, "log.1"))
    if err != nil {
        return err
    }
    orig_log1 := b.log1
    b.log1 = b.log2
    orig_log1.Close()

    // Open a new second log.
    newlog2, err := OpenLocked(path.Join(b.path, "log.2"))
    if err != nil {
        return err
    }
    b.log2 = newlog2

    // Serialize our current database.
    data, err := OpenLocked(path.Join(b.path, ".data"))
    if err != nil {
        return err
    }
    for key, val := range b.accepted {
        err = serialize(data, &Entry{key, val})
        if err != nil {
            return err
        }
    }
    data.Sync()
    err = os.Rename(path.Join(b.path, ".data"), path.Join(b.path, "data"))
    if err != nil {
        return err
    }
    orig_data := b.data
    b.data = data
    orig_data.Close()

    return nil
}

func (b *Backend) loadFile(file *os.File) error {

    // Reset the pointer.
    file.Seek(0, 0)

    for {
        var entry Entry
        err := deserialize(file, &entry)
        if err == io.EOF {
            break
        } else if err != nil {
            return err
        }

        // Atomic set on our map.
        if entry.Val.Value == nil {
            delete(b.accepted, entry.Key)
        } else {
            b.accepted[entry.Key] = entry.Val
        }
    }

    return nil
}

func (b *Backend) logWriter() {

    finished := make([]*Update, 0, MaximumLogBatch)

    complete := func() {
        // Ensure we're synced.
        b.log2.Sync()

        // Notify waiters.
        for _, update := range finished {
            update.result <- nil
        }
        finished = finished[0:0]

        // Kick off a pivot if we've exceed our size.
        off, err := b.log2.Seek(0, 1)
        if err != nil && off > MaximumLogSize {
            b.pivotLogs()
        }
    }

    process := func(update *Update) {
        if update.Entry.Val.Value == nil {
            // It's a promise, try to set.
            delete(b.accepted, update.Entry.Key)
        } else {
            // Update the in-memory database.
            // NOTE: This should be atomic.
            b.accepted[update.Entry.Key] = update.Entry.Val
        }

        // Serialize the entry to the log.
        serialize(b.log2, &update.Entry)

        // Add it to our list of done.
        finished = append(finished, update)
    }

    for {
        if len(finished) == 0 {
            // Nothing in the queue?
            // Block indefinitely.
            select {
            case update := <-b.cs:
                process(update)
                break
            }

        } else if len(finished) < MaximumLogBatch {
            // Something in the queue?
            // Do a non-blocking call. If we don't
            // find anything right now, then we do
            // a flush log to complete this batch.
            select {
            case update := <-b.cs:
                process(update)
                break
            default:
                complete()
                break
            }

        } else {
            // We're at a full batch.
            // We have to do a flush of the log.
            complete()
        }
    }
}

func (b *Backend) Write(key string, value []byte, rev uint64) error {
    val := Val{rev, value}
    entry := Entry{key, val}
    update := &Update{entry, make(chan error, 1)}
    b.cs <- update
    return <-update.result
}

func (b *Backend) Read(key string) ([]byte, uint64, error) {
    val := b.accepted[key]
    return val.Value, val.Rev, nil
}

func (b *Backend) Promise(key string, rev uint64) error {
    return b.Write(key, nil, rev)
}

func (b *Backend) Delete(key string) error {
    return b.Write(key, nil, 0)
}

func (b *Backend) List() ([]string, error) {
    keys := make([]string, 0, len(b.accepted))
    for k, _ := range b.accepted {
        keys = append(keys, k)
    }
    return keys, nil
}

func (b *Backend) Clear() error {
    // Remove all files.
    os.Remove(path.Join(b.path, "log.2"))
    os.Remove(path.Join(b.path, "log.1"))
    os.Remove(path.Join(b.path, "data"))
    return b.init()
}

func (b *Backend) LoadIds(number uint) ([]string, error) {
    ids := make([]string, 0)

    // Read our current set of ids.
    iddata, err := ioutil.ReadFile(path.Join(b.path, "id"))
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
    err = ioutil.WriteFile(path.Join(b.path, "id"), buf.Bytes(), 0644)
    if err != nil {
        return nil, err
    }

    // Return our ids.
    return ids[0:number], nil
}

func (b *Backend) Run() {
    b.logWriter()
}
