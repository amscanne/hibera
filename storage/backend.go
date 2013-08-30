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

var MaximumLogBatch = 256
var MaximumLogSize = int64(1024 * 1024)

type key string

type value struct {
    data []byte
    metadata []byte
}

type entry struct {
    key
    value
}

type update struct {
    entry
    result chan error
}

type Backend struct {
    path string

    accepted map[key]value

    data *os.File
    log1 *os.File
    log2 *os.File

    cs chan *update
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

func NewBackend(p string) (*Backend, error) {
    // Create the directory.
    err := os.MkdirAll(p, 0644)
    if err != nil {
        log.Print("Error initializing storage: ", err)
        return nil, err
    }

    // Create our backend.
    b := new(Backend)
    b.path = p
    b.cs = make(chan *update, MaximumLogBatch)
    err = b.init()
    if err != nil {
        return nil, err
    }
    return b, nil
}

func (b *Backend) init() error {
    b.accepted = make(map[key]value)

    // Open our files.
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
    b.data = data
    b.log1 = log1
    b.log2 = log2

    // After our initial load do a pivot.
    b.pivotLogs()
    b.pivotLogs()

    return nil
}

func serialize(output *os.File, ent *entry) error {

    // Do the encoding.
    encoded := bytes.NewBuffer(make([]byte, 0))
    enc := gob.NewEncoder(encoded)
    err := enc.Encode(ent)
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

func deserialize(input *os.File, ent *entry) error {

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
    err = dec.Decode(ent)
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
    for id, val := range b.accepted {
        err = serialize(data, &entry{id, val})
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
        var ent entry
        err := deserialize(file, &ent)
        if err == io.EOF {
            break
        } else if err != nil {
            return err
        }

        // Atomic set on our map.
        if ent.value.data == nil && ent.value.metadata == nil {
            delete(b.accepted, ent.key)
        } else {
            b.accepted[ent.key] = ent.value
        }
    }

    return nil
}

func (b *Backend) logWriter() {

    finished := make([]*update, 0, MaximumLogBatch)

    complete := func() {
        // Ensure we're synced.
        b.log2.Sync()

        // Notify waiters.
        for _, upd := range finished {
            upd.result <- nil
        }
        finished = finished[0:0]

        // Kick off a pivot if we've exceed our size.
        off, err := b.log2.Seek(0, 1)
        if err != nil && off > MaximumLogSize {
            b.pivotLogs()
        }
    }

    process := func(upd *update) {
        if upd.entry.value.data == nil && upd.entry.value.metadata == nil {
            // It's a promise, try to set.
            delete(b.accepted, upd.entry.key)
        } else {
            // Update the in-memory database.
            // NOTE: This should be atomic.
            b.accepted[upd.entry.key] = upd.entry.value
        }

        // Serialize the entry to the log.
        serialize(b.log2, &upd.entry)

        // Add it to our list of done.
        finished = append(finished, upd)
    }

    for {
        if len(finished) == 0 {
            // Nothing in the queue?
            // Block indefinitely.
            select {
            case upd := <-b.cs:
                process(upd)
                break
            }

        } else if len(finished) < MaximumLogBatch {
            // Something in the queue?
            // Do a non-blocking call. If we don't
            // find anything right now, then we do
            // a flush log to complete this batch.
            select {
            case upd := <-b.cs:
                process(upd)
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

func (b *Backend) Write(id string, data []byte, metadata []byte) error {
    ent := entry{key(id), value{data, metadata}}
    upd := &update{ent, make(chan error, 1)}
    b.cs <- upd
    return <-upd.result
}

func (b *Backend) Read(id string) ([]byte, []byte, error) {
    ent := b.accepted[key(id)]
    return ent.data, ent.metadata, nil
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
