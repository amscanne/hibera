package storage

import (
    "io"
    "log"
    "os"
    "sync"
    "sync/atomic"
)

type chunk struct {
    offset uint64
    length uint64
}

type logFile struct {
    file *os.File
    sync.Mutex

    chunks []chunk

    refs    int32
    number  uint64
    entries int64
}

type logRecord struct {
    log    *logFile
    offset uint64
}

func OpenLog(path string, number uint64) (*logFile, error) {
    log := new(logFile)
    log.number = number
    log.entries = int64(0)
    logfile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return nil, err
    }
    log.file = logfile
    log.refs = int32(1)
    return log, nil
}

func (l *logFile) Size() (uint64, error) {
    // Get the filesize.
    fi, err := os.Stat(l.file.Name())
    if err != nil {
        return uint64(0), err
    }
    return uint64(fi.Size()), err
}

func (l *logFile) NewRecord(offset uint64) *logRecord {
    // Add a reference and return a record.
    atomic.AddInt32(&l.refs, 1)
    return &logRecord{l, offset}
}

func (l *logFile) Sync() {
    l.file.Sync()
}

func (l *logFile) Write(ent *entry) (*logRecord, error) {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Find a free chunk.
    var offset int64
    length := ent.Usage()
    remaining := uint64(0)
    found := false

    var index int
    var chk chunk
    for index, chk = range l.chunks {
        if chk.length >= length &&
            ((chk.length-length) == 0 ||
                (chk.length-length) >= 4) {
            found = true
            offset = int64(chk.offset)
            remaining = (chk.length - length)
            break
        }
    }

    if found {
        // Seek to the given offset.
        var err error
        offset, err = l.file.Seek(offset, 0)
        if err != nil {
            return nil, err
        }

        // Update our chunk list.
        if index != (len(l.chunks) - 1) {
            // Move the last element into this position.
            l.chunks[index] = l.chunks[len(l.chunks)-1]
        }
        // Trim the array by length one.
        l.chunks = l.chunks[:len(l.chunks)-1]

    } else {
        // Seek to the end of the file.
        var err error
        offset, err = l.file.Seek(0, 2)
        if err != nil {
            return nil, err
        }
    }

    // Serialize the entry.
    err := serialize(l.file, ent)
    if err != nil {
        return nil, err
    }
    atomic.AddInt64(&l.entries, 1)

    // Mark any remaining space as still free,
    // and add the chunk to our list of chunks.
    if remaining > 0 {
        clear(l.file, remaining)
        l.chunks = append(l.chunks, chunk{uint64(offset) + length, remaining})
    }

    return l.NewRecord(uint64(offset)), nil
}

func (l *logFile) Close() {
    if atomic.AddInt32(&l.refs, -1) == 0 {
        l.file.Close()
    }
}

func (l *logRecord) Read(ent *entry) (uint64, error) {
    l.log.Mutex.Lock()
    defer l.log.Mutex.Unlock()

    // Seek to the appropriate offset.
    _, err := l.log.file.Seek(int64(l.offset), 0)
    if err != nil {
        return uint64(0), err
    }

    // Deserialize the record.
    _, free_space, err := deserialize(l.log.file, ent)
    return free_space, err
}

func (l *logRecord) Delete() error {

    // Read the original record.
    var ent entry
    free_space, err := l.Read(&ent)
    if err != nil || free_space > 0 {
        return err
    }

    l.log.Mutex.Lock()
    defer l.log.Mutex.Unlock()

    atomic.AddInt64(&l.log.entries, -1)

    // Merge it with any existing chunks by
    // popping them out of the list and keep going.
    current := chunk{l.offset, ent.Usage()}

    for {
        merged := false

        // Find a potential merge.
        var index int
        var other chunk
        for index, other = range l.log.chunks {
            if other.offset+other.length == current.offset {
                // Update our current chunk.
                current.offset = other.offset
                current.length = other.length + current.length
                merged = true
                break
            }
            if current.offset+current.length == other.offset {
                // Update our current chunk.
                current.length = current.length + other.length
                merged = true
                break
            }
        }

        if merged {
            // Remove the last element from our list of chunks.
            if index < len(l.log.chunks)-1 {
                l.log.chunks[index] = l.log.chunks[len(l.log.chunks)-1]
            }
            l.log.chunks = l.log.chunks[:len(l.log.chunks)-1]
            continue
        } else {
            // No merge found, stop.
            break
        }
    }

    // Seek to the appropriate offset.
    _, err = l.log.file.Seek(int64(current.offset), 0)
    if err != nil {
        return err
    }

    // Clear our the record in the underlying file.
    err = clear(l.log.file, current.length)
    if err != nil {
        return err
    }

    // Add in our newly merged chunk.
    l.log.chunks = append(l.log.chunks, current)

    // Drop the reference.
    l.Discard()
    return nil
}

func (l *logRecord) Copy(output *logFile) (bool, error) {
    // Check if it makes sense.
    if l.log == output {
        return false, nil
    }

    // Read the current record.
    var ent entry
    free_space, err := l.Read(&ent)
    if free_space != 0 || err != nil {
        return false, err
    }

    // Create a new record.
    out_rec, err := output.Write(&ent)
    if err != nil {
        return false, err
    }

    l.log.Mutex.Lock()
    out_rec.log.Mutex.Lock()
    defer l.log.Mutex.Unlock()
    defer out_rec.log.Mutex.Unlock()

    // Swap the bits.
    orig_log := l.log
    l.log = out_rec.log
    l.offset = out_rec.offset
    orig_log.Close()

    return true, nil
}

func (l *logRecord) Discard() {
    // Drop our reference.
    l.log.Close()
}

func (l *logFile) Load() ([]*logRecord, error) {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Entries to return.
    l.entries = int64(0)
    records := make([]*logRecord, 0)

    // Reset the pointer.
    _, err := l.file.Seek(0, 0)
    if err != nil {
        return nil, err
    }

    // Reset our chunks.
    l.chunks = make([]chunk, 0)

    for {
        var ent entry
        _, free_space, err := deserialize(l.file, &ent)
        if err == io.EOF {
            break
        } else if err != nil {
            return records, err
        }

        // Get the current offset.
        offset, err := l.file.Seek(1, 0)
        if err != nil {
            log.Print("Error reading offset: ", err)
            continue
        }

        if free_space != 0 {
            // Save it as a free chunk.
            l.chunks = append(l.chunks, chunk{uint64(offset), free_space})
            continue
        }

        // Return the read entry.
        atomic.AddInt64(&l.entries, 1)
        records = append(records, l.NewRecord(uint64(offset)))
    }

    return records, nil
}
