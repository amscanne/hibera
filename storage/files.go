package storage

import (
    "hibera/utils"
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

    start  int64
    refs   int32
    number uint64
}

type logRecord struct {
    log *logFile
    sync.Mutex

    offset uint64
}

func OpenLog(path string, number uint64, create bool) (*logFile, error) {
    l := new(logFile)
    l.number = number
    var logfile *os.File
    var offset int64
    var err error
    if create {
        logfile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
        if err != nil {
            return nil, err
        }
        offset, err = writeMagic(logfile)
        if err != nil {
            logfile.Close()
            return nil, err
        }
    } else {
        logfile, err = os.OpenFile(path, os.O_RDONLY, 0)
        if err != nil {
            return nil, err
        }
        offset, err = readMagic(logfile)
        if err != nil {
            logfile.Close()
            return nil, err
        }
    }
    l.start = offset
    l.file = logfile
    l.refs = int32(1)
    return l, nil
}

func ReadLog(path string, number uint64) (*logFile, error) {
    return OpenLog(path, number, false)
}

func (l *logFile) NewRecord(offset uint64) *logRecord {
    // Add a reference and return a record.
    atomic.AddInt32(&l.refs, 1)

    var lr logRecord
    lr.log = l
    lr.offset = offset
    return &lr
}

func (l *logFile) Sync() {
    l.file.Sync()
}

func (l *logFile) Write(ent *entry) (*logRecord, error) {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Find a free chunk.
    var offset int64
    required := usage(ent)
    min_required := usage(nil)
    remaining := uint64(0)
    found := false

    var index int
    var chk chunk
    for index, chk = range l.chunks {
        if chk.length >= required &&
            ((chk.length-required) == 0 ||
                (chk.length-required) >= min_required) {
            found = true
            offset = int64(chk.offset)
            remaining = (chk.length - required)
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

    // Mark any remaining space as still free,
    // and add the chunk to our list of chunks.
    if remaining > 0 {
        clear(l.file, remaining)
        l.chunks = append(l.chunks, chunk{uint64(offset) + required, remaining})
    }

    return l.NewRecord(uint64(offset)), nil
}

func (l *logFile) Close() {
    if atomic.AddInt32(&l.refs, -1) == 0 {
        l.file.Close()
    }
}

func (l *logFile) Open() {
    atomic.AddInt32(&l.refs, 1)
}

func (l *logRecord) ReadUnsafe(ent *entry) (uint64, error) {
    // Lock the file to guard for the seek().
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

func (l *logRecord) Read(ent *entry) (uint64, error) {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()
    return l.ReadUnsafe(ent)
}

func (l *logRecord) Delete() error {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Read the original record.
    var ent entry
    free_space, err := l.ReadUnsafe(&ent)
    if err != nil || free_space > 0 {
        return err
    }

    // Protect access to the file via seek().
    // (and the scanning of the available chunks).
    l.log.Mutex.Lock()
    defer l.log.Mutex.Unlock()

    // Merge it with any existing chunks by
    // popping them out of the list and keep going.
    current := chunk{l.offset, usage(&ent)}

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

func (l *logRecord) Copy(output *logFile) (*entry, error) {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Read the current record.
    var ent entry
    free_space, err := l.ReadUnsafe(&ent)
    if free_space != 0 || err != nil {
        return nil, err
    }

    // Create a new record.
    out_rec, err := output.Write(&ent)
    if err != nil {
        return &ent, err
    }

    // Swap the bits.
    orig_log := l.log
    l.log = out_rec.log
    l.offset = out_rec.offset

    // Discard the original log reference.
    orig_log.Close()

    return &ent, nil
}

func (l *logRecord) Discard() {
    // Drop our reference.
    l.log.Close()
}

func (l *logRecord) Grab() {
    // Grab another reference.
    l.log.Open()
}

func (l *logFile) Load() ([]*logRecord, error) {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Entries to return.
    records := make([]*logRecord, 0)

    // Reset the pointer.
    _, err := l.file.Seek(l.start, 0)
    if err != nil {
        log.Print("Error during seek: ", err)
        return nil, err
    }

    // Reset our chunks.
    l.chunks = make([]chunk, 0)

    for {
        // Get the current offset.
        offset, err := l.file.Seek(0, 1)
        if err != nil {
            log.Print("Error reading offset: ", err)
            continue
        }

        // Deserialize this entry.
        var ent entry
        record_size, free_space, err := deserialize(l.file, &ent)
        if err == io.EOF || err == io.ErrUnexpectedEOF {
            break
        } else if err != nil {
            return records, err
        }

        if free_space != 0 {
            // Save it as a free chunk.
            l.chunks = append(l.chunks, chunk{uint64(offset), free_space})
            utils.Print("STORAGE", "Free space @ %d (length: %d)", offset, free_space)
            continue
        }

        // Return the read entry.
        records = append(records, l.NewRecord(uint64(offset)))
        utils.Print("STORAGE", "Record for '%s' @ %d (length: %d)",
            ent.key, offset, record_size)
    }

    return records, nil
}
