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
    offset int64
    length int64
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

    offset int64
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

func (l *logFile) NewRecord(offset int64) *logRecord {
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

func (l *logFile) Write(dio *deferredIO) (*logRecord, error) {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Find a free chunk.
    var offset int64
    length, padding := dio.usage()
    required := length + padding
    remaining := int64(0)
    found := false

    var index int
    var chk chunk
    for index, chk = range l.chunks {
        if chk.length >= int64(required) {
            found = true
            offset = chk.offset
            remaining = chk.length - int64(required)
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
    run, err := serialize(l.file, dio)
    if err != nil {
        return nil, err
    }

    // Do the actual write.
    // NOTE: The reason for this structure,
    // should be obvious by looking at the locking.
    l.Mutex.Unlock()
    err = run()
    l.Mutex.Lock()
    if err != nil {
        return nil, err
    }

    // Mark any remaining space as still free,
    // and add the chunk to our list of chunks.
    if remaining > 0 {
        free_remaining := remaining - int64(int32_size)
        free_offset := offset + int64(int32_size) + int64(required)
        clear(l.file, int32(free_remaining))
        l.chunks = append(l.chunks, chunk{free_offset, free_remaining})
    }

    return l.NewRecord(offset), nil
}

func (l *logFile) Close() {
    if atomic.AddInt32(&l.refs, -1) == 0 {
        l.file.Close()
    }
}

func (l *logFile) Open() {
    atomic.AddInt32(&l.refs, 1)
}

func (l *logRecord) readGuarded() (*deferredIO, func(), error) {
    // Lock the file to guard for the seek().
    l.log.Mutex.Lock()
    defer l.log.Mutex.Unlock()

    // Seek to the appropriate offset.
    _, err := l.log.file.Seek(int64(l.offset), 0)
    if err != nil {
        return nil, nop_cancel, err
    }

    // Deserialize the record.
    dio, err := deserialize(l.log.file)
    if err != nil {
        return nil, nop_cancel, err
    }

    // Guard the logfile against closing.
    // NOTE: We do *not* guard against calling
    // these functions multiple times. Whoever
    // is calling this function should be very
    // careful to ensure that only one is called,
    // and only once. This is done below with the
    // closure trick.
    this_log := l.log
    this_log.Open()
    finished := func() {
        this_log.Close()
    }
    return dio, finished, err
}

func (l *logRecord) ReadFD() (string, []byte, int32, func(*os.File, *int64) error, func(), error) {
    l.Mutex.Lock()

    // Get our deferred functions.
    dio, finished, err := l.readGuarded()
    if err != nil {
        l.Mutex.Unlock()
        return "", nil, 0, nop_read, nop_cancel, err
    }

    // Wrap the read function.
    // NOTE: We cleverly use the closure here
    // in order to protect users from doing mulitple
    // cancellations. This is because sometimes it's
    // difficult to know when you should (i.e. what
    // has gone wrong!). So you can do it all the time.
    yes_done := false
    done := func() {
        if !yes_done {
            yes_done = true
            l.Mutex.Unlock()
            finished()
        }
    }
    read := func(output *os.File, offset *int64) error {
        defer done()
        _, err := dio.run(output, offset)
        return err
    }

    return dio.key, dio.metadata, dio.length, read, done, nil
}

func (l *logRecord) Read() (string, []byte, []byte, error) {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Get our deferred functions.
    dio, finished, err := l.readGuarded()
    if err != nil {
        return "", nil, nil, err
    }

    // Make sure we call finished().
    defer finished()

    // Get our data.
    data, err := dio.run(nil, nil)
    if err != nil {
        return dio.key, dio.metadata, nil, err
    }

    return dio.key, dio.metadata, data, nil
}

func (l *logRecord) Delete() error {
    // NOTE: For reads and writes, we actually
    // drop the lock before doing the read/write.
    // This is to maximize performance. We can't
    // really do the same for delete(), since some
    // other thread could be reading here.
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Read the original record.
    dio, finished, err := l.readGuarded()
    if err != nil {
        return err
    }

    // Make sure we drop the reference.
    defer finished()

    // Grab the global lock before we start
    // scanning through chunks. This should only
    // really happen on the squashLogs() code path
    // for the data file, so it's not in the critical
    // path.
    l.log.Mutex.Lock()
    defer l.log.Mutex.Unlock()

    // Merge it with any existing chunks by
    // popping them out of the list and keep going.
    current := chunk{l.offset, int64(dio.length)}

    for {
        merged := false

        // Find a potential merge.
        var index int
        var other chunk
        for index, other = range l.log.chunks {
            if other.offset+int64(int32_size)+other.length == current.offset {
                // Update our current chunk.
                // NOTE: We lose one header, so this is added to the length.
                current.offset = other.offset
                current.length = other.length + int64(int32_size) + current.length
                merged = true
                break
            }
            if current.offset+int64(int32_size)+current.length == other.offset {
                // Update our current chunk.
                // NOTE: As per above, we are down one header, so it's added.
                current.length = current.length + int64(int32_size) + other.length
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
    err = clear(l.log.file, int32(current.length))
    if err != nil {
        return err
    }

    // Add in our newly merged chunk.
    l.log.chunks = append(l.log.chunks, current)

    // Drop the reference.
    l.log.Close()

    // Release the lock.
    return nil
}

func (l *logRecord) Copy(output *logFile) error {
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

    // Read the original record.
    dio, finished, err := l.readGuarded()
    if err != nil {
        return err
    }

    // Make sure we finish our ref.
    defer finished()

    // Create a new record.
    // After this point, we don't call cancel.
    out_rec, err := output.Write(dio)
    if err != nil {
        return err
    }

    // Swap our references.
    orig_log := l.log
    l.log = out_rec.log
    l.offset = out_rec.offset

    // Drop a reference.
    orig_log.Close()

    return nil
}

func (l *logRecord) Discard() {
    // NOTE: There are possible race
    // conditions with the discard only.
    // These are protected by the lock,
    // but it's not strictly necessary to
    // protect Grab() in the same way.
    l.Mutex.Lock()
    defer l.Mutex.Unlock()

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
        dio, err := deserialize(l.file)
        if err == io.EOF || err == io.ErrUnexpectedEOF {
            break
        } else if err == freeSpace {
            // Save it as a free chunk.
            l.chunks = append(l.chunks, chunk{offset, int64(dio.length)})
            utils.Print("STORAGE", "Free space @ %d (length: %d)", offset, dio.length)
            continue
        } else if err != nil {
            return records, err
        }

        // Return the read entry.
        records = append(records, l.NewRecord(offset))
        utils.Print("STORAGE", "Record for '%s' @ %d (length: %d)",
            dio.key, offset, dio.length)
    }

    return records, nil
}
