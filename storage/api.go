// Le storage.
//
// This submodule implements a simple storage system based
// around a very simple log database. It is structured as follows:
//
// logFile -- This type represents a log file. It allows for
//            reading/writing/erasing of logRecords. A logRecord
//            is a basic type which encapsulates the state for
//            a given position in the file. 
//
// logRecord -- A record for an entry in a logFile. When you read()
//              a record, you will get back functions that allow for
//              arbitrary I/O at a later point. This record is safe
//              from modification and deletion until you release it.
//
// logManager -- The high-level manager which controls the records
//               being serialized, etc. The storage API is essentially
//               the logManager.
// 
// There is a single thread which controls scheduling of I/O. This
// is found in queue.go. (It makes use of goroutines whenever possible,
// but it's careful about limiting sync() calls, etc.).
//
// The on disk storage strategy is as follows:
//
// 1) A data directory contains a file data.0,
//    which is a log file but written in a non-linear fashion.
//    This stores all the data keys, and log files are frequently
//    flattened into this file (asynchronously).
//
// 2) A series of log.* files in the log directory.
//    Writes are batched and written to a log file. The log file
//    will be sync()'ed when, a) the deadline expires (50ms) for
//    the oldest write in progress or b) there are no pending writes.
//    The write() requests in progress will be ACK'ed only when they
//    are sync()'ed on the disk. Once they are on disk, an asyncronous
//    flatten of the logfile will start.

package storage

import (
    "hibera/utils"
    "os"
)

func (s *Store) Write(id string, metadata []byte, data []byte) error {
    length := nilLength
    if data != nil {
        length = int32(len(data))
    }
    utils.Print("STORAGE", "WRITE %s (len %d)", id, length)

    // Just return the data directly.
    run := func(output *os.File, offset *int64) ([]byte, error) {
        return data, nil
    }

    // Submit the request.
    dio := &deferredIO{id, metadata, length, run}
    upd := &update{dio, make(chan error, 1)}
    s.pending <- upd
    return <-upd.result
}

func (s *Store) WriteFile(id string, metadata []byte, length int32, input *os.File, offset *int64) error {
    utils.Print("STORAGE", "WRITEFILE %s (len %d)", id, length)

    // Generate a splice function.
    run := generateSplice(input, length, offset)

    // Submit the request.
    dio := &deferredIO{id, metadata, length, run}
    upd := &update{dio, make(chan error, 1)}
    s.pending <- upd
    return <-upd.result
}

func (s *Store) Read(id string) ([]byte, []byte, error) {
    utils.Print("STORAGE", "READ %s", id)

    record, ok := s.logs.records[id]
    if ok {
        _, metadata, data, err := record.Read()
        return metadata, data, err
    }

    return nil, nil, nil
}

func (s *Store) ReadFile(id string, output *os.File, offset *int64) ([]byte, int32, IOWork, IODone, error) {
    utils.Print("STORAGE", "READFILE %s", id)

    record, ok := s.logs.records[id]
    if ok {
        _, metadata, length, read, done, err := record.ReadFile(output, offset)
        return metadata, length, read, done, err
    }

    return nil, 0, IOWorkNOP, IODoneNOP, nil
}

func (s *Store) Delete(id string) error {
    utils.Print("STORAGE", "DELETE %s", id)
    return s.Write(id, nil, nil)
}

func (s *Store) List() ([]string, error) {
    utils.Print("STORAGE", "LIST")
    keys := make([]string, 0, 0)
    for id, _ := range s.logs.records {
        keys = append(keys, string(id))
    }
    return keys, nil
}

func (s *Store) Run() error {
    err := s.flusher()
    s.stopped <- true
    return err
}

func (s *Store) Stop() {
    s.pending <- nil
    <-s.stopped
    s.logs.squashLogs()
    s.logs.close()
}
