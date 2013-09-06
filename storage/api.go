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
