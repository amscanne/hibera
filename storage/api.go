package storage

import (
    "hibera/utils"
)

func (s *Store) Write(id string, data []byte, metadata []byte) error {
    utils.Print("STORAGE", "WRITE %s (len %d)", id, len(data))
    ent := entry{key(id), value{data, metadata}}
    upd := &update{ent, make(chan error, 1)}
    s.pending <- upd
    return <-upd.result
}

func (s *Store) Read(id string) ([]byte, []byte, error) {
    utils.Print("STORAGE", "READ %s", id)
    record, ok := s.logs.records[key(id)]
    if ok {
        var ent entry
        _, err := record.Read(&ent)
        if err != nil {
            return nil, nil, err
        }
        return ent.value.data, ent.value.metadata, nil
    }
    return nil, nil, nil
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
}
