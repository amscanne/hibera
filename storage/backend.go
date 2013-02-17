package storage

import (
	"os"
	"log"
	"sync"
	"path"
	"encoding/gob"
)

var DEFAULT_PATH = "/var/lib/hibera"
var MAXIMUM_LOG_BATCH = 256
var MAXIMUM_LOG_SIZE = int64(1024 * 1024)

type Val struct {
	rev   uint64
	value []byte
}

type Entry struct {
	key string
	Val
}

type Update struct {
	Entry
	error
	*sync.Cond
}

type Backend struct {
	memory   map[string]Val
	data     *os.File
	log1name string
	log1     *os.File
	log2name string
	log2     *os.File
	lock     *sync.Mutex
	cs       chan *Update
}

func NewBackend(p string) *Backend {
	if len(p) == 0 {
		p = DEFAULT_PATH
	}

	// Create the directory.
	err := os.MkdirAll(p, 0644)
	if err != nil {
		log.Fatal("Error initializing storage: ", err)
		return nil
	}

	// Open our files.
	data, err := os.OpenFile(path.Join(p, "data"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Error initializing data file: ", err)
		return nil
	}
	log1name := path.Join(p, "log.1")
	log1, err := os.OpenFile(log1name, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		data.Close()
		log.Fatal("Error initializing log file: ", err)
		return nil
	}
	log2name := path.Join(p, "log.2")
	log2, err := os.OpenFile(log2name, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		data.Close()
		log.Fatal("Error initializing log file: ", err)
		return nil
	}

	// Create our log writer channel.
	cs := make(chan *Update)

	// Create our backend.
	b := &Backend{make(map[string]Val), data, log1name, log1, log2name, log2, new(sync.Mutex), cs}
	b.loadFile(b.data)
	b.loadFile(b.log1)
	b.loadFile(b.log2)
	return b
}

func (b *Backend) pivotLogs() error {

	// Remove the first log.
	err := os.Remove(b.log1name)
	if err != nil {
		return err
	}

	// Pivot the second one to the first by name.
	err = os.Rename(b.log2name, b.log1name)
	if err != nil {
		return err
	}
	b.log1.Close()
	b.log1 = b.log2

	// Open a new second log.
	newlog2, err := os.OpenFile(b.log2name, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	b.log2 = newlog2

	// Reset the data pointer.
	b.data.Seek(0, 0)
	enc := gob.NewEncoder(b.data)

	// Serialize our current database.
	for key, val := range b.memory {
		err = enc.Encode(Entry{key, val})
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) loadFile(file *os.File) error {

	// Reset the pointer.
	file.Seek(0, 0)
	dec := gob.NewDecoder(file)

	for {
		var entry Entry
		err := dec.Decode(&entry)
		if err != nil {
			break
		}

		// Atomic set on our map.
		if entry.Val.value == nil {
			delete(b.memory, entry.key)
		} else {
			b.memory[entry.key] = entry.Val
		}
	}

	return nil
}

func (b *Backend) logWriter() {

	enc := gob.NewEncoder(b.log2)
	finished := make([]*Update, 0, MAXIMUM_LOG_BATCH)

	complete := func() {
		// Notify waiters.
		for _, update := range finished {
			update.Cond.Broadcast()
		}
		finished = finished[0:0]

		// Kick off a pivot if we've exceed our size.
		off, err := b.log2.Seek(0, 1)
		if err != nil && off > MAXIMUM_LOG_SIZE {
			go b.pivotLogs()
		}
	}

	for {
		select {
		case update := <-b.cs:
			// Update the in-memory database.
			// NOTE: This should be atomic.
			if update.Entry.Val.value == nil {
				delete(b.memory, update.Entry.key)
			} else {
				b.memory[update.Entry.key] = update.Entry.Val
			}

			// Serialize the entry to the log.
			b.lock.Lock()
			update.error = enc.Encode(update.Entry)
			b.lock.Unlock()

			// Add it to our list of done.
			finished = append(finished, update)
			break

		default:
			complete()
			break
		}

		if len(finished) == MAXIMUM_LOG_BATCH {
			complete()
		}
	}
}

func (b *Backend) Write(key string, rev uint64, value []byte) error {
	val := Val{rev, value}
	entry := Entry{key, val}
	mutex := sync.Mutex{}
	update := Update{entry, nil, sync.NewCond(&mutex)}
	mutex.Lock()
	b.cs <- &update
	update.Cond.Wait()
	mutex.Unlock()
	return update.error
}

func (b *Backend) Read(key string) ([]byte, uint64, error) {
	val := b.memory[key]
	return val.value, val.rev, nil
}

func (b *Backend) Delete(key string, rev uint64) error {
	return b.Write(key, rev, nil)
}

func (b *Backend) Run() {
	b.logWriter()
}
