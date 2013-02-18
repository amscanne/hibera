package storage

import (
	"os"
	"io"
	"log"
	"sync"
	"path"
	"bytes"
	"strings"
	"syscall"
	"io/ioutil"
	"encoding/binary"
	"encoding/gob"
)

var DEFAULT_PATH = "/var/lib/hibera"
var MAXIMUM_LOG_BATCH = 256
var MAXIMUM_LOG_SIZE = int64(1024 * 1024)

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
	error
	*sync.Cond
}

type Backend struct {
	path   string
	memory map[string]Val
	data   *os.File
	log1   *os.File
	log2   *os.File
	cs     chan *Update
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
		p = DEFAULT_PATH
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
	err = b.init()
	if err != nil {
		return nil
	}
	return b
}

func (b *Backend) init() error {
	b.memory = make(map[string]Val)

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
	b.cs = make(chan *Update)

	// After our initial load do a pivot.
	b.pivotLogs()
	b.pivotLogs()

	return nil
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
	b.log1.Close()
	b.log1 = b.log2

	// Open a new second log.
	newlog2, err := OpenLocked(path.Join(b.path, "log.2"))
	if err != nil {
		return err
	}
	b.log2 = newlog2

	// Serialize our current database.
	b.data.Seek(0, 0)
	for key, val := range b.memory {
		err = serialize(b.data, &Entry{key, val})
		if err != nil {
			return err
		}
	}
	b.data.Sync()

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
			delete(b.memory, entry.Key)
		} else {
			b.memory[entry.Key] = entry.Val
		}
	}

	return nil
}

func (b *Backend) logWriter() {

	finished := make([]*Update, 0, MAXIMUM_LOG_BATCH)

	complete := func() {
		// Ensure we're synced.
		b.log2.Sync()

		// Notify waiters.
		for _, update := range finished {
			update.Cond.Broadcast()
		}
		finished = finished[0:0]

		// Kick off a pivot if we've exceed our size.
		off, err := b.log2.Seek(0, 1)
		if err != nil && off > MAXIMUM_LOG_SIZE {
			b.pivotLogs()
		}
	}

	process := func(update *Update) {
		// Update the in-memory database.
		// NOTE: This should be atomic.
		if update.Entry.Val.Value == nil {
			delete(b.memory, update.Entry.Key)
		} else {
			b.memory[update.Entry.Key] = update.Entry.Val
		}

		// Serialize the entry to the log.
		serialize(b.log2, &update.Entry)

		// Add it to our list of done.
		finished = append(finished, update)
	}

	for {
		// Do a non-blocking call.
		// If there's nothing to do at the moment,
		// we can call complete() to flush the batch.
		select {
		case update := <-b.cs:
			process(update)
			break
		default:
			complete()
			break
		}

		// Do a blocking call.
		update := <-b.cs
		process(update)

		if len(finished) == MAXIMUM_LOG_BATCH {
			complete()
		}
	}
}

func (b *Backend) Write(key string, value []byte, rev uint64) error {
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
	return val.Value, val.Rev, nil
}

func (b *Backend) Delete(key string) error {
	return b.Write(key, nil, 0)
}

func (b *Backend) List() ([]string, error) {
	keys := make([]string, 0, len(b.memory))
	for k, _ := range b.memory {
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
		uuid, err := Uuid()
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
