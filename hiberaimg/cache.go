package main

import (
    "container/heap"
    "github.com/amscanne/hibera/utils"
    "io"
    "os"
    "path"
    "path/filepath"
    "strings"
    "sync"
    "time"
)

type cacheEntry struct {
    hash      string
    timestamp time.Time
    size      int64
    index     int
}

type cacheHeap []*cacheEntry

type cache struct {
    maxSize int64
    curSize int64

    dir string

    age     *cacheHeap
    index   map[string]*cacheEntry
    pending map[string]bool

    *sync.Cond
}

func (c cacheHeap) Len() int {
    return len(c)
}

func (c cacheHeap) Less(i, j int) bool {
    return c[i].timestamp.Before(c[j].timestamp)
}

func (c cacheHeap) Swap(i, j int) {
    c[i], c[j] = c[j], c[i]
    c[i].index = i
    c[j].index = j
}

func (c *cacheHeap) Push(x interface{}) {
    n := len(*c)
    entry := x.(*cacheEntry)
    entry.index = n
    utils.Debug("cache: pushed %s\n", entry.hash)
    *c = append(*c, entry)
}

func (c *cacheHeap) Pop() interface{} {
    old := *c
    n := len(old)
    entry := old[n-1]
    entry.index = -1
    utils.Debug("cache: popped %s\n", entry.hash)
    *c = old[0 : n-1]
    return entry
}

func newCache(size int64, dir string) (*cache, error) {
    // Ensure the directory exists.
    err := os.MkdirAll(dir, 0755)
    if err != nil {
        return nil, err
    }

    // Setup the cache.
    age := &cacheHeap{}
    heap.Init(age)

    // Create our cache.
    cache := &cache{
        size,
        0,
        dir,
        age,
        make(map[string]*cacheEntry),
        make(map[string]bool),
        sync.NewCond(&sync.Mutex{}),
    }

    // Walk the cache.
    walk := func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        // Ignore directories.
        if info.IsDir() {
            return nil
        }

        // Create the entry.
        filename := path[len(dir):]
        hash := strings.Replace(filename, "/", "", -1)
        entry := &cacheEntry{hash, info.ModTime(), info.Size(), -1}
        cache.curSize += entry.size

        // Add away.
        cache.index[hash] = entry
        heap.Push(cache.age, entry)
        return nil
    }
    err = filepath.Walk(dir, walk)
    if err != nil {
        return nil, err
    }

    return cache, cache.clean()
}

func (c *cache) makeFilename(hash string) (string, string) {
    return path.Join(c.dir, hash[0:2]+"/"+hash[2:4]), hash[4:]
}

func (c *cache) touch(entry *cacheEntry) error {
    heap.Remove(c.age, entry.index)
    dirname, filename := c.makeFilename(entry.hash)
    err := os.Chtimes(path.Join(dirname, filename), time.Now(), time.Now())
    heap.Push(c.age, entry)
    return err
}

func (c *cache) start(hash string) ([]byte, error) {
    c.Cond.L.Lock()

    // For for anything currently downloading this hash.
    for c.pending[hash] {
        c.Cond.Wait()
    }

    // Check if it's here.
    entry := c.index[hash]
    if entry != nil {
        utils.Debug("cache: hit %s (index=%d)\n", hash, entry.index)
        dirname, filename := c.makeFilename(hash)
        file, err := os.OpenFile(path.Join(dirname, filename), os.O_RDONLY, 0)
        if err != nil {
            c.Cond.L.Unlock()
            return nil, err
        }
        defer file.Close()
        err = c.touch(entry)
        if err != nil {
            c.Cond.L.Unlock()
            return nil, err
        }
        c.Cond.L.Unlock()

        value := make([]byte, entry.size, entry.size)
        _, err = io.ReadFull(file, value)
        if err != nil {
            return nil, err
        }
        return value, nil
    }

    // Nothing here. Mark this as pending.
    // They will either call cancel() or save().
    utils.Debug("cache: miss %s\n", hash)
    c.pending[hash] = true
    c.Cond.L.Unlock()
    return nil, nil
}

func (c *cache) cancel(hash string) {
    c.Cond.L.Lock()
    defer c.Cond.L.Unlock()
    delete(c.pending, hash)
    c.Broadcast()
}

func (c *cache) save(hash string, value []byte) error {
    c.Cond.L.Lock()

    entry := c.index[hash]
    if entry == nil && c.pending[hash] {
        c.Cond.L.Unlock()
        dirname, filename := c.makeFilename(hash)
        err := os.MkdirAll(dirname, 0755)
        if err != nil {
            return err
        }
        file, err := os.OpenFile(path.Join(dirname, filename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
        if err != nil {
            return err
        }
        for n := 0; n < len(value); {
            written, err := file.Write(value[n:])
            if err != nil {
                return err
            }
            n += written
        }
        entry := &cacheEntry{hash, time.Now(), int64(len(value)), -1}

        c.Cond.L.Lock()
        heap.Push(c.age, entry)
        c.index[entry.hash] = entry
        c.curSize += entry.size
        delete(c.pending, hash)
        c.Broadcast()
        utils.Debug("cache: saved %s (index=%d)\n", hash, entry.index)
    }

    c.clean()
    c.Cond.L.Unlock()
    return nil
}

func (c *cache) clean() error {
    for c.curSize > c.maxSize && len(*c.age) > 0 {
        x := heap.Pop(c.age)
        entry := x.(*cacheEntry)
        utils.Debug("cache: removing %s (index=%d)\n", entry.hash, entry.index)
        delete(c.index, entry.hash)
        dirname, filename := c.makeFilename(entry.hash)
        err := os.Remove(path.Join(dirname, filename))
        if err != nil {
            c.index[entry.hash] = entry
            heap.Push(c.age, entry)
            return err
        }
        c.curSize -= entry.size
    }

    return nil
}
