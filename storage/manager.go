package storage

import (
    "fmt"
    "log"
    "os"
    "path"
    "path/filepath"
    "sort"
    "strconv"
    "sync/atomic"
)

// The maximum log size.
// If this log size is hit (1Mb default) then we will do
// a full pivot of the logs and synchronize the data file.
var MaximumLogSize = int64(1024 * 1024)

type logManager struct {
    logPath  string
    dataPath string

    data *logFile
    data_records map[key]*logRecord

    log_size     int64
    log_number   uint64

    flush chan bool
    squash chan error

    records map[key]*logRecord
}

func (l *logManager) loadData() error {

    // Just check all records.
    records, err := l.data.Load()
    if err != nil {
        return err
    }

    // Set just the data record.
    for _, record := range records {
        var ent entry
        _, err = record.Read(&ent)
        if err != nil {
            continue
        }
        l.data_records[ent.key] = record
    }

    return nil
}

func (l *logManager) loadLogs() error {

    // Create the log path if it doesn't exist.
    err := os.MkdirAll(l.logPath, 0644)
    if err != nil {
        log.Print("Error initializing log: ", err)
        return err
    }

    // List all entries.
    logfiles, err := filepath.Glob(path.Join(l.logPath, "log.*"))
    if err != nil {
        log.Print("Error listing logs: ", err)
        return err
    }

    lognumbers := make([]int, 0, len(logfiles))
    for _, logfile := range logfiles {
        // Extract suffix (after log.).
        basename := filepath.Base(logfile)[4:]

        // Extract the number.
        // NOTE: This is currently limited to 32 bits just
        // to support the IntSlice below. This is somewhat
        // annoying, but if necessary it could be expanded
        // without breaking compatibilit.
        basenum, err := strconv.ParseInt(basename, 10, 32)
        if err != nil {
            // Skip unknown files.
            continue
        }

        // Save the number.
        lognumbers = append(lognumbers, int(basenum))
    }

    // Sort the logs.
    sort.Sort(sort.IntSlice(lognumbers))

    for _, basenum := range lognumbers {
        // Open the file.
        file, err := OpenLog(
            path.Join(l.logPath, fmt.Sprintf("log.%d", basenum)),
            uint64(basenum))

        // Get the filesize.
        size, err := file.Size()
        if err != nil {
            file.Close()
            continue
        }

        // Read all the entries.
        records, err := file.Load()
        if err != nil {
            file.Close()
            continue
        }

        // Set the current record.
        for _, record := range records {
            var ent entry
            _, err = record.Read(&ent)
            if err != nil {
                continue
            }
            l.records[ent.key] = record
        }

        // Save the highest log number.
        if uint64(basenum) > l.log_number {
            l.log_number = uint64(basenum)
        }

        // Keep going.
        l.log_size += int64(size)
        file.Close()
    }

    return nil
}

func (l *logManager) NewLog() (*logFile, error) {
    log_number := atomic.AddUint64(&l.log_number, 1)
    return OpenLog(
        path.Join(l.logPath, fmt.Sprintf("log.%d", log_number)),
        log_number)
}

func (l *logManager) flushReady() chan bool {
    return l.flush
}

func (l *logManager) closeLog(logfile *logFile) {
    // Ensure we're synced.
    logfile.Sync()
    logfile.Close()

    // Allow other flushes.
    l.flush <- true

    // Kick off a pivot if we've exceed our size.
    size, err := logfile.Size()
    if err == nil {
        atomic.AddInt64(&l.log_size, int64(size))
    }

    // Allow only one squash at a time.
    last_err := <-l.squash
    if last_err == nil {
        l.squash<- l.checkSquash(logfile.number)
    } else {
        l.squash<- last_err
    }
}

func (l *logManager) removeLogs(limit uint64) error {

    // List all log files.
    logfiles, err := filepath.Glob(path.Join(l.logPath, "log.*"))
    if err != nil {
        log.Print("Error listing log files: ", err)
        return err
    }

    // Delete them all.
    // Unfortunately a stale logfile can have unintended
    // consequences so we have to bail. At the higher level,
    // we ensure that the data is written out before bailling,
    // because *some* log file may have been erased.
    for _, logfile := range logfiles {
        // Extract suffix (after log.).
        basename := filepath.Base(logfile)[4:]

        // Extract the number.
        basenum, err := strconv.ParseUint(basename, 10, 64)
        if err != nil {
            // Skip unknown files.
            continue
        }

        // Don't erase past where we're given.
        if basenum > limit {
            continue
        }

        // Get the filesize.
        fi, err := os.Stat(logfile)
        if err != nil {
            continue
        }

        // Try the actual remove.
        err = os.Remove(logfile)
        if err != nil {
            log.Print("Error removing log file: ", err)
            return err
        }

        // Take off the entries.
        atomic.AddInt64(&l.log_size, -fi.Size())
    }

    // Reset the log number if it's still limit.
    atomic.CompareAndSwapUint64(&l.log_number, limit, 0)

    return nil
}

func (l *logManager) checkSquash(limit uint64) error {
    if atomic.LoadInt64(&l.log_size) > MaximumLogSize {
        return l.squashLogsUntil(limit)
    }

    return nil
}

func (l *logManager) squashLogsUntil(limit uint64) error {

    // Update all current records.
    for key, record := range l.records {

        // Copy all logs down to the data layer.
        orig_data, has_orig_data := l.data_records[key]
        copied, err := record.Copy(l.data)
        if err != nil {
            return err
        }

        // Remove the original record if it exists.
        l.data_records[key] = record
        if copied && has_orig_data {
            orig_data.Delete()
        }
    }

    // Synchronize the data.
    l.data.Sync()

    // Remove old log files.
    return l.removeLogs(limit)
}

func (l *logManager) squashLogs() error {
    // Grab the current highest log.
    limit := atomic.LoadUint64(&l.log_number)
    return l.squashLogsUntil(limit)
}

func (l *logManager) squashResult() error {
    return <-l.squash
}

func (l *logManager) writeEntry(ent *entry, logfile *logFile) error {
    record, err := logfile.Write(ent)
    if err != nil {
        return err
    }
    l.records[ent.key] = record
    return nil
}

func NewLogManager(logPath string, dataPath string) (*logManager, error) {
    l := new(logManager)
    l.logPath = logPath
    l.dataPath = dataPath
    l.records = make(map[key]*logRecord)
    l.data_records = make(map[key]*logRecord)
    l.flush = make(chan bool, 1)
    l.squash = make(chan error, 1)
    l.flush <- true
    l.squash <- nil

    // Create the directories.
    err := os.MkdirAll(l.dataPath, 0644)
    if err != nil {
        log.Print("Error initializing data: ", err)
        return nil, err
    }

    // Open our files.
    l.data, err = OpenLog(path.Join(l.dataPath, "data"), 0)
    if err != nil {
        log.Print("Error initializing data file: ", err)
        return nil, err
    }
    err = l.loadData()
    if err != nil {
        l.data.Close()
        l.data = nil
        log.Print("Error loading data file: ", err)
        return nil, err
    }

    // Load log files.
    err = l.loadLogs()
    if err != nil {
        l.data.Close()
        l.data = nil
        log.Print("Unable to load initial log files: ", err)
        return nil, err
    }

    // Clean old log files.
    err = l.squashLogs()
    if err != nil {
        l.data.Close()
        l.data = nil
        log.Print("Unable to clean squash log files: ", err)
        return nil, err
    }

    return l, nil
}
