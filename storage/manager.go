package storage

import (
    "fmt"
    "hibera/utils"
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

    data         *logFile
    data_records map[key]*logRecord

    log_size   int64
    log_number uint64

    records map[key]*logRecord
}

func (l *logManager) loadData() error {

    utils.Print("STORAGE", "Loading data...")

    // Just check all records.
    records, err := l.data.Load()
    if err != nil {
        utils.Print("STORAGE", "Error loading: %s", err)
        return err
    }

    // Reset records.
    l.data_records = make(map[key]*logRecord)

    // Set just the data record.
    for _, record := range records {
        var ent entry
        _, err = record.Read(&ent)
        if err != nil {
            utils.Print("STORAGE", "Skipping record: %s", err)
            continue
        }

        // Check for an earlier key.
        // NOTE: It's quite possible this the "original" record
        // here is in fact the later record and we are deleting
        // the wrong piece. However, if we somehow wound up with
        // two records for the same key in the data log, then we
        // know that something went wrong during the squash.
        // Specifically, something went wrong between the Copy()
        // and the Delete() in squashLogsUntil(). We know then,
        // that the original log with the new record is still
        // around and we will read it shortly.
        orig_data_rec, has_data_rec := l.data_records[ent.key]

        if ent.value.data != nil || ent.value.metadata != nil {
            // Set the latest in the file.
            utils.Print("STORAGE", "Loading data record '%s'...", ent.key)
            l.data_records[ent.key] = record
        } else {
            // Clear the records.
            utils.Print("STORAGE", "Deleting data record '%s'...", ent.key)
            delete(l.data_records, ent.key)
            record.Delete()
        }

        // Remove the "older" record (see above).
        if has_data_rec {
            utils.Print("STORAGE", "Deleting original record '%s'...", ent.key)
            orig_data_rec.Delete()
        }
    }

    // Setup the records.
    for key, record := range l.data_records {

        orig_ref, has_orig_ref := l.records[key]

        // Grab an additional reference to the data record,
        // since when we drop this it will be discarded.
        record.Grab()
        l.records[key] = record

        // Drop a reference to the original record.
        if has_orig_ref {
            orig_ref.Discard()
        }
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
        filename := fmt.Sprintf("log.%d", basenum)
        utils.Print("STORAGE", "Loading from %s...", filename)

        // Open the file.
        file, err := ReadLog(
            path.Join(l.logPath, filename),
            uint64(basenum))
        if err != nil {
            return err
        }

        // Get the filesize.
        size, err := file.Size()
        if err != nil {
            file.Close()
            return err
        }

        // Read all the entries.
        records, err := file.Load()
        if err != nil {
            file.Close()
            return err
        }

        // Set the current record.
        for _, record := range records {
            var ent entry
            _, err = record.Read(&ent)

            // The end of the file may have
            // been corrupted, so we let invalid
            // records go. We are not as tolerant
            // in other aspects of loading files.
            if err != nil {
                continue
            }

            orig_rec, has_orig_rec := l.records[ent.key]

            // Save the new record.
            l.records[ent.key] = record

            // If there was an original record,
            // unlike the data case we don't delete it.
            // We simply drop the old reference. It will
            // be deleted when the log files are squashed.
            if has_orig_rec {
                orig_rec.Discard()
            }
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
        log_number, true)
}

func (l *logManager) closeLog(logfile *logFile) {
    // Ensure we're synced.
    logfile.Sync()
    logfile.Close()

    // Add the final size to the tally.
    size, err := logfile.Size()
    if err == nil {
        atomic.AddInt64(&l.log_size, int64(size))
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
        utils.Print("STORAGE", "Removed log file '%s'.", logfile)
        atomic.AddInt64(&l.log_size, -fi.Size())
    }

    // Reset the log number if it's still limit.
    atomic.CompareAndSwapUint64(&l.log_number, limit, 0)

    return nil
}

func (l *logManager) checkSquash(logfile *logFile) error {

    // Check if we should squash the logs.
    if atomic.LoadInt64(&l.log_size) > MaximumLogSize {
        limit := logfile.number
        return l.squashLogsUntil(limit)
    }

    return nil
}

func (l *logManager) squashLogsUntil(limit uint64) error {

    // Update all current records.
    for key, record := range l.records {

        // Ignore this record if it's the data record.
        if l.data_records[key] == record {
            continue
        }

        // Copy all logs down to the data layer.
        orig_data, has_orig_data := l.data_records[key]
        ent, err := record.Copy(l.data)
        if err != nil {
            continue
        }
        utils.Print("STORAGE", "Saved data record '%s'...", ent.key)

        // Remove the original record if it exists.
        if has_orig_data {
            utils.Print("STORAGE", "Deleting original data record '%s'...", ent.key)
            orig_data.Delete()
        }

        // Check for a delete.
        if ent.value.data == nil && ent.value.metadata == nil {
            // Delete this record also.
            utils.Print("STORAGE", "Deleting saved record '%s'...", ent.key)
            delete(l.data_records, key)
            record.Delete()
        } else {
            // The record now points to data.
            utils.Print("STORAGE", "Leaving new record '%s'...", ent.key)
            l.data_records[key] = record
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

func (l *logManager) writeEntry(ent *entry, logfile *logFile) error {
    record, err := logfile.Write(ent)
    if err != nil {
        return err
    }
    orig_rec, has_orig_rec := l.records[ent.key]

    // Was this a delete? If so, forget our delete
    // record and just store nothing. Otherwise, store
    // the latest record.
    if ent.value.data == nil && ent.value.metadata == nil {
        delete(l.records, ent.key)
        record.Discard()
    } else {
        l.records[ent.key] = record
    }

    // Discard the original reference.
    if has_orig_rec {
        orig_rec.Discard()
    }

    return nil
}

func NewLogManager(logPath string, dataPath string) (*logManager, error) {
    l := new(logManager)
    l.logPath = logPath
    l.dataPath = dataPath
    l.records = make(map[key]*logRecord)
    l.data_records = make(map[key]*logRecord)

    // Create the directories.
    err := os.MkdirAll(l.dataPath, 0644)
    if err != nil {
        log.Print("Error initializing data: ", err)
        return nil, err
    }

    // Open our files.
    l.data, err = OpenLog(path.Join(l.dataPath, "data.0"), 0, true)
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
    // NOTE: This will set our highest log number as
    // a side-effect for when we squashLogs() below.
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
