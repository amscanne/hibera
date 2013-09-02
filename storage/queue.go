package storage

import (
    "log"
    "sync/atomic"
)

// The log buffer.
// How big the buffer is for pending calls.
var LogBuffer = 10240

type update struct {
    entry
    result chan error
}

type Store struct {
    logs *logManager

    pending chan *update
    handled uint64

    flush  chan bool
    squash chan error

    stopped chan bool
}

func NewStore(logPath string, dataPath string) (*Store, error) {
    // Create our log manager.
    logs, err := NewLogManager(logPath, dataPath)
    if err != nil {
        return nil, err
    }

    // Create our backend.
    s := new(Store)
    s.logs = logs
    s.pending = make(chan *update, LogBuffer)
    s.stopped = make(chan bool, 1)

    return s, nil
}

func (s *Store) flusher() error {

    var finished []*update
    var logfile *logFile

    // Allow a single flush at any given moment.
    flush_chan := make(chan bool, 1)
    flush_chan <- true

    // Allow a single squash at any given moment.
    squash_chan := make(chan error, 1)
    squash_chan <- nil

    process := func(upd *update) {
        // Track stats.
        atomic.AddUint64(&s.handled, 1)

        // Write out the data.
        err := s.logs.writeEntry(&upd.entry, logfile)

        // Notify that an error occured.
        if err != nil {
            upd.result <- err
        }

        // Add it to our list of done.
        finished = append(finished, upd)
    }

    complete := func() {
        sync := func(logfile *logFile, finished []*update) {
            // Sync the log.
            s.logs.closeLog(logfile)

            // Allow other flushes.
            flush_chan <- true

            // Notify waiters.
            for _, upd := range finished {
                upd.result <- nil
            }

            // Squash the log.
            last_squash := <-squash_chan
            if last_squash == nil {
                last_squash = s.logs.checkSquash(logfile)
            }
            squash_chan <- last_squash
        }

        // Run the sync.
        go sync(logfile, finished)

        // Reset our buffers.
        logfile = nil
        finished = nil
    }

    for {

        if finished == nil {

            // Nothing in the queue?
            // Block indefinitely.
            select {
            case upd := <-s.pending:
                // Check for terminal.
                if upd == nil {
                    return <-squash_chan
                }

                // Initialize the log.
                var err error
                finished = make([]*update, 0, 0)
                logfile, err = s.logs.NewLog()
                if err != nil {
                    log.Print("Error opening log: ", err)
                    upd.result <- err
                    continue
                }

                // Process normally.
                process(upd)
                break
            }

        } else {

            // Something in the queue?
            // Do a non-blocking call. If we don't
            // find anything right now, then we do
            // a flush log to complete this batch.
            select {
            case <-flush_chan:
                complete()
                break

            case upd := <-s.pending:
                // Check for terminal.
                if upd == nil {
                    <-flush_chan
                    complete()
                    return <-squash_chan
                }

                // Process normally.
                process(upd)
                break
            }
        }
    }
}
