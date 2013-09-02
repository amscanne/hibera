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

    flush chan bool
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

    process := func(upd *update) bool {
        // Check for terminal.
        if upd == nil {
            return true
        }

        // Track stats.
        atomic.AddUint64(&s.handled, 1)

        // Write out the data.
        err := s.logs.writeEntry(&upd.entry, logfile)

        // Notify that an error occured.
        if err != nil {
            upd.result <- err
            return false
        }

        // Add it to our list of done.
        finished = append(finished, upd)

        // Keep going.
        return false
    }

    for {

        if finished == nil {

            // Nothing in the queue?
            // Block indefinitely.
            select {
            case upd := <-s.pending:
                // Initialize the log.
                var err error
                finished = make([]*update, 0, 0)
                logfile, err = s.logs.NewLog()
                if err != nil {
                    log.Print("Error opening log: ", err)
                    return err
                }

                // Process normally.
                if process(upd) {
                    return <-squash_chan
                }
                break
            }

        } else {

            // Something in the queue?
            // Do a non-blocking call. If we don't
            // find anything right now, then we do
            // a flush log to complete this batch.
            select {
            case <-flush_chan:
                go func(logfile *logFile, finished []*update) {
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

                }(logfile, finished)

                // Reset our buffers.
                logfile = nil
                finished = nil
                break

            case upd := <-s.pending:
                if process(upd) {
                    return <-squash_chan
                }
                break
            }
        }
    }
}
