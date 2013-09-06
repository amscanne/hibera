package storage

import (
    "log"
    "time"
)

// The log buffer.
// How big the buffer is for pending calls. There
// is no minimum size, but it should be big enough
// to prevent back-and-forth blocking of the flusher
// thread and the write() callers. It's more efficient
// if the flusher thread can process as many outstanding
// requests in a single CPU burst.
var Buffer = 1024

// The timeout deadline.
// We aggressively batch updates. We don't flush the
// log until one of two things happens: no updates come
// are queued, the timeout deadline is hit.
var Deadline = 50 * time.Millisecond

// An update to the database.
type update struct {
    dio    *deferredIO
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
    s.pending = make(chan *update, Buffer)
    s.stopped = make(chan bool, 1)

    return s, s.logs.open()
}

func (s *Store) flusher() error {

    var finished []*update
    var logfile *logFile
    var timeout <-chan time.Time

    // Allow a single flush at any given moment.
    flush_chan := make(chan bool, 1)
    flush_chan <- true

    // Allow a single squash at any given moment.
    squash_chan := make(chan error, 1)
    squash_chan <- nil

    // See NOTE about the Deadline at the top.
    ready_to_flush := false
    writes := make(chan *update)
    outstanding := 0

    process := func(upd *update) {
        // Track stats.
        s.handled += 1

        // Add it to our list of done.
        finished = append(finished, upd)

        // Mark a new function as outstanding.
        outstanding += 1

        // Write out the data.
        go func(upd *update, logfile *logFile, writes chan<- *update) {
            err := s.logs.writeEntry(upd.dio, logfile)
            if err != nil {
                // Mark out the error immediately.
                // We send back a nil over the channel.
                upd.result <- err
                writes <- nil
            } else {
                // Send back the update to wait for the flush.
                writes <- upd
            }
        }(upd, logfile, writes)
    }

    sync := func(logfile *logFile, outstanding int, finished []*update, writes <-chan *update) {

        // Clear the array.
        finished = finished[:0]

        // Ensure we have no outstanding writes.
        for outstanding > 0 {
            outstanding -= 1
            upd := <-writes
            if upd != nil {
                finished = append(finished, upd)
            }
        }

        // Sync the log.
        s.logs.closeLog(logfile)

        // Allow other flushes.
        flush_chan <- true

        // Notify waiters.
        for _, upd := range finished {
            upd.result <- nil
        }

        // Squash the log.
        // NOTE: Just like the flushers above, we only allow
        // a single squash to proceed at any time. This basically
        // means that the two operations won't trample on each other's
        // feet -- but it's quite possible one squash will be covered
        // in the other one.
        last_squash := <-squash_chan
        if last_squash == nil {
            last_squash = s.logs.squashLogsUntil(logfile.number)
        }
        squash_chan <- last_squash
    }

    complete := func() {

        // Run the sync() asycnhronously.
        go sync(logfile, outstanding, finished, writes)

        // Drop this logfile.
        logfile = nil

        // Reset our state.
        ready_to_flush = false
        writes = make(chan *update)
        outstanding = 0

        // Reset our buffers.
        finished = make([]*update, 0, Buffer)
    }

    for {

        if logfile == nil {

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
                logfile, err = s.logs.NewLog()
                if err != nil {
                    log.Print("Error opening log: ", err)
                    upd.result <- err
                    continue
                }

                // Set the deadline.
                timeout = time.After(Deadline)

                // Process normally.
                process(upd)
                break
            }

        } else if !ready_to_flush {

            // Something in the queue?
            // Do a non-blocking call. If we don't
            // find anything right now, then we do
            // a flush log to complete this batch.
            select {
            case <-timeout:
                // We've past our deadline.
                ready_to_flush = true
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
            default:
                // Nothing in the queue.
                // Even though we haven't timed out,
                // we schedule a flush for this.
                ready_to_flush = true
                break
            }
        } else {

            // Do a full-blocking call.
            // Very shortly after we've able to flush
            // (given that the selection is pseudo-random)
            // we will execute the flush call.

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

    return nil
}
