package storage

import (
    "bytes"
    "encoding/binary"
    "errors"
    "fmt"
    "hibera/utils"
    "io"
    "log"
    "os"
    "syscall"
)

// Magic numbers.
// These are defined arbitrarily.
var magic1 = int32(0x37f273e1)
var magic2 = int32(0x78cd3928)

// The size of an int32.
// Just defined for convenience, and to
// prevent weird manipulations when using
// the unsafe module. The size is 4 bytes,
// it's not going to change.
var int32Size = int32(4)

// The alignment of the file.
// We always ensure that records start and
// stop at these boundaries. This is done
// by seeking for the start of each record.
// NOTE: This also should be the same same
// as the header, as it will prevent smaller
// chunks than a single header being left.
var alignment = int32Size

// The length used to encode a nil.
var nilLength = int32(-1)

// Error returned when free space is deserialized.
var freeSpace = errors.New("free space")

// Error returned when length doesn't match expected.
var lengthMismatch = errors.New("length mismatch")

// Error returned when magic number doesn't match.
var invalidMagic = errors.New("invalid magic number")

// Generated function that performs real I/O.
type IOFunc func(*os.File, *int64) ([]byte, error)

func IOFuncNOP(*os.File, *int64) ([]byte, error) {
    return nil, nil
}

// Generated function to be called to do given work.
type IOWork func() error

func IOWorkNOP() error {
    return nil
}

// I/O can be deferred for data entries.
//
// Instead of performing real operations in the
// storage engine, we generally return a function
// that allows the user to perform real I/O at a
// later point. This is done so that the server
// (for example) could efficiently implement HEAD.
//
// Also, I/O is done efficiently by accessing the
// underlying socket once headers have been pushed
// out. And it happens when no critical locks are
// held.

type deferredIO struct {
    // The key for this entry.
    key string

    // Metadata (is read/written directly).
    metadata []byte

    // The length of the data. Must be fixed.
    length int32

    // The function to read/write the data.
    run IOFunc
}

func (dio *deferredIO) String() string {
    length, padding := dio.usage()
    return fmt.Sprintf("key=%s metadata=%d data_length=%d length=%d padding=%d",
        dio.key, len(dio.metadata), dio.length, length, padding)
}

func newFreeSpace(length int32) *deferredIO {
    return &deferredIO{"", nil, length, IOFuncNOP}
}

func writeMagic(output *os.File) (int64, error) {

    // Write the magic numbers.
    err := binary.Write(output, binary.LittleEndian, magic1)
    if err != nil {
        return int64(0), err
    }
    err = binary.Write(output, binary.LittleEndian, magic2)
    if err != nil {
        return int64(0), err
    }

    // Grab the current offset.
    return output.Seek(0, 1)
}

func readMagic(input *os.File) (int64, error) {

    var test_magic1 int32
    var test_magic2 int32

    // Read the magic numbers.
    err := binary.Read(input, binary.LittleEndian, &test_magic1)
    if err != nil {
        return int64(0), err
    }
    err = binary.Read(input, binary.LittleEndian, &test_magic2)
    if err != nil {
        return int64(0), err
    }

    // Ensure that our magic numbers match.
    if test_magic1 != magic1 || test_magic2 != magic2 {
        return int64(0), invalidMagic
    }

    // Grab the current offset.
    return input.Seek(0, 1)
}

func (dio *deferredIO) usage() (int32, int32) {

    // NOTE: We encode nil as length -1.
    // So the usage is the same as a zero length
    // array, except it's a special encoding that
    // is used internally to signify a delete (as
    // opposed to just setting data to nil).
    key_len := len([]byte(dio.key))
    var metadata_len int
    if dio.metadata == nil {
        metadata_len = 0
    } else {
        metadata_len = len(dio.metadata)
    }

    // Compute the encoded size.
    length := int32Size + int32(key_len) +
        int32Size + int32(dio.length) +
        int32Size + int32(metadata_len)

    // Adjust it to the alignment.
    padding := (alignment - (length % alignment)) % alignment

    return length, padding
}

func clear(output *os.File, offset int64, length int32) (int64, error) {
    utils.Print("STORAGE", "Clearing record: %d bytes.", length)

    // Write our free space header.
    // Note that generally this space includes
    // the bytes required to write this header.
    // Callers are expected to use usage() when
    // calculating how to clear() a section.
    free_space := int32(-length)
    err := binary.Write(output, binary.LittleEndian, free_space)
    if err != nil {
        return offset, err
    }

    if length > 0 {
        // Ensure that the file size is sufficient.
        // We never shrink files. This ensures that we won't
        // trip over anyones toes when we have multiple outstanding
        // reads and writes to the underlying data store.
        _, err := output.Seek(int64(length)-1, 1)
        if err != nil {
            offset, _ := output.Seek(offset, 0)
            return offset, err
        }

        // Write a single byte.
        zero := make([]byte, 1, 1)
        n, err := output.Write(zero)
        if err != nil {
            offset, _ := output.Seek(offset, 0)
            return offset, err
        } else if n == 0 {
            offset, _ := output.Seek(offset, 0)
            return offset, io.ErrUnexpectedEOF
        }
    }

    // Account for the header.
    offset += int64(int32Size)
    offset += int64(length)

    return offset, nil
}

func serialize(output *os.File, offset int64, dio *deferredIO) (int64, IOWork, error) {
    utils.Print("STORAGE", "Writing record: %s => %s @ %d",
        dio.String(), output.Name(), offset)

    // Do the encoding.
    // NOTE: We see the size of the encoding buffer to
    // the size of everything except the actual content.
    length, padding := dio.usage()
    encoded := bytes.NewBuffer(make([]byte, 0, length-dio.length))
    header := bytes.NewBuffer(make([]byte, 0, int32Size))

    // Encode the key.
    key_bytes := []byte(dio.key)
    err := binary.Write(encoded, binary.LittleEndian, int32(len(key_bytes)))
    if err != nil {
        return offset, IOWorkNOP, err
    }
    for n := 0; n < len(key_bytes); {
        written, err := encoded.Write(key_bytes[n:])
        if err != nil {
            return offset, IOWorkNOP, err
        }
        n += written
    }

    // Encode metadata.
    if dio.metadata == nil {
        // See NOTE above in usage().
        // We encode nil as length -1.
        err = binary.Write(encoded, binary.LittleEndian, nilLength)
    } else {
        err = binary.Write(encoded, binary.LittleEndian, int32(len(dio.metadata)))
    }
    if err != nil {
        return offset, IOWorkNOP, err
    }
    for n := 0; n < len(dio.metadata); {
        written, err := encoded.Write(dio.metadata[n:])
        if err != nil {
            return offset, IOWorkNOP, err
        }
        n += written
    }

    // Encode data length.
    // NOTE: This just encodes the data length,
    // not the actual data. This is taken care of
    // indirectly below (through sendfile).
    err = binary.Write(encoded, binary.LittleEndian, int32(dio.length))
    if err != nil {
        return offset, IOWorkNOP, err
    }

    // Simulate a cleared header.
    // This is done first to avoid any race conditions.
    end_offset, err := clear(output, offset, int32(length+padding))
    if err != nil {
        return end_offset, IOWorkNOP, err
    }

    // Seek back to the start.
    _, err = output.Seek(offset+int64(int32Size), 0)
    if err != nil {
        offset, _ = output.Seek(offset, 0)
        return offset, IOWorkNOP, err
    }

    // Write the full buffer.
    // This includes key + metadata + (data length).
    for n := 0; n < len(encoded.Bytes()); {
        written, err := output.Write(encoded.Bytes()[n:])
        if err != nil {
            offset, _ = output.Seek(offset, 0)
            return offset, IOWorkNOP, err
        }
        n += written
    }
    utils.Print("STORAGE", "   key+metadata = %d @ %d",
        len(encoded.Bytes()), offset+int64(int32Size))

    // Encode the header.
    err = binary.Write(header, binary.LittleEndian, int32(length+padding))
    if err != nil {
        offset, _ = output.Seek(offset, 0)
        return offset, IOWorkNOP, err
    }
    header_bytes := header.Bytes()

    // Get the current offset.
    header_offset := offset
    output_offset := offset + int64(int32Size) + int64(len(encoded.Bytes()))
    utils.Print("STORAGE", "   header = %d @ %d",
        len(header.Bytes()), header_offset)
    utils.Print("STORAGE", "   data = %d @ %d",
        dio.length, output_offset)

    // Generate a closure for actually writing the data.
    run := func() error {
        data, err := dio.run(output, &output_offset)
        if err != nil {
            return err
        }

        // Write the data if the run didn't handle it.
        if data != nil {
            if len(data) != int(dio.length) {
                return lengthMismatch
            }
            for n := 0; n < len(data); {
                written, err := syscall.Pwrite(int(output.Fd()), data[n:], output_offset)
                if err != nil {
                    return err
                }
                output_offset += int64(written)
                n += written
            }
        }

        // Write our real header.
        for n := 0; n < len(header_bytes); {
            written, err := syscall.Pwrite(int(output.Fd()), header_bytes[n:], header_offset)
            if err != nil {
                return err
            }
            header_offset += int64(written)
            n += written
        }
        return nil
    }

    // Jump to the end of the record.
    offset, err = output.Seek(end_offset, 0)
    return offset, run, err
}

func generateSplice(input *os.File, length int32, input_offset *int64) IOFunc {

    run := func(output *os.File, output_offset *int64) ([]byte, error) {

        // Need a least one pipe.
        // Pipes should be specified without an offset,
        // otherwise the splice will fail to proceed.

        if input_offset == nil || output_offset == nil {

            for n := 0; n < int(length); {
                // Splice the two files.
                done, err := syscall.Splice(
                    int(input.Fd()), input_offset,
                    int(output.Fd()), output_offset,
                    int(length-int32(n)), 0)

                if (err == io.EOF || err == io.ErrUnexpectedEOF) && (int64(n)+done) == int64(length) {
                    // Everything has been sent.
                } else if err != nil {
                    log.Printf("Error splicing data (%d/%d bytes): %s",
                        int64(n)+done, length, err.Error())
                    return nil, err
                }
                n += int(done)
            }

        } else {
            // Okay, so at this point we've got a run() in the
            // deferred IO object that will do a Splice given
            // another file. Unfortunately, our other file is
            // *not* a pipe -- so we need to do some manually
            // copying of bytes here. This sucks, but again, I
            // hope that this step is *not* in the critical path.

            // Calculate a good buffer size.
            buf_length := length
            if buf_length >= 4096 {
                buf_length = 4096
            }
            buffer := make([]byte, buf_length, buf_length)

            for n := 0; n < int(length); {

                // Read a page.
                read, err := syscall.Pread(int(input.Fd()), buffer, *input_offset)
                if (err == io.EOF || err == io.ErrUnexpectedEOF) && (n+read) == int(length) {
                    // We're still okay, so long as we've got what we need.
                    break
                } else if err != nil {
                    return nil, err
                } else if read == 0 {
                    // Uh-oh, a genuine EOF!
                    return nil, io.ErrUnexpectedEOF
                }

                // Write the page.
                for m := 0; m < read; {
                    written, err := syscall.Pwrite(int(output.Fd()), buffer[m:read], *output_offset)
                    if err != nil {
                        return nil, err
                    }
                    *output_offset += int64(written)
                    m += written
                }

                *input_offset += int64(read)
                n += read
            }
        }

        return nil, nil
    }

    return run
}

func deserialize(input *os.File, start_offset int64) (int64, *deferredIO, error) {
    utils.Print("STORAGE", "Reading record: %s @ %d", input.Name(), start_offset)

    // Read the header.
    length := int32(0)
    offset := start_offset
    err := binary.Read(input, binary.LittleEndian, &length)
    if err != nil {
        return offset, newFreeSpace(0), err
    }
    utils.Print("STORAGE", "   header = %d @ %d", length, offset)
    remaining := int32(length)
    offset += int64(int32Size)

    // Check if it's free space.
    if length <= 0 {
        // Skip ahead past the free space.
        length = -length
        _, err = input.Seek(int64(length), 1)
        return offset + int64(length), newFreeSpace(length), freeSpace
    }

    // Read the key.
    var key_length int32
    var key string

    err = binary.Read(input, binary.LittleEndian, &key_length)
    if err != nil {
        offset, _ := input.Seek(start_offset, 0)
        return offset, newFreeSpace(0), err
    }
    utils.Print("STORAGE", "   key = %d @ %d", key_length, offset)
    remaining -= int32Size
    offset += int64(int32Size)

    key_bytes := make([]byte, key_length, key_length)
    n, err := io.ReadFull(input, key_bytes)
    if (err == io.EOF || err == io.ErrUnexpectedEOF) && n == int(key_length) {
        // Perfect. We got exactly this object.
        err = nil
    } else if err == io.ErrUnexpectedEOF {
        offset, _ := input.Seek(start_offset, 0)
        return offset, newFreeSpace(0), io.EOF
    } else if err != nil {
        offset, _ := input.Seek(start_offset, 0)
        return offset, newFreeSpace(0), err
    }
    key = string(key_bytes)
    remaining -= key_length
    offset += int64(key_length)

    // Read the metadata.
    var metadata_length int32
    var metadata []byte

    err = binary.Read(input, binary.LittleEndian, &metadata_length)
    if err != nil {
        offset, _ := input.Seek(start_offset, 0)
        return offset, newFreeSpace(0), err
    }
    utils.Print("STORAGE", "   metadata = %d @ %d", metadata_length, offset)
    remaining -= int32Size
    offset += int64(int32Size)

    // See NOTE above in usage(). We encode nil as length -1.
    if metadata_length == nilLength {
        metadata = nil
    } else {
        metadata = make([]byte, metadata_length, metadata_length)
        n, err := io.ReadFull(input, metadata)
        if (err == io.EOF || err == io.ErrUnexpectedEOF) && n == int(metadata_length) {
            // Perfect. We got exactly this object.
            err = nil
        } else if err == io.ErrUnexpectedEOF {
            offset, _ := input.Seek(start_offset, 0)
            return offset, newFreeSpace(0), io.EOF
        } else if err != nil {
            offset, _ := input.Seek(start_offset, 0)
            return offset, newFreeSpace(0), err
        }
        remaining -= metadata_length
        offset += int64(metadata_length)
    }

    // Read the data length.
    var data_length int32
    err = binary.Read(input, binary.LittleEndian, &data_length)
    if err != nil {
        offset, _ := input.Seek(start_offset, 0)
        return offset, newFreeSpace(0), err
    }
    remaining -= int32Size
    offset += int64(int32Size)

    // Grab the current offset.
    utils.Print("STORAGE", "   data = %d @ %d", data_length, offset)
    input_offset := offset

    // Construct our function for reading data.
    run := func(output *os.File, output_offset *int64) ([]byte, error) {
        if data_length == nilLength {
            return nil, nil
        }

        if output == nil {
            // Read the file directly.
            data := make([]byte, data_length, data_length)
            for n := 0; n < len(data); {
                read, err := syscall.Pread(int(input.Fd()), data[n:], input_offset)
                if (err == io.EOF || err == io.ErrUnexpectedEOF) && (n+read) == int(data_length) {
                    // This is a really annoying way of passing EOF.
                    // Same as other two cases above, we've read the
                    // object we just happen to be at the end of file.
                } else if err != nil {
                    log.Printf("Error decoding data (%d/%d bytes read): %s",
                        n+read, data_length, err.Error())
                    return nil, err
                }
                input_offset += int64(read)
                n += read
            }
            return data, nil
        }

        // Use sendfile to send the file.
        return generateSplice(input, data_length, &input_offset)(output, output_offset)
    }

    // Seek to the end of the record.
    _, err = input.Seek(int64(remaining), 1)
    if err != nil {
        offset, _ := input.Seek(start_offset, 0)
        return offset, nil, err
    }
    offset += int64(remaining)

    // Construct our deferredio.
    dio := &deferredIO{key, metadata, data_length, run}
    utils.Print("STORAGE", "Loaded record: %s.", dio.String())
    return offset, dio, nil
}
