package storage

import (
    "bytes"
    "encoding/binary"
    "errors"
    "io"
    "log"
    "os"
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
var int32_size = uint64(4)

func writeMagic(output *os.File) (int64, error) {
    err := binary.Write(output, binary.LittleEndian, magic1)
    if err != nil {
        return -1, err
    }
    err = binary.Write(output, binary.LittleEndian, magic2)
    if err != nil {
        return -1, err
    }

    // Grab the current offset.
    return output.Seek(0, 1)
}

func readMagic(input *os.File) (int64, error) {
    var test_magic1 int32
    var test_magic2 int32
    err := binary.Read(input, binary.LittleEndian, &test_magic1)
    if err != nil {
        return -1, err
    }
    err = binary.Read(input, binary.LittleEndian, &test_magic2)
    if err != nil {
        return -1, err
    }

    // Ensure that our magic numbers match.
    if test_magic1 != magic1 || test_magic2 != magic2 {
        return -1, errors.New("invalid magic number")
    }

    // Grab the current offset.
    return input.Seek(0, 1)
}

func usage(ent *entry) uint64 {
    // Return the length including header.
    if ent == nil {
        return int32_size
    }

    // NOTE: We encode nil as length -1.
    // So the usage is the same as a zero length
    // array, except it's a special encoding that
    // is used internally to signify a delete (as
    // opposed to just setting data to nil).
    var data_len int
    if ent.value.data == nil {
        data_len = 0
    } else {
        data_len = len(ent.value.data)
    }

    var metadata_len int
    if ent.value.metadata == nil {
        metadata_len = 0
    } else {
        metadata_len = len(ent.value.metadata)
    }

    return (int32_size +
        int32_size + uint64(len([]byte(ent.key))) +
        int32_size + uint64(data_len) +
        int32_size + uint64(metadata_len))
}

func clear(output *os.File, length uint64) error {

    // Write our free space header.
    // Note that generally this space includes
    // the bytes required to write this header.
    // Callers are expected to use Usage() when
    // calculating how to clear() a section.
    free_space := int32(-length)
    err := binary.Write(output, binary.LittleEndian, free_space)
    if err != nil {
        log.Print("Error writing free space: ", err)
        return err
    }

    return nil
}

func serialize(output *os.File, ent *entry) error {

    // Get the current offset.
    offset, err := output.Seek(0, 1)
    if err != nil {
        log.Print("Error seeking: ", err)
        return err
    }

    // Do the encoding.
    encoded := bytes.NewBuffer(make([]byte, 0))

    doEncode := func(data []byte) error {
        if data == nil {
            // See NOTE above in usage().
            // We encode nil as length -1.
            return binary.Write(encoded, binary.LittleEndian, int32(-1))
        }
        err := binary.Write(encoded, binary.LittleEndian, int32(len(data)))
        if err != nil {
            log.Print("Error encoding length: ", err)
            return err
        }
        for n := 0; n < len(data); {
            written, err := encoded.Write(data[n:])
            if err != nil {
                log.Print("Error encoding data: ", err)
                return err
            }
            n += written
        }
        return nil
    }

    err = doEncode([]byte(ent.key))
    if err != nil {
        return err
    }

    err = doEncode(ent.value.data)
    if err != nil {
        return err
    }

    err = doEncode(ent.value.metadata)
    if err != nil {
        return err
    }

    // Simulate a cleared header.
    err = clear(output, usage(ent))
    if err != nil {
        return err
    }

    // Write the full buffer.
    for n := 0; n < len(encoded.Bytes()); {
        written, err := output.Write(encoded.Bytes()[n:])
        if err != nil {
            log.Print("Error writing full entry: ", err)
            return err
        }
        n += written
    }

    // Return the original offset.
    offset, err = output.Seek(offset, 0)
    if err != nil {
        log.Print("Error seeking: ", err)
        return err
    }

    // Write our real header.
    err = binary.Write(output, binary.LittleEndian, int32(encoded.Len()))
    if err != nil {
        log.Print("Error writing entry length: ", err)
        return err
    }

    // Go to the end of the record.
    offset, err = output.Seek(int64(encoded.Len()), 1)
    if err != nil {
        log.Print("Error seeking: ", err)
    }
    return err
}

func deserialize(input *os.File, ent *entry) (uint64, uint64, error) {

    // Read the header.
    length := int32(0)
    err := binary.Read(input, binary.LittleEndian, &length)
    if err != nil {
        return uint64(0), uint64(0), err
    }

    // Check if it's free space.
    if length <= 0 {
        // Skip ahead past the free space.
        length = -length
        _, err = input.Seek(int64(length-4), 1)
        return uint64(0), uint64(length), err
    }

    // Read the object.
    data := make([]byte, length, length)
    n, err := io.ReadFull(input, data)
    if (err == io.EOF || err == io.ErrUnexpectedEOF) && n == int(length) {
        // Perfect. We got exactly this object.
        err = nil
    } else if err == io.ErrUnexpectedEOF {
        return uint64(length), uint64(0), io.EOF
    } else if err != nil {
        return uint64(length), uint64(0), err
    }

    // Do the decoding.
    encoded := bytes.NewBuffer(data)
    doDecode := func() ([]byte, error) {
        var length int32
        err := binary.Read(encoded, binary.LittleEndian, &length)
        if err == io.EOF {
            // Okay. We still read the object,
            // other it would be ErrUnexpectedEOF.
        } else if err != nil {
            log.Print("Error decoding length: ", err)
            return nil, err
        }
        if length == int32(-1) {
            // See NOTE above in usage().
            // We encode nil as length -1.
            return nil, nil
        }
        blob := make([]byte, length, length)
        for n := 0; n < len(blob); {
            read, err := encoded.Read(blob[n:])
            if (err == io.EOF || err == io.ErrUnexpectedEOF) && (n+read) == int(length) {
                // This is a really annoying way of passing EOF.
                // Same as other two cases above, we've read the
                // object we just happen to be at the end of file.
            } else if err != nil {
                log.Printf("Error decoding data (%d/%d bytes read): %s",
                    n+read, length, err.Error())
                return nil, err
            }
            n += read
        }
        return blob, nil
    }

    key_bytes, err := doDecode()
    if err != nil {
        return uint64(length), uint64(0), err
    }
    ent.key = key(key_bytes)

    ent.value.data, err = doDecode()
    if err != nil {
        return uint64(length), uint64(0), err
    }

    ent.value.metadata, err = doDecode()
    if err != nil {
        return uint64(length), uint64(0), err
    }

    return uint64(length), uint64(0), nil
}
