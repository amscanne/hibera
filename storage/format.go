package storage

import (
    "bytes"
    "encoding/binary"
    "io"
    "log"
    "os"
)

func (ent *entry) Length() uint64 {
    // Return the encoded length.
    return uint64(
        4 + len([]byte(ent.key)) +
            4 + len(ent.value.data) +
            4 + len(ent.value.metadata))
}

func (ent *entry) Usage() uint64 {
    // Return the length including header.
    return 4 + ent.Length()
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
        return err
    }

    // Do the encoding.
    encoded := bytes.NewBuffer(make([]byte, 0))

    doEncode := func(data []byte) error {
        err := binary.Write(encoded, binary.LittleEndian, uint32(len(data)))
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
    err = clear(output, ent.Usage())
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
    return err
}

func deserialize(input *os.File, ent *entry) (uint64, uint64, error) {

    // Read the header.
    length := int32(0)
    err := binary.Read(input, binary.LittleEndian, &length)
    if err == io.ErrUnexpectedEOF {
        return uint64(0), uint64(0), io.EOF
    } else if err != nil {
        if err != io.EOF {
            log.Print("Error reading entry header: ", err)
        }
        return uint64(0), uint64(0), err
    }

    // Check if it's free space.
    if length <= 0 {
        // Skip ahead past the free space.
        _, err = input.Seek(int64(-length)-4, 1)
        return uint64(0), uint64(length), err
    }

    // Read the object.
    data := make([]byte, length, length)
    n, err := io.ReadFull(input, data)
    if (err == io.EOF || err == io.ErrUnexpectedEOF) && n == int(length) {
        // Perfect. We got exactly this object.
    } else if err != nil {
        log.Print("Error reading full entry: ", err)
        return uint64(length), uint64(0), err
    }

    // Do the decoding.
    encoded := bytes.NewBuffer(data)
    doDecode := func() ([]byte, error) {
        var length uint32
        err := binary.Read(encoded, binary.LittleEndian, &length)
        if err != nil {
            log.Print("Error decoding length: ", err)
            return nil, err
        }
        blob := make([]byte, length, length)
        for n := 0; n < len(blob); {
            read, err := encoded.Read(blob[n:])
            if err != nil {
                log.Print("Error decoding data: ", err)
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
