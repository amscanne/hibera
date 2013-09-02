package storage

// Keys are simple strings.
type key string

// Values are a pair of (data, metadata) byte arrays.
type value struct {
    data     []byte
    metadata []byte
}

// An entry is an association between a key and a value.
type entry struct {
    key
    value
}
