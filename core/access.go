package core

import (
    "bytes"
    "encoding/json"
    "sync"
    "regexp"
)

type Token struct {
    // The authorization token.
    id string

    // The regular expression.
    Path string
    regex *regexp.Regexp

    // The permissions.
    Read bool
    Write bool
    Execute bool

    // The last revision modified.
    Modified Revision
}

func NewToken(path string, read bool, write bool, execute bool, rev Revision) *Token {
    token := new(Token)
    token.Path = path
    token.Read = read
    token.Write = write
    token.Execute = execute
    token.regex, _ = regexp.Compile(token.Path)
    return token
}

type Access struct {
    // The map of all access.
    all map[string]*Token
    proposed map[string]*Token

    // The default admin authorization.
    auth string

    sync.Mutex
}

func (access *Access) Check(id string, path string, read bool, write bool, execute bool) bool {
    token := access.all[id]
    if token == nil || token.regex == nil {
        return false
    }
    if (!token.Read && read) || (!token.Write && write) || (!token.Execute && execute) {
        return false
    }
    return token.regex.Match([]byte(path))
}

func (access *Access) List() []string {
    res := make([]string, len(access.all), len(access.all))
    i := 0
    for id, _ := range access.all {
        res[i] = id
        i += 1
    }
    return res
}

func (access *Access) Get(id string) ([]byte, error) {
    token := access.all[id]
    buf := new(bytes.Buffer)
    enc := json.NewEncoder(buf)
    err := enc.Encode(&token)
    return buf.Bytes(), err
}

func (access *Access) Set(id string, value []byte) error {
    var token Token
    buf := bytes.NewBuffer(value)
    dec := json.NewDecoder(buf)
    err := dec.Decode(&token)
    if err != nil {
        return err
    }
    access.proposed[id] = &token
    return nil
}

func (access *Access) Remove(id string, rev Revision) {
    access.proposed[id] = NewToken("", false, false, false, rev)
}

func (access *Access) Encode(rev Revision, next bool, na map[string]*Token) error {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()

    // Create a list of access modified after rev.
    for id, token := range access.all {
        if token.Modified >= rev {
            na[id] = token
        }
    }
    for id, token := range access.proposed {
        na[id] = token
    }

    return nil
}

func (access *Access) Decode(na map[string]*Token) error {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()

    // Update all access with revs > Modified.
    for id, token := range na {
        if access.all[id] == nil ||
            access.all[id].Modified < token.Modified {
            if token.Read || token.Write || token.Execute {
                token.regex, _ = regexp.Compile(token.Path)
                access.all[id] = token
            } else {
                delete(access.all, id)
            }
        }

        // Remove any proposals.
        delete(access.proposed, id)
    }

    return nil
}

func (access *Access) Reset() {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()
    access.all = make(map[string]*Token)
    access.all[access.auth] = NewToken(".*", true, true, true, Revision(0))
}

func NewAccess(auth string) *Access {
    access := new(Access)
    access.auth = auth
    access.Reset()
    return access
}
