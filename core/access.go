package core

import (
    "bytes"
    "strings"
    "encoding/json"
    "sync"
    "regexp"
)

type Perms struct {
    re *regexp.Regexp

    // Permissions.
    Read bool
    Write bool
    Execute bool
}

type Token map[string]Perms

func OneToken(path string, read bool, write bool, execute bool) *Token {
    token := make(Token)
    if path != "" && (read || write || execute) {
        re, _ := regexp.Compile(path)
        token[path] = Perms{re, read, write, execute}
    }
    return &token
}

func (token *Token) toMap() map[string]string {
    output := make(map[string]string)
    for path, perms := range *token {
        str := ""
        if perms.Read {
            str += "r"
        }
        if perms.Write {
            str += "w"
        }
        if perms.Execute {
            str += "x"
        }
        output[path] = str
    }
    return output
}

func fromMap(input map[string]string) *Token {
    token := make(Token)
    for path, perms := range input {
        read := strings.Contains(perms, "r")
        write := strings.Contains(perms, "w")
        execute := strings.Contains(perms, "x")
        re, _ := regexp.Compile(path)
        token[path] = Perms{re, read, write, execute}
    }
    return &token
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
    if token == nil {
        return false
    }
    for path, perms := range *token {
        if perms.re.Match([]byte(path)) {
            if (!read || perms.Read) && (!write || perms.Write) || (!execute || perms.Execute) {
                return true
            }
        }
    }
    return false
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
    output := token.toMap()
    buf := new(bytes.Buffer)
    enc := json.NewEncoder(buf)
    err := enc.Encode(&output)
    return buf.Bytes(), err
}

func (access *Access) Set(id string, value []byte) error {
    var input map[string]string
    buf := bytes.NewBuffer(value)
    dec := json.NewDecoder(buf)
    err := dec.Decode(&input)
    if err != nil {
        return err
    }
    access.proposed[id] = fromMap(input)
    return nil
}

func (access *Access) Remove(id string) {
    access.proposed[id] = OneToken(".*", false, false, false)
}

func (access *Access) Encode(next bool, na map[string]map[string]string) error {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()

    if next && len(access.proposed) == 0 {
        return nil
    }

    if next {
        for id, token := range access.proposed {
            na[id] = token.toMap()
        }
    } else {
        for id, token := range access.all {
            na[id] = token.toMap()
        }
    }

    return nil
}

func (access *Access) Decode(na map[string]map[string]string) error {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()

    // Update all access tokens.
    for id, token := range na {
        if len(token) == 0 {
            delete(access.all, id)
        } else {
            access.all[id] = fromMap(token)
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
    access.all[access.auth] = OneToken(".*", true, true, true)
}

func NewAccess(auth string) *Access {
    access := new(Access)
    access.auth = auth
    access.Reset()
    return access
}
