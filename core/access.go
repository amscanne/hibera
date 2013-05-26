package core

import (
    "sync"
    "regexp"
)

type Perms struct {
    // Permissions.
    Read bool
    Write bool
    Execute bool

    // Cached regular expression.
    re *regexp.Regexp
}

type Access struct {
    // The map of all access.
    all AccessInfo

    // Proposed tokens.
    proposed AccessInfo

    // The default admin authorization.
    auth string

    sync.Mutex
}

func (access *Access) Check(id string, path string, read bool, write bool, execute bool) bool {
    token := access.all[id]
    if token == nil {
        return false
    }
    for tokpath, perms := range *token {
        if perms.re == nil {
            var err error
            perms.re, err = regexp.Compile(tokpath)
            if err != nil {
                continue
            }
        }
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

func (access *Access) Get(id string) (*Token, error) {
    token := access.all[id]
    if token == nil {
        return nil, nil
    }
    return token, nil
}

func (access *Access) Update(id string, path string, read bool, write bool, execute bool) error {
    if access.proposed[id] == nil {
        token := make(Token)
        access.proposed[id] = &token
    }
    (*access.proposed[id])[path] = Perms{read, write, execute, nil}
    return nil
}

func (access *Access) Remove(id string) {
    access.proposed[id] = nil
}

func (access *Access) Encode(next bool, na AccessInfo) error {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()

    if next && len(access.proposed) == 0 {
        return nil
    }

    if next {
        for id, token := range access.proposed {
            na[id] = token
        }

    } else {
        for id, token := range access.all {
            na[id] = token
        }
    }

    return nil
}

func (access *Access) Decode(na AccessInfo) error {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()

    // Update all access tokens.
    for id, token := range na {
        if token == nil || len(*token) == 0 {
            delete(access.all, id)
        } else {
            if access.all[id] == nil {
                token := make(Token)
                access.all[id] = &token
            }
            for tokpath, perms := range *token {
                if !perms.Read && !perms.Write && !perms.Execute {
                    delete(*(access.all[id]), tokpath)
                } else {
                    (*access.all[id])[tokpath] = perms
                }
            }
            if len(*access.all[id]) == 0 {
                delete(access.all, id)
            }
        }

        delete(access.proposed, id)
    }

    return nil
}

func (access *Access) Reset() {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()
    access.all = make(AccessInfo)
    token := make(Token)
    access.all[access.auth] = &token
    (*access.all[access.auth])[".*"] = Perms{true, true, true, nil}
    access.proposed = make(AccessInfo)
}

func NewAccess(auth string) *Access {
    access := new(Access)
    access.auth = auth
    access.Reset()
    return access
}
