package core

import (
    "regexp"
    "sync"
)

type Access struct {
    // The map of all access.
    all     AccessInfo
    allLock sync.RWMutex

    // Proposals for tokens.
    proposed     AccessInfo
    proposedLock sync.RWMutex

    // The default admin authorization.
    auth string
}

func (access *Access) Check(key Key, auth string, read bool, write bool, execute bool) bool {
    access.allLock.RLock()
    defer access.allLock.RUnlock()

    // If no namespace permissions are defined,
    // check this authentication token against the
    // global token. This allows the global token
    // to manipulate unconfigured namespaces.
    token, ok := access.all[key.Namespace][auth]
    if !ok {
        return auth == access.auth
    }

    // Check the paths against the current.
    for _, perms := range token {
        // Check the specific operations.
        if perms.re != nil && perms.re.Match([]byte(key.Key)) {
            if (!read || perms.Read) && (!write || perms.Write) || (!execute || perms.Execute) {
                return true
            }
        }
    }

    // No match found.
    return false
}

func (access *Access) List(ns Namespace) []string {
    access.allLock.RLock()
    defer access.allLock.RUnlock()

    // Namespace does not exist.
    if _, ok := access.all[ns]; !ok {
        return make([]string, 0, 0)
    }

    // List all available tokens.
    res := make([]string, len(access.all[ns]), len(access.all[ns]))
    i := 0
    for auth, _ := range access.all[ns] {
        res[i] = auth
        i += 1
    }
    return res
}

func (access *Access) Get(auth Key) (*Token, error) {
    access.allLock.RLock()
    defer access.allLock.RUnlock()

    // Namespace does not exist.
    if _, ok := access.all[auth.Namespace]; !ok {
        return nil, nil
    }

    // Get associated paths.
    token, ok := access.all[auth.Namespace][auth.Key]
    if !ok {
        return nil, nil
    }
    return &token, nil
}

func (access *Access) Update(auth Key, path string, read bool, write bool, execute bool) error {
    access.proposedLock.Lock()
    defer access.proposedLock.Unlock()

    // Ensure the namespace exists.
    if _, ok := access.proposed[auth.Namespace]; !ok {
        access.proposed[auth.Namespace] = make(NamespaceAccess)
    }

    // Add the proposal.
    if _, ok := access.proposed[auth.Namespace][auth.Key]; !ok {
        access.proposed[auth.Namespace][auth.Key] = make(Token)
    }
    access.proposed[auth.Namespace][auth.Key][path] = Perms{read, write, execute, nil}
    return nil
}

func (access *Access) Encode(next bool, info AccessInfo) (bool, error) {
    access.allLock.RLock()
    access.proposedLock.RLock()
    defer access.allLock.RUnlock()
    defer access.proposedLock.RUnlock()

    changed := false

    if next && len(access.proposed) == 0 {
        return changed, nil
    }

    for ns, na := range access.all {
        info[ns] = make(NamespaceAccess)
        for auth, token := range na {
            info[ns][auth] = token
        }
    }

    if next {
        for ns, na := range access.proposed {
            if _, ok := info[ns]; !ok {
                info[ns] = make(NamespaceAccess)
            }
            for auth, token := range na {
                info[ns][auth] = token
                changed = true
            }
        }
    }

    return changed, nil
}

func (access *Access) Decode(info AccessInfo) error {
    access.allLock.Lock()
    access.proposedLock.Lock()
    defer access.allLock.Unlock()
    defer access.proposedLock.Unlock()

    // Update all access tokens.
    for ns, na := range info {
        for auth, token := range na {

            if len(token) == 0 {
                // Process a delete request.
                if _, ok := access.all[ns]; ok {
                    delete(access.all[ns], auth)
                }

            } else {
                // Process a normal request.
                if _, ok := access.all[ns]; !ok {
                    access.all[ns] = make(NamespaceAccess)
                }
                if _, ok := access.all[ns][auth]; !ok {
                    access.all[ns][auth] = make(Token)
                }
                for tokpath, perms := range token {
                    if !perms.Read && !perms.Write && !perms.Execute {
                        delete(access.all[ns][auth], tokpath)
                    } else {
                        var err error
                        perms.re, err = regexp.Compile(tokpath)
                        if err != nil {
                            continue
                        }
                        access.all[ns][auth][tokpath] = perms
                    }
                }
                if len(access.all[ns][auth]) == 0 {
                    delete(access.all[ns], auth)
                }
            }

            // Clear out the namespace if it's empty.
            if len(access.all[ns]) == 0 {
                delete(access.all, ns)
            }

            // Clear out the relevant proposed bits.
            if _, ok := access.proposed[ns]; ok {
                delete(access.proposed[ns], auth)
                if len(access.proposed[ns]) == 0 {
                    delete(access.proposed, ns)
                }
            }
        }
    }

    return nil
}

func (access *Access) Reset() {
    access.allLock.Lock()
    access.proposedLock.Lock()
    defer access.allLock.Unlock()
    defer access.proposedLock.Unlock()

    // Setup the basic maps.
    access.all = make(AccessInfo)
    access.proposed = make(AccessInfo)
}

func NewAccess(auth string) *Access {
    access := new(Access)
    access.auth = auth
    access.Reset()
    return access
}
