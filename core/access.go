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
    root Token
}

func (access *Access) Check(ns Namespace, auth Token, key Key, read bool, write bool, execute bool) bool {
    access.allLock.RLock()
    defer access.allLock.RUnlock()

    // Disallow access to unconfigured namespaces.
    // This is because we delete keys in all namespace
    // that don't exist in order to prevent lost data.
    // You can always create the namespace with zero
    // permission bits on some dumb token.
    if ns != Namespace("") {
        _, exists := access.all[ns]
        if !exists {
            return false
        }
    }

    // Check for the root token.
    // This is the only way that new namespaces
    // can be created and destroyed.
    if auth == access.root {
        return true
    }

    // Grab the token.
    token, exists := access.all[ns][auth]
    if !exists {
        return false
    }

    // Check the paths against the current.
    for _, perms := range token {
        // Check the specific operations.
        if perms.re != nil && perms.re.Match([]byte(key)) {
            if (!read || perms.Read) && (!write || perms.Write) && (!execute || perms.Execute) {
                return true
            }
        }
    }

    // No match found.
    return false
}

func (access *Access) Has(ns Namespace) bool {
    access.allLock.RLock()
    defer access.allLock.RUnlock()

    if ns == Namespace("") {
        return true
    }

    _, ok := access.all[ns]
    return ok
}

func (access *Access) List(ns Namespace) []Token {
    access.allLock.RLock()
    defer access.allLock.RUnlock()

    // Namespace does not exist.
    if _, ok := access.all[ns]; !ok {
        return make([]Token, 0, 0)
    }

    // List all available tokens.
    res := make([]Token, len(access.all[ns]), len(access.all[ns]))
    i := 0
    for auth, _ := range access.all[ns] {
        res[i] = auth
        i += 1
    }
    return res
}

func (access *Access) Get(ns Namespace, auth Token) (*Permissions, error) {
    access.allLock.RLock()
    defer access.allLock.RUnlock()

    // Namespace does not exist.
    if _, ok := access.all[ns]; !ok {
        return nil, nil
    }

    // Get associated paths.
    token, ok := access.all[ns][auth]
    if !ok {
        return nil, nil
    }
    return &token, nil
}

func (access *Access) Update(ns Namespace, auth Token, key Key, read bool, write bool, execute bool) error {
    access.proposedLock.Lock()
    defer access.proposedLock.Unlock()

    // Ensure the namespace exists.
    if _, ok := access.proposed[ns]; !ok {
        access.proposed[ns] = make(NamespaceAccess)
    }

    // Add the proposal.
    if _, ok := access.proposed[ns][auth]; !ok {
        access.proposed[ns][auth] = make(Permissions)
    }
    access.proposed[ns][auth][string(key)] = PermissionInfo{read, write, execute, nil}
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
                    access.all[ns][auth] = make(Permissions)
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

func NewAccess(root Token) *Access {
    access := new(Access)
    access.root = root
    access.Reset()
    return access
}
