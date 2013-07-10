package core

import (
    "regexp"
    "sync"
)

type Perms struct {
    // Permissions.
    Read    bool
    Write   bool
    Execute bool

    // Cached regular expression.
    re  *regexp.Regexp
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

func (access *Access) Check(ns Namespace, id string, path string, read bool, write bool, execute bool) bool {
    // If no namespace permissions are defined,
    // check this authentication token against the
    // global token. This allows the global token
    // to manipulate unconfigured namespaces.
    if _, ok := access.all[ns]; !ok {
        return id == access.auth
    }

    // Lookup the token.
    token, ok := access.all[ns][id]
    if !ok {
        return id == access.auth
    }

    // Check the paths against the current.
    for tokpath, perms := range token {
        // (Lazily) build the regular expression.
        if perms.re == nil {
            var err error
            perms.re, err = regexp.Compile(tokpath)
            if err != nil {
                continue
            }
        }
        // Check the specific operations.
        if perms.re.Match([]byte(path)) {
            if (!read || perms.Read) && (!write || perms.Write) || (!execute || perms.Execute) {
                return true
            }
        }
    }

    // No match found.
    return false
}

func (access *Access) List(ns Namespace) []string {
    // Namespace does not exist.
    if _, ok := access.all[ns]; !ok {
        return make([]string, 0, 0)
    }

    // List all available tokens.
    res := make([]string, len(access.all[ns]), len(access.all[ns]))
    i := 0
    for id, _ := range access.all[ns] {
        res[i] = id
        i += 1
    }
    return res
}

func (access *Access) Get(ns Namespace, id string) (*Token, error) {
    // Namespace does not exist.
    if _, ok := access.all[ns]; !ok {
        return nil, nil
    }

    // Get associated paths.
    token, ok := access.all[ns][id]
    if !ok {
        return nil, nil
    }
    return &token, nil
}

func (access *Access) Update(ns Namespace, id string, path string, read bool, write bool, execute bool) error {
    // Ensure the namespace exists.
    if _, ok := access.proposed[ns]; !ok {
        access.proposed[ns] = make(NamespaceAccess)
    }

    // Add the proposal.
    if _, ok := access.proposed[ns][id]; !ok {
        access.proposed[ns][id] = make(Token)
    }
    access.proposed[ns][id][path] = Perms{read, write, execute, nil}
    return nil
}

func (access *Access) Remove(ns Namespace, id string) {
    // Ensure the namespace exists.
    if _, ok := access.proposed[ns]; !ok {
        access.proposed[ns] = make(NamespaceAccess)
    }

    // Add the proposal.
    access.proposed[ns][id] = nil
}

func (access *Access) Encode(next bool, info AccessInfo) (bool, error) {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()

    changed := false

    if next && len(access.proposed) == 0 {
        return changed, nil
    }

    for ns, na := range access.all {
        info[ns] = make(NamespaceAccess)
        for id, token := range na {
            info[ns][id] = token
        }
    }

    if next {
        for ns, na := range access.proposed {
            if _, ok := info[ns]; !ok {
                info[ns] = make(NamespaceAccess)
            }
            for id, token := range na {
                info[ns][id] = token
                changed = true
            }
        }
    }

    return changed, nil
}

func (access *Access) Decode(info AccessInfo) error {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()

    // Update all access tokens.
    for ns, na := range info {
        for id, token := range na {

            // Process a delete request.
            if len(token) == 0 {
                if _, ok := access.all[ns]; ok {
                    delete(access.all[ns], id)
                }

                // Process a normal request.
            } else {
                if _, ok := access.all[ns]; !ok {
                    access.all[ns] = make(NamespaceAccess)
                }
                if _, ok := access.all[ns][id]; !ok {
                    access.all[ns][id] = make(Token)
                }
                for tokpath, perms := range token {
                    if !perms.Read && !perms.Write && !perms.Execute {
                        delete(access.all[ns][id], tokpath)
                    } else {
                        access.all[ns][id][tokpath] = perms
                    }
                }
                if len(access.all[ns][id]) == 0 {
                    delete(access.all[ns], id)
                }
            }

            // Clear out the namespace if it's empty.
            if len(access.all[ns]) == 0 {
                delete(access.all, ns)
            }

            // Clear out the relevant proposed bits.
            if _, ok := access.proposed[ns]; ok {
                delete(access.proposed[ns], id)
                if len(access.proposed[ns]) == 0 {
                    delete(access.proposed, ns)
                }
            }
        }
    }

    return nil
}

func (access *Access) Reset() {
    access.Mutex.Lock()
    defer access.Mutex.Unlock()

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
