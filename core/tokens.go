package core

import (
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

type Tokens struct {
    // The map of all tokens.
    all map[string]*Token

    sync.Mutex
}

func (tokens *Tokens) Check(id string, path string, read bool, write bool, execute bool) bool {
    token := tokens.all[id]
    if token == nil || token.regex == nil {
        return false
    }
    if (!token.Read && read) || (!token.Write && write) || (!token.Execute && execute) {
        return false
    }
    return token.regex.Match([]byte(path))
}

func (tokens *Tokens) Encode(rev Revision, na map[string]*Token) error {
    tokens.Mutex.Lock()
    defer tokens.Mutex.Unlock()

    // Create a list of tokens modified after rev.
    for id, token := range tokens.all {
        if token.Modified >= rev {
            na[id] = token
        }
    }
    return nil
}

func (tokens *Tokens) Decode(na map[string]*Token) error {
    tokens.Mutex.Lock()
    defer tokens.Mutex.Unlock()

    // Update all tokens with revs > Modified.
    for id, token := range na {
        if tokens.all[id] == nil ||
            tokens.all[id].Modified < token.Modified {
            if token.Read || token.Write || token.Execute {
                token.regex, _ = regexp.Compile(token.Path)
                tokens.all[id] = token
            } else {
                delete(tokens.all, id)
            }
        }
    }

    return nil
}

func (tokens *Tokens) Reset() {
    tokens.Mutex.Lock()
    defer tokens.Mutex.Unlock()
    tokens.all = make(map[string]*Token)
}

func NewTokens(auth string) *Tokens {
    tokens := new(Tokens)
    tokens.all = make(map[string]*Token)
    tokens.all[auth] = NewToken(".*", true, true, true, Revision(0))
    return tokens
}
