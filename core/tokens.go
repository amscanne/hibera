package core

import (
    "sync"
    "strings"
    "regexp"
    "hibera/utils"
)

type Token struct {
    // The authorization token.
    Id string

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

func NewToken(path string, perms string, rev Revision) *Token {
    token := new(Token)
    token.Id, _ = utils.Uuid()
    token.Path = path
    token.Read = strings.Contains(perms, "r")
    token.Write = strings.Contains(perms, "w")
    token.Execute = strings.Contains(perms, "x")
    return token
}

type Tokens struct {
    // The map of all tokens.
    all map[string]*Token

    sync.Mutex
}

func (tokens *Tokens) Generate(path string, perms string, rev Revision) (string, error) {
    token := NewToken(path, perms, rev)
    token.regex, _ = regexp.Compile(token.Path)
    tokens.Mutex.Lock()
    defer tokens.Mutex.Unlock()
    tokens.all[token.Id] = token
    return token.Id, nil
}

func (tokens *Tokens) Revoke(id string) {
    delete(tokens.all, id)
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
            token.regex, _ = regexp.Compile(token.Path)
            tokens.all[id] = token
        }
    }

    return nil
}

func NewTokens() *Tokens {
    tokens := new(Tokens)
    tokens.all = make(map[string]*Token)
    return tokens
}
