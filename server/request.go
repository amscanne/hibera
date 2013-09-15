package server

import (
    "hibera/core"
)

type Request struct {
    *Connection

    auth     core.Token
    ns       core.Namespace
    notifier <-chan bool
}

func NewRequest(conn *Connection, auth core.Token, ns core.Namespace, notify <-chan bool) *Request {
    return &Request{conn, auth, ns, notify}
}

func (r *Request) Auth() core.Token {
    return r.auth
}

func (r *Request) Namespace() core.Namespace {
    return r.ns
}

func (r *Request) Notifier() <-chan bool {
    return r.notifier
}
