package server

import (
    "hibera/core"
)

type Request struct {
    *Connection

    auth core.Token
    ns   core.Namespace
}

func NewRequest(conn *Connection, auth core.Token, ns core.Namespace) *Request {
    return &Request{conn, auth, ns}
}

func (r *Request) Auth() core.Token {
    return r.auth
}

func (r *Request) Namespace() core.Namespace {
    return r.ns
}
