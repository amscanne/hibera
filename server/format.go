package server

import (
    "hibera/core"
)

type messageType int

var pingMessage = messageType(0)
var pongMessage = messageType(1)

type message struct {
    Type messageType `json:"type"`
    Revision core.Revision `json:"revision"`
    Id string `json:"id"`
    Dead []string `json:"dead"`
}
