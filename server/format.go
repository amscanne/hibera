package server

import (
    "github.com/amscanne/hibera/core"
)

type messageType int

var pingMessage = messageType(0)
var pongMessage = messageType(1)

type message struct {
    Type     messageType   `json:"type"`
    Revision core.Revision `json:"revision"`
    Id       string        `json:"id"`
    URL      string        `json:"url"`
    Dead     []string      `json:"dead"`
}
