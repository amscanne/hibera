// Code generated by protoc-gen-go.
// source: src/hibera/server/gossip.proto
// DO NOT EDIT!

package server

import proto "code.google.com/p/goprotobuf/proto"
import json "encoding/json"
import math "math"

// Reference proto, json, and math imports to suppress error if they are not otherwise used.
var _ = proto.Marshal
var _ = &json.SyntaxError{}
var _ = math.Inf

type TYPE int32

const (
	TYPE_PING      TYPE = 0
	TYPE_PONG      TYPE = 1
	TYPE_PROPOSE   TYPE = 2
	TYPE_PROMISE   TYPE = 3
	TYPE_NOPROMISE TYPE = 4
	TYPE_ACCEPT    TYPE = 5
	TYPE_ACCEPTED  TYPE = 6
)

var TYPE_name = map[int32]string{
	0: "PING",
	1: "PONG",
	2: "PROPOSE",
	3: "PROMISE",
	4: "NOPROMISE",
	5: "ACCEPT",
	6: "ACCEPTED",
}
var TYPE_value = map[string]int32{
	"PING":      0,
	"PONG":      1,
	"PROPOSE":   2,
	"PROMISE":   3,
	"NOPROMISE": 4,
	"ACCEPT":    5,
	"ACCEPTED":  6,
}

func (x TYPE) Enum() *TYPE {
	p := new(TYPE)
	*p = x
	return p
}
func (x TYPE) String() string {
	return proto.EnumName(TYPE_name, int32(x))
}
func (x TYPE) MarshalJSON() ([]byte, error) {
	return json.Marshal(x.String())
}
func (x *TYPE) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(TYPE_value, data, "TYPE")
	if err != nil {
		return err
	}
	*x = TYPE(value)
	return nil
}

type Gossip struct {
	Id               *string  `protobuf:"bytes,1,req,name=id" json:"id,omitempty"`
	Dead             []string `protobuf:"bytes,2,rep,name=dead" json:"dead,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (this *Gossip) Reset()         { *this = Gossip{} }
func (this *Gossip) String() string { return proto.CompactTextString(this) }
func (*Gossip) ProtoMessage()       {}

func (this *Gossip) GetId() string {
	if this != nil && this.Id != nil {
		return *this.Id
	}
	return ""
}

func (this *Gossip) GetDead() []string {
	if this != nil {
		return this.Dead
	}
	return nil
}

type Message struct {
	Type             *uint32 `protobuf:"varint,1,req,name=type" json:"type,omitempty"`
	Version          *uint64 `protobuf:"varint,2,req,name=version" json:"version,omitempty"`
	Gossip           *Gossip `protobuf:"bytes,5,opt,name=gossip" json:"gossip,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (this *Message) Reset()         { *this = Message{} }
func (this *Message) String() string { return proto.CompactTextString(this) }
func (*Message) ProtoMessage()       {}

func (this *Message) GetType() uint32 {
	if this != nil && this.Type != nil {
		return *this.Type
	}
	return 0
}

func (this *Message) GetVersion() uint64 {
	if this != nil && this.Version != nil {
		return *this.Version
	}
	return 0
}

func (this *Message) GetGossip() *Gossip {
	if this != nil {
		return this.Gossip
	}
	return nil
}

func init() {
	proto.RegisterEnum("server.TYPE", TYPE_name, TYPE_value)
}
