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
	TYPE_PING TYPE = 0
	TYPE_PONG TYPE = 1
)

var TYPE_name = map[int32]string{
	0: "PING",
	1: "PONG",
}
var TYPE_value = map[string]int32{
	"PING": 0,
	"PONG": 1,
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

type ServerInfo struct {
	Id               *string `protobuf:"bytes,1,req,name=id" json:"id,omitempty"`
	Ipv4             *uint32 `protobuf:"varint,2,req,name=ipv4" json:"ipv4,omitempty"`
	Port             *uint32 `protobuf:"varint,3,req,name=port" json:"port,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (this *ServerInfo) Reset()         { *this = ServerInfo{} }
func (this *ServerInfo) String() string { return proto.CompactTextString(this) }
func (*ServerInfo) ProtoMessage()       {}

func (this *ServerInfo) GetId() string {
	if this != nil && this.Id != nil {
		return *this.Id
	}
	return ""
}

func (this *ServerInfo) GetIpv4() uint32 {
	if this != nil && this.Ipv4 != nil {
		return *this.Ipv4
	}
	return 0
}

func (this *ServerInfo) GetPort() uint32 {
	if this != nil && this.Port != nil {
		return *this.Port
	}
	return 0
}

type Gossip struct {
	Type             *uint32       `protobuf:"varint,1,req,name=type" json:"type,omitempty"`
	Version          *uint64       `protobuf:"varint,2,req,name=version" json:"version,omitempty"`
	Dead             []*ServerInfo `protobuf:"bytes,3,rep,name=dead" json:"dead,omitempty"`
	XXX_unrecognized []byte        `json:"-"`
}

func (this *Gossip) Reset()         { *this = Gossip{} }
func (this *Gossip) String() string { return proto.CompactTextString(this) }
func (*Gossip) ProtoMessage()       {}

func (this *Gossip) GetType() uint32 {
	if this != nil && this.Type != nil {
		return *this.Type
	}
	return 0
}

func (this *Gossip) GetVersion() uint64 {
	if this != nil && this.Version != nil {
		return *this.Version
	}
	return 0
}

func (this *Gossip) GetDead() []*ServerInfo {
	if this != nil {
		return this.Dead
	}
	return nil
}

func init() {
	proto.RegisterEnum("server.TYPE", TYPE_name, TYPE_value)
}
