package core

type Key string
type Revision uint64
type EphemId uint64

type NodeInfo map[string]*Node

type Token map[string]Perms
type AccessInfo map[string]*Token

type Info struct {
    Nodes NodeInfo
    Access AccessInfo
}

func NewInfo() *Info {
    info := new(Info)
    info.Nodes = make(NodeInfo)
    info.Access = make(AccessInfo)
    return info
}
