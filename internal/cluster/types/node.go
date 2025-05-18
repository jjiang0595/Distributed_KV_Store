package types

import (
	"fmt"
	"sync"
)

type Node struct {
	ID       string `yaml:"id"`
	Address  string `yaml:"address"`
	Port     int    `yaml:"port"`
	GrpcPort int    `yaml:"grpc_port"`
	Data     map[string][]byte
	Mu       sync.Mutex
}

type NodeMap struct {
	Nodes []Node
}

func (n *Node) Put(key string, value []byte) {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	n.Data[key] = value
}

func (n *Node) Get(key string) ([]byte, error) {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	val, ok := n.Data[key]
	if !ok {
		return nil, fmt.Errorf("404 - Key Not Found: %s", key)
	}
	return val, nil
}
