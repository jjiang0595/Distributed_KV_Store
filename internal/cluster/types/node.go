package types

import (
	"fmt"
	"sync"
)

type Node struct {
	ID      string
	Address string
	Port    int
	Data    map[string][]byte
	mu      sync.Mutex
}

type NodeMap struct {
	Nodes []Node
}

func (n *Node) Put(key string, value []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Data[key] = value
}

func (n *Node) Get(key string) ([]byte, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	val, ok := n.Data[key]
	if !ok {
		return nil, fmt.Errorf("404 - Key Not Found: %s", key)
	}
	return val, nil
}
