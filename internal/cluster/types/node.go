package types

import (
	"fmt"
	"sync"
	"time"
)

type RaftState string

const (
	Follower  RaftState = "follower"
	Candidate RaftState = "candidate"
	Leader    RaftState = "leader"
)

type Node struct {
	ID       string `yaml:"id"`
	Address  string `yaml:"address"`
	Port     int    `yaml:"port"`
	GrpcPort int    `yaml:"grpc_port"`

	// KV Store Data
	Data map[string][]byte
	Mu   sync.Mutex

	// Raft
	RaftMu sync.Mutex

	CurrentTerm      uint64            // Latest term server
	VotedFor         string            // Candidate ID that the node voted for
	Log              []LogEntry        // Replicated messages log
	CommitIndex      uint64            // Index of highest log entry to be committed
	State            RaftState         // Leader, Candidate, Follower
	LeaderID         string            // Default "" if not leader, node ID otherwise
	VotesReceived    map[string]bool   // Set of node IDs that stores whether the node voted for the current candidate
	NextIndex        map[string]uint64 // Follower's next log entry's index
	MatchIndex       map[string]uint64 // Follower's index of the highest log entry to be replicated
	ElectionTimeout  *time.Timer       // Timer for election timeouts
	HeartbeatTimeout *time.Timer       // Timer for leader heartbeats
}

type NodeMap struct {
	Nodes []Node
}

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
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
