package types

import (
	pb "distributed_kv_store/internal/cluster"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type RaftState string

const (
	Follower  RaftState = "follower"
	Candidate RaftState = "candidate"
	Leader    RaftState = "leader"
)

type RaftSavedState struct {
	CurrentTerm uint64
	VotedFor    string
}

type Node struct {
	ID       string `yaml:"id"`
	Address  string `yaml:"address"`
	Port     int    `yaml:"port"`
	GrpcPort int    `yaml:"grpc_port"`
	DataDir  string `yaml:"data_dir"`

	// KV Store Data
	Data map[string][]byte
	Mu   sync.Mutex

	// Raft
	RaftMu sync.Mutex

	CurrentTerm      uint64            // Latest term server
	VotedFor         string            // Candidate ID that the node voted for
	Log              []*pb.LogEntry    // Replicated messages log
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

func (n *Node) SaveRaftState() error {
	filePath := filepath.Join(n.DataDir, "raft_state.gob")
	fmt.Printf("%s", filePath)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("Error opening file: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	savedState := &RaftSavedState{
		CurrentTerm: n.CurrentTerm,
		VotedFor:    n.VotedFor,
	}
	if err := encoder.Encode(savedState); err != nil {
		return fmt.Errorf("error saving raft state: %v", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("error saving raft state: %v", err)
	}
	return nil
}

func (n *Node) LoadRaftState() error {
	filePath := filepath.Join(n.DataDir, "raft_state.gob")
	fmt.Printf("%s", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Raft state file not found for node %s", n.ID)
			return nil
		}
		log.Printf("Error opening file: %v", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	var savedState RaftSavedState
	if err := decoder.Decode(&savedState); err != nil {
		return fmt.Errorf("error decoding raft state: %v", err)
	}
	n.CurrentTerm = savedState.CurrentTerm
	n.VotedFor = savedState.VotedFor
	return nil
}

func (n *Node) AppendLogEntry(entry *pb.LogEntry) error {
	filePath := filepath.Join(n.DataDir, "raft_log.gob")
	fmt.Printf("%s", filePath)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(entry); err != nil {
		return fmt.Errorf("error encoding raft log: %v", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("error saving raft log: %v", err)
	}
	return nil
}

func (n *Node) AppendLogEntries(entries []*pb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	filePath := filepath.Join(n.DataDir, "raft_log.gob")
	fmt.Printf("%s", filePath)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)

	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("error encoding raft log: %v", err)
		}
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("error saving raft log: %v", err)
	}
	return nil
}

func (n *Node) LoadRaftLog() error {
	filePath := filepath.Join(n.DataDir, "raft_log.gob")
	fmt.Printf("%s", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("raft log file not found for node %s", n.ID)
		}
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	n.Log = make([]*pb.LogEntry, 0)
	for {
		var entry pb.LogEntry
		err := decoder.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error decoding log entry: %v", err)
		}
		n.Log = append(n.Log, &entry)
	}
	return nil
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
