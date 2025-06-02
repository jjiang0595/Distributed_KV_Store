package cluster

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type RaftState string
type CommandType string

const (
	Follower  RaftState = "follower"
	Candidate RaftState = "candidate"
	Leader    RaftState = "leader"
)

const (
	CommandPut CommandType = "put"
	CommandGet CommandType = "get"
	CommandDel CommandType = "del"
)

const (
	minElectionTimeoutMs = 150
	maxElectionTimeoutMs = 300
)

type RaftSavedState struct {
	CurrentTerm uint64
	VotedFor    string
}

type Command struct {
	Type  CommandType
	Key   string
	Value []byte
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

	Peers           map[string]*Node
	CurrentTerm     uint64      // Latest term server
	VotedFor        string      // Candidate ID that the node voted for
	Log             []*LogEntry // Replicated messages log
	CommitIndex     uint64      // Index of highest log entry to be committed
	State           RaftState   // Leader, Candidate, Follower
	LastApplied     uint64
	LeaderID        string            // Default "" if not leader, node ID otherwise
	VotesReceived   map[string]bool   // Set of node IDs that stores whether the node voted for the current candidate
	NextIndex       map[string]uint64 // Follower's next log entry's index
	MatchIndex      map[string]uint64 // Follower's index of the highest log entry to be replicated
	ElectionTimeout *time.Timer       // Timer for election timeouts

	AppendEntriesChan         chan *AppendEntriesRequestWrapper
	AppendEntriesResponseChan chan *AppendEntriesResponseWrapper
	ClientCommandChan         chan *Command
	RequestVoteChan           chan *RequestVoteRequestWrapper
	RequestVoteResponseChan   chan *RequestVoteResponse

	ReplicatorCtx    context.Context
	ReplicatorCancel context.CancelFunc
}

type NodeMap struct {
	Nodes []*Node
}

type AppendEntriesRequestWrapper struct {
	Ctx      context.Context
	Request  *AppendEntriesRequest
	Response chan *AppendEntriesResponse
}

type AppendEntriesResponseWrapper struct {
	Response     *AppendEntriesResponse
	Error        error
	PeerID       string
	PrevLogIndex uint64
	SentEntries  []*LogEntry
}

type RequestVoteRequestWrapper struct {
	Ctx      context.Context
	Request  *RequestVoteRequest
	Response chan *RequestVoteResponse
}

type RaftServer struct {
	UnimplementedRaftServiceServer
	mainNode *Node
}

func NewRaftServer(mainNode *Node) *RaftServer {
	return &RaftServer{
		mainNode: mainNode,
	}
}

// State Persistence

func (n *Node) ApplyCommittedEntries() {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	for n.LastApplied < n.CommitIndex {
		n.LastApplied++
		entry := n.Log[n.LastApplied-1]

		var cmd Command
		err := json.Unmarshal(entry.Command, &cmd)
		if err != nil {
			log.Fatalf("Error unmarshalling command: %v", err)
		}

		n.Data[cmd.Key] = cmd.Value
		log.Printf("Node %s: PUT %s -> %s", n.ID, cmd.Key, string(cmd.Value))
	}
}

func (n *Node) SaveRaftState() error {
	filePath := filepath.Join(n.DataDir, "raft_state.gob")
	fmt.Printf("%s", filePath)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("Error opening file: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()

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
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()

	decoder := gob.NewDecoder(file)
	var savedState RaftSavedState
	if err := decoder.Decode(&savedState); err != nil {
		return fmt.Errorf("error decoding raft state: %v", err)
	}
	n.CurrentTerm = savedState.CurrentTerm
	n.VotedFor = savedState.VotedFor
	return nil
}

func (n *Node) SaveLogEntry(entry *LogEntry) error {
	filePath := filepath.Join(n.DataDir, "raft_log.gob")
	fmt.Printf("%s", filePath)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(entry); err != nil {
		return fmt.Errorf("error encoding raft log: %v", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("error saving raft log: %v", err)
	}
	return nil
}

func (n *Node) SaveLogEntries(entries []*LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	filePath := filepath.Join(n.DataDir, "raft_log.gob")
	fmt.Printf("%s", filePath)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()
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
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()

	decoder := gob.NewDecoder(file)
	n.Log = make([]*LogEntry, 0)
	for {
		var entry LogEntry
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

// Raft

func (n *Node) StartReplicators() {
	n.RaftMu.Lock()
	if n.ReplicatorCtx != nil {
		n.ReplicatorCancel()
	}
	n.ReplicatorCtx, n.ReplicatorCancel = context.WithCancel(context.Background())
	n.RaftMu.Unlock()

	for _, peer := range n.Peers {
		log.Printf("Leader %s: Starting replicator for peer %s", n.ID, peer.ID)
		go n.ReplicateToFollower(n.ReplicatorCtx, peer.ID)
	}
}

func (n *Node) StopReplicators() {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	if n.ReplicatorCtx != nil {
		n.ReplicatorCancel()
		n.ReplicatorCancel = nil
		n.ReplicatorCtx = nil
	} else {
		log.Printf("Leader %s: No replicators to end", n.ID)
	}
}

func (n *Node) ReplicateToFollower(stopCtx context.Context, followerID string) {
	peer, ok := n.Peers[followerID]
	if !ok || peer == nil {
		return
	}
	address, grpcPort := peer.Address, peer.GrpcPort

	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", address, grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Error creating grpc client: %v", err)
		if conn != nil {
			conn.Close()
		}
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Fatalf("Error closing grpc client: %v", err)
		}
	}()
	peerClient := NewRaftServiceClient(conn)

	for {
		select {
		case <-stopCtx.Done():
			log.Printf("Node %s: ReplicateToFollower stopped", n.ID)
			return
		default:
			n.RaftMu.Lock()
			if n.State != Leader {
				n.RaftMu.Unlock()
				return
			}
			term, leaderID, commitIndex := n.CurrentTerm, n.LeaderID, n.CommitIndex
			logSnapshot := make([]*LogEntry, len(n.Log))
			copy(logSnapshot, n.Log)

			peerNextIndex, ok := n.NextIndex[followerID]
			if !ok {
				peerNextIndex = 1
			}
			n.RaftMu.Unlock()

			prevLogIndex, prevLogTerm := uint64(0), uint64(0)
			if peerNextIndex > 1 {
				if peerNextIndex-2 < uint64(len(logSnapshot)) {
					prevLogIndex = peerNextIndex - 1
					prevLogTerm = logSnapshot[peerNextIndex-2].Term
				} else {
					log.Print("Indexes out of bound. Resetting to prevLogIndex & prevLogTerm to 0")
					prevLogIndex = 0
					prevLogTerm = 0
				}
			}

			var entries []*LogEntry

			if peerNextIndex > 0 && peerNextIndex <= uint64(len(logSnapshot)) {
				entries = logSnapshot[peerNextIndex-1:]
			} else if len(logSnapshot) > 0 && peerNextIndex == 0 {
				entries = logSnapshot
			} else if len(logSnapshot) > 0 && peerNextIndex > uint64(len(logSnapshot)) {
				entries = []*LogEntry{}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			response, err := peerClient.AppendEntries(ctx, &AppendEntriesRequest{
				Term:         term,
				LeaderId:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			})
			if err != nil {
				log.Printf("Error appending raft log: %v", err)
			}
			n.RaftMu.Lock()
			if response != nil && term < response.Term {
				log.Printf("Leader's %s term is less than follower, reverting to follower.", n.ID)
				n.State = Follower
				n.LeaderID = ""
				n.CurrentTerm = response.Term
				err := n.SaveRaftState()
				if err != nil {
					log.Fatalf("Error saving raft state: %v", err)
				}
				n.StopReplicators()
				n.ResetElectionTimeout()
				n.RaftMu.Unlock()
				return
			}

			sleepDuration := 10 * time.Millisecond
			if response.Success {
				log.Printf("Node %s: Successfully replicated", n.ID)
				sleepDuration = 50 * time.Millisecond
				if n.NextIndex[followerID] > 0 {
					n.MatchIndex[followerID] = prevLogIndex + uint64(len(entries))
					n.NextIndex[followerID] = prevLogIndex + uint64(len(entries)) + 1
				}
			} else {
				log.Printf("Node %s: Failed to replicate to follower", n.ID)
				if n.NextIndex[followerID] > 1 {
					n.NextIndex[followerID] -= 1
				}
			}
			n.RaftMu.Unlock()

			log.Printf("ReplicateToFollower: Node %s is sleeping for %v", n.ID, sleepDuration)
			time.Sleep(sleepDuration)
		}
	}
}

func (n *Node) ResetElectionTimeout() {
	durationMs := rand.Intn(maxElectionTimeoutMs-minElectionTimeoutMs+1) + minElectionTimeoutMs
	timeout := time.Duration(durationMs) * time.Millisecond

	if n.ElectionTimeout != nil {
		n.ElectionTimeout.Stop()
	}
	n.ElectionTimeout = time.NewTimer(timeout)
	log.Printf("Node %s: Election timeout set to %dms", n.ID, durationMs)
}

func (s *RaftServer) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if req.Term > s.mainNode.CurrentTerm {
		s.mainNode.VotedFor = ""
		s.mainNode.CurrentTerm = req.Term
		err := s.mainNode.SaveRaftState()
		if err != nil {
			return nil, err
		}
		s.mainNode.State = Follower
	}
	var lastLogTerm uint64 = 0
	if len(s.mainNode.Log) > 0 {
		lastLogTerm = s.mainNode.Log[len(s.mainNode.Log)-1].Term
	}
	var lastLogIndex uint64 = 0
	if len(s.mainNode.Log) > 0 {
		lastLogIndex = s.mainNode.Log[len(s.mainNode.Log)-1].Index
	}

	logOk := (req.LastLogTerm > lastLogTerm) ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if req.Term == s.mainNode.CurrentTerm && logOk && (s.mainNode.VotedFor == req.CandidateId || s.mainNode.VotedFor == "") {
		s.mainNode.VotedFor = req.CandidateId
		err := s.mainNode.SaveRaftState()
		if err != nil {
			return nil, err
		}
		return &RequestVoteResponse{Term: s.mainNode.CurrentTerm, VoteGranted: true, VoterId: req.CandidateId}, nil
	}
	return &RequestVoteResponse{Term: s.mainNode.CurrentTerm, VoteGranted: false, VoterId: req.CandidateId}, nil
}

func (s *RaftServer) ReceiveVote(req *RequestVoteResponse) {
	term, voteGranted, voterId := req.Term, req.VoteGranted, req.VoterId
	if term > s.mainNode.CurrentTerm {
		s.mainNode.VotedFor = ""
		s.mainNode.CurrentTerm = req.Term
		s.mainNode.State = Follower
		s.mainNode.LeaderID = ""
		if err := s.mainNode.SaveRaftState(); err != nil {
			log.Fatalf("error saving raft state: %v", err)
		}
		return
	}

	if term == s.mainNode.CurrentTerm && voteGranted {
		s.mainNode.VotesReceived[voterId] = true
		log.Printf("Received vote from follower %s to leader %s", voterId, s.mainNode.LeaderID)
	}
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if req.Term > s.mainNode.CurrentTerm {
		s.mainNode.VotedFor = ""
		s.mainNode.CurrentTerm = req.Term
		err := s.mainNode.SaveRaftState()
		if err != nil {
			return nil, err
		}
		s.mainNode.State = Follower
	}
	s.mainNode.LeaderID = req.GetLeaderId()

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < s.mainNode.CurrentTerm {
		return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if req.PrevLogIndex > uint64(len(s.mainNode.Log)) {
		log.Printf("Missing entries in the %s's log, append canceled from %s", req.GetLeaderId(), s.mainNode.ID)
		return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
	}
	if req.PrevLogIndex > 0 && req.GetPrevLogTerm() != s.mainNode.Log[req.PrevLogIndex-1].Term {
		log.Printf("PrevLogTerm of receiver %s doesn't match PrevLogTerm of sender %s", req.GetLeaderId(), s.mainNode.ID)
		return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
	}

	originalLength := len(s.mainNode.Log)
	// Find and truncate if a potential conflicting entry is found, otherwise append all entries
	for i, entry := range req.Entries {
		leaderIndex := req.PrevLogIndex + uint64(i) + 1

		if leaderIndex > uint64(len(s.mainNode.Log)) {
			s.mainNode.Log = append(s.mainNode.Log, entry)
		} else {
			if s.mainNode.Log[leaderIndex-1].Term != entry.Term {
				s.mainNode.Log = s.mainNode.Log[:leaderIndex-1]
			}
			s.mainNode.Log = append(s.mainNode.Log, entry)
		}
	}

	newEntries := s.mainNode.Log[originalLength:]
	if len(newEntries) > 0 {
		err := s.mainNode.SaveLogEntries(newEntries)
		if err != nil {
			return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
		}
	}

	if req.LeaderCommit > s.mainNode.CommitIndex {
		lastEntryIndex := uint64(0)
		if len(s.mainNode.Log) > 0 {
			lastEntryIndex = s.mainNode.Log[len(s.mainNode.Log)-1].Index
		}
		s.mainNode.CommitIndex = min(req.LeaderCommit, lastEntryIndex)
	}
	err := s.mainNode.LoadRaftLog()
	if err != nil {
		return nil, err
	}
	fmt.Printf("%+v\n", s.mainNode.Log)
	return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: true}, nil
}

func (s *RaftServer) AppendEntriesHandler(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	respChan := make(chan *AppendEntriesResponse, 1)
	wrapper := &AppendEntriesRequestWrapper{
		Ctx:      ctx,
		Request:  req,
		Response: respChan,
	}
	s.mainNode.AppendEntriesChan <- wrapper

	resp := <-respChan
	return resp, nil
}

func (s *RaftServer) RequestVoteHandler(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	respChan := make(chan *RequestVoteResponse, 1)
	wrapper := &RequestVoteRequestWrapper{
		Ctx:      ctx,
		Request:  req,
		Response: respChan,
	}
	s.mainNode.RequestVoteChan <- wrapper

	resp := <-respChan
	return resp, nil
}
