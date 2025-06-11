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
	"net"
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

const CommandPut CommandType = "put"

const (
	minElectionTimeoutMs = 150
	maxElectionTimeoutMs = 300
)

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []*LogEntry
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
	CurrentTerm     uint64            // Latest term server
	VotedFor        string            // Candidate ID that the node voted for
	Log             []*LogEntry       // Replicated messages log
	CommitIndex     uint64            // Index of highest entry that was known to be committed
	State           RaftState         // Leader, Candidate, Follower
	LastApplied     uint64            // Highest index that was applied to state machine (data)
	LeaderID        string            // Current leader's ID, default ""
	VotesReceived   map[string]bool   // Set of node IDs that the candidate has received votes from
	NextIndex       map[string]uint64 // (Leader) The next index that the leader will send to a follower
	MatchIndex      map[string]uint64 // (Leader) The index that the leader has already replicated its logs up to
	ElectionTimeout *time.Timer       // Election timer that triggers if no gRPC response is heard from leader

	AppendEntriesChan         chan *AppendEntriesRequestWrapper
	AppendEntriesResponseChan chan *AppendEntriesResponseWrapper
	ClientCommandChan         chan *Command
	PersistStateChan          chan *PersistentState
	RequestVoteChan           chan *RequestVoteRequestWrapper
	RequestVoteResponseChan   chan *RequestVoteResponse

	ApplierCond *sync.Cond

	Ctx              context.Context
	Cancel           context.CancelFunc
	ReplicatorCancel map[string]context.CancelFunc

	// WaitGroups
	ApplierWg    sync.WaitGroup
	RaftLoopWg   sync.WaitGroup
	PersistWg    sync.WaitGroup
	ReplicatorWg sync.WaitGroup
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

func (n *Node) PersistRaftState() {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()

	savedState := &PersistentState{
		CurrentTerm: n.CurrentTerm,
		VotedFor:    n.VotedFor,
		Log:         n.Log,
	}

	n.PersistStateChan <- savedState
}

func (n *Node) LoadRaftState() error {
	filePath := filepath.Join(n.DataDir, "raft_state.gob")
	info, err := os.Stat(filePath)
	if info.Size() == 0 {
		log.Printf("Node %s: Raft State File Empty", n.ID)
		return nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Raft state file not found for n %s", n.ID)
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
	var savedState PersistentState
	if err := decoder.Decode(&savedState); err != nil {
		return fmt.Errorf("error decoding raft state: %v", err)
	}
	log.Printf("Saved State: %v", savedState)
	n.CurrentTerm = savedState.CurrentTerm
	n.VotedFor = savedState.VotedFor
	n.Log = savedState.Log
	return nil
}

// Raft

func (n *Node) RunRaftLoop() {
	defer n.RaftLoopWg.Done()
	grpcServer := grpc.NewServer()
	raftServer := NewRaftServer(n)
	RegisterRaftServiceServer(grpcServer, raftServer)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", n.GrpcPort))
	if err != nil {
		log.Fatalf("Error listening on gRPC port %d: %v", n.GrpcPort, err)
	}

	go func() {
		log.Printf("Listening on gRPC port %d", n.GrpcPort)
		serveErr := grpcServer.Serve(listener)
		if serveErr != nil {
			log.Fatalf("Error serving on gRPC port %d: %v", n.GrpcPort, serveErr)
		}
	}()

	n.ResetElectionTimeout()
	for {
		n.RaftMu.Lock()
		currentState := n.State
		n.RaftMu.Unlock()
		log.Printf("Current data: %v", n.Data)
		switch currentState {
		case Leader:
			log.Printf("------------------------Leader---------------------------------")
			select {
			case <-n.Ctx.Done():
				log.Printf("Leader %s: Shutting down", n.ID)
				return
			case wrappedResp := <-n.AppendEntriesResponseChan:
				n.RaftMu.Lock()
				grpcResponse := wrappedResp.Response
				if grpcResponse != nil && n.CurrentTerm < grpcResponse.Term {
					log.Printf("Leader's %s term is less than follower, reverting to follower.", n.ID)
					n.State = Follower
					n.LeaderID = ""
					n.CurrentTerm = grpcResponse.Term
					go n.PersistRaftState()
					n.StopReplicators()
					n.ResetElectionTimeout()
					n.RaftMu.Unlock()
					return
				}

				if grpcResponse.Success {
					log.Printf("Leader %s: Successfully replicated to nodes", n.ID)
					if n.NextIndex[wrappedResp.PeerID] > 0 {
						n.MatchIndex[wrappedResp.PeerID] = wrappedResp.PrevLogIndex + uint64(len(wrappedResp.SentEntries))
						n.NextIndex[wrappedResp.PeerID] = wrappedResp.PrevLogIndex + uint64(len(wrappedResp.SentEntries)) + 1
					}
				} else {
					log.Printf("Node %s: Failed to replicate to follower", n.ID)
					if n.NextIndex[wrappedResp.PeerID] > 1 {
						n.NextIndex[wrappedResp.PeerID] -= 1
					}
				}
				n.RaftMu.Unlock()
				// Update Leader's Commit Index
				for i := len(n.Log) - 1; i >= 0; i-- {
					if n.CurrentTerm != n.Log[i].Term {
						break
					}
					majorityCount := 1
					for peerId, _ := range n.Peers {
						if n.MatchIndex[peerId] >= uint64(i) {
							majorityCount++
						}
					}
					if majorityCount >= len(n.Peers)/2+1 {
						n.CommitIndex = uint64(i) + 1
						log.Printf("Committed Index is %v", n.CommitIndex)
						n.ApplierCond.Broadcast()
						break
					}
				}
			case clientReq := <-n.ClientCommandChan:
				log.Printf("Client Request Received: %v", clientReq)
				commandBytes, err := json.Marshal(clientReq)
				if err != nil {
					log.Printf("Error marshalling command to bytes: %v", err)
					continue
				}
				n.RaftMu.Lock()
				entry := &LogEntry{
					Term:    n.CurrentTerm,
					Index:   uint64(len(n.Log)) + 1,
					Command: commandBytes,
				}
				n.Log = append(n.Log, entry)

				go n.PersistRaftState()
				n.RaftMu.Unlock()
				log.Printf("Node %s: Successfully appended log entry", n.ID)
			}
		case Candidate:
			log.Printf("------------------------Candidate---------------------------------")
			select {
			case <-n.Ctx.Done():
				log.Printf("Candidate %s: Shutting down", n.ID)
				return
			case <-n.ElectionTimeout.C:
				n.RaftMu.Lock()
				log.Printf("Candidate - Election timeout")

				n.CurrentTerm += 1
				n.VotedFor = n.ID
				n.VotesReceived = make(map[string]bool)
				n.VotesReceived[n.ID] = true
				if err := n.SaveRaftState(); err != nil {
					log.Fatalf("error saving raft state: %v", err)
				}
				go n.PersistRaftState()
				n.RaftMu.Unlock()

				n.ResetElectionTimeout()
				for _, peer := range n.Peers {
					if peer.ID == n.ID {
						continue
					}
					go func(peer *Node) {
						log.Printf("Candidate Election - Node %s sending to Node %s", n.ID, peer.ID)
						conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", peer.Address, peer.GrpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
						if err != nil {
							log.Printf("Error connecting to peer %s:%d %v", peer.Address, peer.Port, err)
							if conn != nil {
								conn.Close()
							}
							return
						}
						defer func(conn *grpc.ClientConn) {
							err := conn.Close()
							if err != nil {
								log.Printf("Error closing peer %s:%d %v", peer.Address, peer.Port, err)
							}
						}(conn)
						peerClient := NewRaftServiceClient(conn)

						n.RaftMu.Lock()
						currentTerm := n.CurrentTerm
						lastLogIndex := func() uint64 {
							if len(n.Log) == 0 {
								return 0
							}
							return n.Log[len(n.Log)-1].Index
						}()
						lastLogTerm := func() uint64 {
							if len(n.Log) == 0 {
								return 0
							}
							return n.Log[len(n.Log)-1].Term
						}()
						n.RaftMu.Unlock()

						voteRequest := &RequestVoteRequest{
							Term:         currentTerm,
							CandidateId:  n.ID,
							LastLogIndex: lastLogIndex,
							LastLogTerm:  lastLogTerm,
						}
						response, err := peerClient.RequestVote(ctx, voteRequest)
						if err != nil {
							log.Printf("Error requesting vote: %v", err)
							return
						}
						log.Printf("Vote Request Response: %v", response)
						n.RequestVoteResponseChan <- response
					}(peer)
				}

			case aeWrapper := <-n.AppendEntriesChan:
				n.RaftMu.Lock()
				ctx, cancel := context.WithTimeout(aeWrapper.Ctx, time.Millisecond*50)
				response, err := raftServer.ProcessAppendEntriesRequest(aeWrapper.Ctx, aeWrapper.Request)
				if err != nil {
					log.Printf("Error appending entries: %v", err)
					if response == nil {
						response = &AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
					}
				}
				if n.CommitIndex > n.LastApplied {
					n.ApplyCommittedEntries()
					n.ApplierWg.Add(1)
					n.ApplierCond.Broadcast()
				}
				cancel()
				n.RaftMu.Unlock()
				aeWrapper.Response <- response

			case reqVoteWrapper := <-n.RequestVoteChan:
				ctx, cancel := context.WithTimeout(reqVoteWrapper.Ctx, 50*time.Millisecond)

				response, err := raftServer.ProcessVoteRequest(ctx, reqVoteWrapper.Request)
				log.Printf("Node %s received a vote request from %s", n.ID, reqVoteReq.Request.CandidateId)
				if err != nil {
					log.Printf("Error requesting vote: %v", err)
				}
				cancel()
				reqVoteWrapper.Response <- response

			case reqVoteRespWrapper := <-n.RequestVoteResponseChan:
				raftServer.ReceiveVote(reqVoteRespWrapper)

				n.RaftMu.Lock()
				votesReceivedLen, peersLen := len(n.VotesReceived), len(n.Peers)

				if uint64(votesReceivedLen) >= uint64(peersLen/2)+1 {
					n.State = Leader
					n.LeaderID = n.ID
					n.NextIndex = make(map[string]uint64)
					n.MatchIndex = make(map[string]uint64)
					for _, peer := range n.Peers {
						if peer.ID == n.ID {
							continue
						}
						n.NextIndex[peer.ID] = func() uint64 {
							if len(n.Log) == 0 {
								return 1
							}
							return n.Log[len(n.Log)-1].Index + 1
						}()
						n.MatchIndex[peer.ID] = 0
					}
					log.Printf("New Leader - %s. VotesReceived %v", n.ID, n.VotesReceived)
					n.StartReplicators()
				}
				n.RaftMu.Unlock()
			}

		case Follower:
			log.Printf("------------------------Follower---------------------------------")
			select {
			case <-n.Ctx.Done():
				log.Printf("Follower %s: Shutting down", n.ID)
				return
			case <-n.ElectionTimeout.C:
				log.Printf("Follower - Election timeout")
				n.RaftMu.Lock()
				n.State = Candidate
				n.CurrentTerm += 1
				n.VotedFor = n.ID
				n.VotesReceived = make(map[string]bool)
				n.VotesReceived[n.ID] = true
				n.RaftMu.Unlock()

				n.ResetElectionTimeout()
				if err := n.SaveRaftState(); err != nil {
					log.Fatalf("error saving raft state: %v", err)
				}
				go n.PersistRaftState()

			case aeReq := <-n.AppendEntriesChan:
				ctx, cancel := context.WithTimeout(aeReq.Ctx, time.Millisecond*50)

				log.Printf("Request received in AppendEntriesChan")
				response, err := raftServer.ProcessAppendEntriesRequest(aeReq.Ctx, aeReq.Request)
				if err != nil {
					log.Printf("Error appending entries: %v", err)
					if response == nil {
						response = &AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
					}
				}
				if n.CommitIndex > n.LastApplied {
					n.ApplyCommittedEntries()
					n.ApplierWg.Add(1)
					n.ApplierCond.Broadcast()
				}
				cancel()
				aeReq.Response <- response

			case reqVoteReq := <-n.RequestVoteChan:
				log.Printf("Node %s received a vote request from %s", n.ID, reqVoteReq.Request.CandidateId)
				response, err := raftServer.ProcessVoteRequest(reqVoteReq.Ctx, reqVoteReq.Request)
				if err != nil {
					log.Printf("Error requesting vote: %v", err)
				}
				reqVoteReq.Response <- response
			}
		}
	}
}

func (n *Node) StartReplicators() {
	log.Printf("Starting replicators...")
	n.ReplicatorCancel = make(map[string]context.CancelFunc)

	for peerID := range n.Peers {
		if n.ID == peerID {
			continue
		}
		replicatorCtx, replicatorCancel := context.WithCancel(n.Ctx)
		n.ReplicatorCancel[peerID] = replicatorCancel
		n.ReplicatorWg.Add(1)
		log.Printf("Leader %s: Starting replicator for peer %s", n.ID, peerID)
		go n.ReplicateToFollower(replicatorCtx, peerID)
	}
}

func (n *Node) StopReplicators() {
	for _, replicatorCancel := range n.ReplicatorCancel {
		replicatorCancel()
	}
	n.ReplicatorCancel = nil
}

func (n *Node) Shutdown() {
	n.RaftMu.Lock()
	n.Cancel()
	n.StopReplicators()
	n.ApplierWg.Done()
	n.RaftMu.Unlock()

	done := make(chan struct{})
	go func() {
		n.ApplierWg.Wait()
		n.PersistWg.Wait()
		n.RaftLoopWg.Wait()
		n.ReplicatorWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("Node %s: All follower goroutines stopped fully", n.ID)
	case <-time.After(3 * time.Second):
		log.Printf("Node %s: Timed out after 3 sec. Some goroutines may still be running", n.ID)
	}
}

func (n *Node) ReplicateToFollower(stopCtx context.Context, followerID string) {
	defer n.ReplicatorWg.Done()
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
		case <-n.Ctx.Done():
			log.Printf("Leader %s: ReplicateToFollower stopped", n.ID)
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

			entries := make([]*LogEntry, 0)

			if peerNextIndex > 0 && peerNextIndex <= uint64(len(logSnapshot)) {
				entries = logSnapshot[peerNextIndex-1:]
			} else if len(logSnapshot) > 0 && peerNextIndex == 0 {
				entries = logSnapshot
			} else if len(logSnapshot) > 0 && peerNextIndex > uint64(len(logSnapshot)) {
				entries = []*LogEntry{}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			log.Printf("Leader %s: ReplicateToFollower sending logs to %v", leaderID, followerID)
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
				cancel()
				return
			}
			wrappedResp := &AppendEntriesResponseWrapper{
				Response:     response,
				Error:        err,
				PeerID:       peer.ID,
				PrevLogIndex: prevLogIndex,
				SentEntries:  entries,
			}
			n.AppendEntriesResponseChan <- wrappedResp

			sleepDuration := 10 * time.Millisecond
			if response.Success {
				log.Printf("Leader %s: Successfully replicated to nodes", n.ID)
				sleepDuration = 50 * time.Millisecond
			}
			cancel()
			log.Printf("MatchIndex: %v, NextIndex: %v", n.MatchIndex[followerID], n.NextIndex[followerID])
			log.Printf("Log: %v", n.Log)
			log.Printf("Leader %s: Sleep %v", n.ID, sleepDuration)
			time.Sleep(sleepDuration)
		}
	}
}

func (n *Node) ApplierGoroutine() {
	n.RaftMu.Lock()
	for n.CommitIndex <= n.LastApplied {
		n.ApplierCond.Wait()
	}

	for n.LastApplied < n.CommitIndex {
		n.LastApplied++

		var cmd Command
		err := json.Unmarshal(entry.Command, &cmd)
		if err != nil {
			log.Fatalf("Error unmarshalling command: %v", err)
		}

		n.Data[cmd.Key] = cmd.Value
		n.ApplierWg.Done()
		log.Printf("Node %s: PUT %s -> %s", n.ID, cmd.Key, string(cmd.Value))
	}
	n.RaftMu.Unlock()
}

func (n *Node) PersistStateGoroutine() {
	for {
		select {
			log.Printf("Leader %s: PersistStateGoroutine stopped through Ctx.Done()", n.ID)
		case raftState, ok := <-n.PersistStateChan:
			if !ok {
				n.PersistWg.Done()
				log.Printf("Leader %s: PersistStateGoroutine stopped through !ok", n.ID)
				return
			}
			var buffer bytes.Buffer
			encoder := gob.NewEncoder(&buffer)
			if err := encoder.Encode(raftState); err != nil {
				log.Fatalf("Error encoding saved state: %v", err)
			}

			filePath := filepath.Join(n.DataDir, "raft_state.gob")
			tmpFilePath := filePath + ".tmp"

			file, err := os.OpenFile(tmpFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Printf("Error opening file: %v", err)
			}

			if _, err := file.Write(buffer.Bytes()); err != nil {
				log.Fatalf("error saving raft state: %v", err)
			}
			if err := file.Sync(); err != nil {
				log.Fatalf("error saving raft state: %v", err)
			}
			file.Close()
			if err := os.Rename(tmpFilePath, filePath); err != nil {
				log.Fatalf("error renaming saved state: %v", err)
			}
			log.Printf("Node %s: Saved to Raft State %s", n.ID, filePath)
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

func (s *RaftServer) ProcessVoteRequest(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	log.Printf("Processing vote request...")
	s.mainNode.RaftMu.Lock()
	defer s.mainNode.RaftMu.Unlock()
	if req.Term > s.mainNode.CurrentTerm {
		s.mainNode.VotedFor = ""
		s.mainNode.CurrentTerm = req.Term
		s.mainNode.VotesReceived = make(map[string]bool)
		s.mainNode.State = Follower
		go s.mainNode.PersistRaftState()
	}
	s.mainNode.ResetElectionTimeout()
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
		go s.mainNode.PersistRaftState()
		return &RequestVoteResponse{Term: s.mainNode.CurrentTerm, VoteGranted: true, VoterId: s.mainNode.ID}, nil
	}
	return &RequestVoteResponse{Term: s.mainNode.CurrentTerm, VoteGranted: false, VoterId: s.mainNode.ID}, nil
}

func (s *RaftServer) ReceiveVote(req *RequestVoteResponse) {
	s.mainNode.RaftMu.Lock()
	defer s.mainNode.RaftMu.Unlock()
	candidateTerm, voterTerm, voteGranted := s.mainNode.CurrentTerm, req.Term, req.VoteGranted

	if voterTerm > candidateTerm {
		s.mainNode.VotedFor = ""
		s.mainNode.CurrentTerm = voterTerm
		s.mainNode.State = Follower
		s.mainNode.VotesReceived = make(map[string]bool)
		s.mainNode.ResetElectionTimeout()
		go s.mainNode.PersistRaftState()

		return
	}

	if candidateTerm == voterTerm && voteGranted {
		s.mainNode.VotesReceived[req.VoterId] = true
		log.Printf("Node %s: Received vote from %s. Total of %v votes", s.mainNode.ID, req.VoterId, len(s.mainNode.VotesReceived))
		log.Printf("Node %v", s.mainNode.VotesReceived)
	}
}

func (s *RaftServer) ProcessAppendEntriesRequest(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	s.mainNode.RaftMu.Lock()
	defer s.mainNode.RaftMu.Unlock()

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < s.mainNode.CurrentTerm {
		return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
	}

	if req.Term > s.mainNode.CurrentTerm {
		s.mainNode.VotedFor = ""
		s.mainNode.CurrentTerm = req.Term
		s.mainNode.State = Follower
		s.mainNode.VotesReceived = make(map[string]bool)
		go s.mainNode.PersistRaftState()
	}

	s.mainNode.ResetElectionTimeout()

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

			}
			s.mainNode.Log = append(s.mainNode.Log, entry)
			go s.mainNode.PersistRaftState()
		}
	}

	newEntries := s.mainNode.Log[originalLength:]
	if len(newEntries) > 0 {
		err := s.mainNode.SaveLogEntries(newEntries)
		if err != nil {
			return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
			go s.mainNode.PersistRaftState()
		}
	}

	if req.LeaderCommit > s.mainNode.CommitIndex {
		lastEntryIndex := uint64(0)
		if len(s.mainNode.Log) > 0 {
			lastEntryIndex = s.mainNode.Log[len(s.mainNode.Log)-1].Index
		}
		s.mainNode.CommitIndex = min(req.LeaderCommit, lastEntryIndex)
	}
	fmt.Printf("%+v\n", s.mainNode.Log)
	log.Printf("Current Log: %v", s.mainNode.Log)
	return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: true}, nil
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
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

func (s *RaftServer) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
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
