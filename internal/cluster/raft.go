package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
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
	CommitIndex uint64
	LastApplied uint64
}

type Command struct {
	Type  CommandType
	Key   string
	Value []byte
}

type ListenerFactory func(address string) (net.Listener, error)

func ProdListenerFactory(address string) (net.Listener, error) {
	return net.Listen("tcp", address)
}

func MockListenerFactory(address string) (net.Listener, error) {
	return NewMockListener(address), nil
}

type Node struct {
	ID       string `yaml:"id"`
	Address  string `yaml:"address"`
	Port     int    `yaml:"port"`
	GrpcPort int    `yaml:"grpc_port"`
	dataDir  string `yaml:"data_dir"`

	// KV Store data
	data map[string][]byte

	// Raft
	RaftMu sync.Mutex

	peers            []string
	currentTerm      uint64            // Latest term server
	votedFor         string            // Candidate ID that the node voted for
	log              []*LogEntry       // Replicated messages log
	commitIndex      uint64            // Index of highest entry that was known to be committed
	state            RaftState         // Leader, Candidate, Follower
	lastApplied      uint64            // Highest index that was applied to state machine (data)
	leaderID         string            // Current leader's ID, default ""
	votesReceived    map[string]bool   // Set of node IDs that the candidate has received votes from
	nextIndex        map[string]uint64 // (Leader) The next index that the leader will send to a follower
	matchIndex       map[string]uint64 // (Leader) The index that the leader has already replicated its logs up to
	electionTimeout  clockwork.Timer   // Election timer that triggers if no gRPC response is heard from leader
	heartbeatTimeout clockwork.Timer

	appendEntriesChan         chan *AppendEntriesRequestWrapper
	appendEntriesResponseChan chan *AppendEntriesResponseWrapper
	ClientCommandChan         chan *Command
	persistStateChan          chan *PersistentState
	requestVoteChan           chan *RequestVoteRequestWrapper
	requestVoteResponseChan   chan *RequestVoteResponse
	resetElectionTimeoutChan  chan struct{}

	applierCond *sync.Cond

	ctx              context.Context
	cancel           context.CancelFunc
	replicatorCancel map[string]context.CancelFunc

	// WaitGroups
	applierWg    sync.WaitGroup
	raftLoopWg   sync.WaitGroup
	persistWg    sync.WaitGroup
	replicatorWg sync.WaitGroup
	startWg      sync.WaitGroup

	Clock     clockwork.Clock
	Transport NetworkTransport
	listenerFactory ListenerFactory
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

func NewNode(ctx context.Context, cancel context.CancelFunc, ID string, Address string, Port int, GrpcPort int, dataDir string, peerIDs []string, clk clockwork.Clock, lf ListenerFactory, t NetworkTransport) *Node {
	node := &Node{
		ID:                        ID,
		Address:                   Address,
		Port:                      Port,
		GrpcPort:                  GrpcPort,
		data:                      make(map[string][]byte),
		votedFor:                  "",
		peers:                     peerIDs,
		currentTerm:               0,
		dataDir:                   dataDir,
		RaftMu:                    sync.Mutex{},
		log:                       make([]*LogEntry, 0),
		commitIndex:               0,
		state:                     Follower,
		lastApplied:               0,
		leaderID:                  "",
		votesReceived:             make(map[string]bool),
		nextIndex:                 make(map[string]uint64),
		matchIndex:                make(map[string]uint64),
		electionTimeout:           nil,
		appendEntriesChan:         make(chan *AppendEntriesRequestWrapper),
		appendEntriesResponseChan: make(chan *AppendEntriesResponseWrapper),
		ClientCommandChan:         make(chan *Command, 1),
		persistStateChan:          make(chan *PersistentState, 1),
		requestVoteChan:           make(chan *RequestVoteRequestWrapper),
		requestVoteResponseChan:   make(chan *RequestVoteResponse),
		resetElectionTimeoutChan:  make(chan struct{}, 1),
		ctx:                       ctx,
		cancel:                    cancel,
		Clock:                     clk,
		Transport:                 t,
		listenerFactory:           lf,
	}
	listener, err := node.listenerFactory(fmt.Sprintf(":%d", node.GrpcPort))
	return node
}

func (n *Node) Start() {
	err := n.LoadRaftState()
	if err != nil {
		log.Fatalf("Error loading raft state: %v", err)
	}
	n.startWg.Add(1)
	n.applierWg.Add(1)
	n.persistWg.Add(1)
	n.raftLoopWg.Add(1)
	go n.RunRaftLoop()
	go n.PersistStateGoroutine()

	n.applierCond = sync.NewCond(&n.RaftMu)
	go n.ApplierGoroutine()
	n.applierCond.Broadcast()
}

func (n *Node) Shutdown() {
	log.Printf("Initializing shutdown for %s", n.ID)
	if n.ctx != nil {
		n.cancel()
	}
	n.StopReplicators()
	n.applierCond.Broadcast()
}

func (n *Node) WaitAllGoroutines() {
	done := make(chan struct{})
	go func() {
		n.applierWg.Wait()
		n.persistWg.Wait()
		n.raftLoopWg.Wait()
		n.replicatorWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("Node %s: All follower goroutines stopped fully", n.ID)
	case <-n.Clock.After(3 * time.Second):
		log.Printf("Node %s: Timed out after 3 sec. Some goroutines may still be running", n.ID)
	}
}

func (n *Node) resetElectionTimeout() {
	durationMs := rand.Intn(maxElectionTimeoutMs-minElectionTimeoutMs+1) + minElectionTimeoutMs
	timeout := time.Duration(durationMs) * time.Millisecond

	if n.electionTimeout != nil {
		n.electionTimeout.Stop()
	}
	n.electionTimeout = n.Clock.NewTimer(timeout)
	log.Printf("Node %s: Election timeout set to %dms", n.ID, durationMs)

func (n *Node) sendVoteRequestToPeer(voteCtx context.Context, voteCancel context.CancelFunc, peerID string, currentTerm uint64, lastLogIndex uint64, lastLogTerm uint64) {
	defer voteCancel()
	defer n.raftLoopWg.Done()

	select {
	case <-voteCtx.Done():
		return
	default:
		voteRequest := &RequestVoteRequest{
			Term:         currentTerm,
			CandidateId:  n.ID,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		response, err := n.Transport.SendRequestVote(voteCtx, peerID, voteRequest)
		if err != nil {
			log.Printf("Error requesting vote: %v", err)
			return
		}
		select {
		case n.requestVoteResponseChan <- response:
			log.Printf("Processing vote request response: %v, VoteGranted: %v", response, response.GetVoteGranted())
		case <-voteCtx.Done():
			return
		}
	}
}

func (n *Node) RunRaftLoop() {
	log.Printf("Node %s: Starting Raft", n.ID)
	defer func() {
		log.Printf("Node %s: Raft Loop Goroutine stopped", n.ID)
		n.raftLoopWg.Done()
	}()
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

	n.resetElectionTimeout()
	n.startWg.Done()
	for {
		n.RaftMu.Lock()
		currentState := n.state
		log.Printf("Current data: %v", n.data)
		log.Printf("Current Term: %v", n.currentTerm)
		log.Printf("Current lastApplied: %v", n.lastApplied)
		log.Printf("Current commitIndex: %v", n.commitIndex)
		n.RaftMu.Unlock()
		switch currentState {
		case Leader:
			log.Printf("------------------------Leader---------------------------------")
			select {
			case <-n.ctx.Done():
				log.Printf("Leader %s: Shutting down", n.ID)
				return
			case <-n.resetElectionTimeoutChan:
				n.RaftMu.Lock()
				n.resetElectionTimeout()
				n.RaftMu.Unlock()
			case wrappedResp := <-n.appendEntriesResponseChan:
				n.RaftMu.Lock()
				grpcResponse := wrappedResp.Response
				if grpcResponse != nil && n.currentTerm < grpcResponse.Term {
					log.Printf("Leader %s %v term is less than follower %s, reverting to follower.", n.ID, n.currentTerm, wrappedResp.PeerID)
					n.state = Follower
					n.leaderID = ""
					n.currentTerm = grpcResponse.Term
					go n.PersistRaftState()
					n.StopReplicators()
					select {
					case n.resetElectionTimeoutChan <- struct{}{}:
						log.Printf("Candidate - Election timeout")
					default:
						log.Printf("Election timeout channel full")
					}
					n.RaftMu.Unlock()
					continue
				}

				if grpcResponse.Success {
					log.Printf("Leader %s: Successfully replicated to nodes", n.ID)
					if n.nextIndex[wrappedResp.PeerID] > 0 {
						n.matchIndex[wrappedResp.PeerID] = wrappedResp.PrevLogIndex + uint64(len(wrappedResp.SentEntries))
						n.nextIndex[wrappedResp.PeerID] = wrappedResp.PrevLogIndex + uint64(len(wrappedResp.SentEntries)) + 1
					}
				} else {
					log.Printf("Node %s: Failed to replicate to follower", n.ID)
					if n.nextIndex[wrappedResp.PeerID] > 1 {
						n.nextIndex[wrappedResp.PeerID] -= 1
					}
				}
				n.RaftMu.Unlock()
				// Update Leader's Commit Index
				for i := len(n.log) - 1; i >= 0; i-- {
					if n.currentTerm != n.log[i].Term {
						break
					}
					majorityCount := 1
					for _, peerID := range n.peers {
						if n.matchIndex[peerID] >= uint64(i) {
							majorityCount++
						}
					}
					if majorityCount >= len(n.peers)/2+1 {
						n.commitIndex = uint64(i) + 1
						log.Printf("Committed Index is %v", n.commitIndex)
						go n.PersistRaftState()
						n.applierCond.Broadcast()
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
					Term:    n.currentTerm,
					Index:   uint64(len(n.log)) + 1,
					Command: commandBytes,
				}
				n.log = append(n.log, entry)

				go n.PersistRaftState()
				n.RaftMu.Unlock()
				log.Printf("Node %s: Successfully appended log entry", n.ID)
			}
		case Candidate:
			log.Printf("------------------------Candidate---------------------------------")
			select {
			case <-n.ctx.Done():
				log.Printf("Candidate %s: Shutting down", n.ID)
				return
			case <-n.resetElectionTimeoutChan:
				n.RaftMu.Lock()
				n.resetElectionTimeout()
				n.RaftMu.Unlock()
			case <-n.electionTimeout.Chan():
				n.RaftMu.Lock()
				n.currentTerm += 1
				n.votedFor = n.ID
				n.votesReceived = make(map[string]bool)
				n.votesReceived[n.ID] = true
				go n.PersistRaftState()
				select {
				case n.resetElectionTimeoutChan <- struct{}{}:
					log.Printf("Candidate - Election timeout")
				default:
					log.Printf("Election timeout channel full")
				}
				n.RaftMu.Unlock()

				for _, peerID := range n.peers {
					if peerID == n.ID {
						continue
					}
					log.Printf("%s", peerID)
					go func(peerID string) {
						n.RaftMu.Lock()
						currentTerm := n.currentTerm
						lastLogIndex := func() uint64 {
							if len(n.log) == 0 {
								return 0
							}
							return n.log[len(n.log)-1].Index
						}()
						lastLogTerm := func() uint64 {
							if len(n.log) == 0 {
								return 0
							}
							return n.log[len(n.log)-1].Term
						}()
						n.RaftMu.Unlock()

						voteRequest := &RequestVoteRequest{
							Term:         currentTerm,
							CandidateId:  n.ID,
							LastLogIndex: lastLogIndex,
							LastLogTerm:  lastLogTerm,
						}
						response, err := n.Transport.SendRequestVote(peerID, voteRequest)
						if err != nil {
							log.Printf("Error requesting vote: %v", err)
							return
						}
						log.Printf("Vote Request Response: %v", response)
						n.requestVoteResponseChan <- response
					}(peerID)
				}

			case aeWrapper := <-n.appendEntriesChan:
				log.Printf("Request received in appendEntriesChan")
				response, err := raftServer.ProcessAppendEntriesRequest(aeWrapper.Ctx, aeWrapper.Request)
				if err != nil {
					log.Printf("Error appending entries: %v", err)
					if response == nil {
						response = &AppendEntriesResponse{Term: n.currentTerm, Success: false}
					}
				}
				aeWrapper.Response <- response

			case reqVoteReq := <-n.requestVoteChan:
				log.Printf("Node %s received a vote request from %s", n.ID, reqVoteReq.Request.CandidateId)
				response, err := raftServer.ProcessVoteRequest(reqVoteReq.Ctx, reqVoteReq.Request)
				if err != nil {
					log.Printf("Error requesting vote: %v", err)
				}
				reqVoteReq.Response <- response

			case reqVoteRespWrapper := <-n.requestVoteResponseChan:
				raftServer.ReceiveVote(reqVoteRespWrapper)

				n.RaftMu.Lock()
				votesReceivedLen, peersLen := len(n.votesReceived), len(n.peers)

				if uint64(votesReceivedLen) >= uint64(peersLen/2)+1 {
					n.state = Leader
					n.leaderID = n.ID
					n.nextIndex = make(map[string]uint64)
					n.matchIndex = make(map[string]uint64)
					for _, peerID := range n.peers {
						if peerID == n.ID {
							continue
						}
						n.nextIndex[peerID] = func() uint64 {
							if len(n.log) == 0 {
								return 1
							}
							return n.log[len(n.log)-1].Index + 1
						}()
						n.matchIndex[peerID] = 0
					}
					log.Printf("New Leader - %s. votesReceived %v", n.ID, n.votesReceived)
					n.StartReplicators()
				}
				n.RaftMu.Unlock()
			}

		case Follower:
			log.Printf("------------------------Follower---------------------------------")
			select {
			case <-n.ctx.Done():
				log.Printf("Follower %s: Shutting down", n.ID)
				return
			case <-n.resetElectionTimeoutChan:
				n.RaftMu.Lock()
				n.resetElectionTimeout()
				n.RaftMu.Unlock()
			case <-n.electionTimeout.Chan():
				log.Printf("Follower - Election timeout")
				n.RaftMu.Lock()
				n.state = Candidate
				n.currentTerm += 1
				n.votedFor = n.ID
				n.votesReceived = make(map[string]bool)
				n.votesReceived[n.ID] = true
				go n.PersistRaftState()
				n.RaftMu.Unlock()

				select {
				case n.resetElectionTimeoutChan <- struct{}{}:
					log.Printf("Candidate - Election timeout")
				default:
					log.Printf("Election timeout channel full")
				}

			case aeReq := <-n.appendEntriesChan:
				log.Printf("Request received in appendEntriesChan")
				response, err := raftServer.ProcessAppendEntriesRequest(aeReq.Ctx, aeReq.Request)
				if err != nil {
					log.Printf("Error appending entries: %v", err)
					if response == nil {
						response = &AppendEntriesResponse{Term: n.currentTerm, Success: false}
					}
				}
				aeReq.Response <- response

			case reqVoteReq := <-n.requestVoteChan:
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
