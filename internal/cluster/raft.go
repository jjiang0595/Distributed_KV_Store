package cluster

import (
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"runtime"
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
	Value string
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
	data map[string]string

	// Raft
	raftMu sync.Mutex
	rwMu   sync.RWMutex

	peers             []string
	currentTerm       uint64            // Latest term server
	votedFor          string            // Candidate ID that the node voted for
	log               []*LogEntry       // Replicated messages log
	commitIndex       uint64            // Index of highest entry that was known to be committed
	state             RaftState         // Leader, Candidate, Follower
	lastApplied       uint64            // Highest index that was applied to state machine (data)
	leaderID          string            // Current leader's ID, default ""
	votesReceived     map[string]bool   // Set of node IDs that the candidate has received votes from
	nextIndex         map[string]uint64 // (Leader) The next index that the leader will send to a follower
	matchIndex        map[string]uint64 // (Leader) The index that the leader has already replicated its logs up to
	electionTimeout   clockwork.Timer   // Election timer that triggers if no gRPC response is heard from leader
	heartbeatTimeout  clockwork.Timer
	electionTimeoutCh chan struct{}

	applyChan                 chan ApplyMsg
	appendEntriesChan         chan *AppendEntriesRequestWrapper
	appendEntriesResponseChan chan *AppendEntriesResponseWrapper
	clientCommandChan         chan *ProposeRequest
	pendingCommands           map[uint64]chan error
	persistStateChan          chan struct{}
	replicatorSendNowChan     map[string]chan struct{}
	requestVoteChan           chan *RequestVoteRequestWrapper
	requestVoteResponseChan   chan *RequestVoteResponse
	resetElectionTimeoutChan  chan struct{}

	grpcServer  *grpc.Server
	applierCond *sync.Cond

	ctx              context.Context
	cancel           context.CancelFunc
	replicatorCancel map[string]context.CancelFunc

	// WaitGroups
	applierWg    sync.WaitGroup
	grpcWg       sync.WaitGroup
	raftLoopWg   sync.WaitGroup
	persistWg    sync.WaitGroup
	replicatorWg sync.WaitGroup

	dirtyPersistenceState bool

	Clock           clockwork.Clock
	Transport       NetworkTransport
	grpcListener    net.Listener
	raftServer      *RaftServer
	listenerFactory ListenerFactory
	kvStore         *KVStore

	minElectionTimeout int
	maxElectionTimeout int
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

func (s *RaftServer) Close() error {
	return nil
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
		grpcListener:              nil,
		votedFor:                  "",
		peers:                     peerIDs,
		currentTerm:               0,
		dataDir:                   dataDir,
		raftMu:                    sync.Mutex{},
		rwMu:                      sync.RWMutex{},
		log:                       make([]*LogEntry, 0),
		commitIndex:               0,
		state:                     Follower,
		lastApplied:               0,
		leaderID:                  "",
		votesReceived:             make(map[string]bool),
		nextIndex:                 make(map[string]uint64),
		matchIndex:                make(map[string]uint64),
		electionTimeout:           nil,
		grpcServer:                nil,
		applyChan:                 make(chan ApplyMsg),
		appendEntriesChan:         make(chan *AppendEntriesRequestWrapper, 1),
		appendEntriesResponseChan: make(chan *AppendEntriesResponseWrapper),
		clientCommandChan:         make(chan *ProposeRequest),
		persistStateChan:          make(chan struct{}, 1),
		pendingCommands:           make(map[uint64]chan error),
		requestVoteChan:           make(chan *RequestVoteRequestWrapper, 1),
		requestVoteResponseChan:   make(chan *RequestVoteResponse),
		resetElectionTimeoutChan:  make(chan struct{}, 1),
		electionTimeoutCh:         make(chan struct{}, 1),
		ctx:                       ctx,
		cancel:                    cancel,
		dirtyPersistenceState:     false,
		Clock:                     clk,
		Transport:                 t,
		listenerFactory:           lf,
		kvStore:                   NewKVStore(),
		minElectionTimeout:        minElectionTimeoutMs,
		maxElectionTimeout:        maxElectionTimeoutMs,
	}
	node.applierCond = sync.NewCond(&node.raftMu)

	node.grpcServer = grpc.NewServer()
	node.raftServer = NewRaftServer(node)
	RegisterRaftServiceServer(node.grpcServer, node.raftServer)
	listener, err := node.listenerFactory(fmt.Sprintf(":%d", node.GrpcPort))
	if err != nil {
		log.Fatalf("Error listening on gRPC port %d: %v", node.GrpcPort, err)
	}

	node.grpcListener = listener
	return node
}

func (n *Node) Start() {
	err := n.LoadRaftState()
	if err != nil {
		log.Fatalf("Error loading raft state: %v", err)
	}

	n.grpcWg.Add(1)
	go func() {
		defer func() {
			n.grpcWg.Done()
			log.Printf("Node %s: gRPC server stopped", n.ID)
		}()
		log.Printf("Listening on gRPC port %d", n.GrpcPort)
		serveErr := n.grpcServer.Serve(n.grpcListener)
		if serveErr != nil {
			log.Printf("Error serving on gRPC port %d: %v", n.GrpcPort, serveErr)
		}
	}()

	n.persistWg.Add(1)
	go n.PersistStateGoroutine()

	n.applierWg.Add(1)
	go n.ApplierGoroutine()

	n.raftLoopWg.Add(1)
	go n.RunRaftLoop()
}

func (n *Node) Shutdown() {
	log.Printf("Initializing shutdown for %s", n.ID)
	n.raftMu.Lock()
	defer n.raftMu.Unlock()

	if n.ctx != nil {
		n.cancel()
	}
	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
	}
	n.StopReplicators()
	n.applierCond.Broadcast()

	if n.grpcListener != nil {
		n.grpcListener.Close()
	}
}

func (n *Node) WaitAllGoroutines() {
	done := make(chan struct{})
	go func() {
		n.applierWg.Wait()
		n.persistWg.Wait()
		n.raftLoopWg.Wait()
		n.replicatorWg.Wait()
		n.grpcWg.Wait()
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
	durationMs := rand.Intn(n.maxElectionTimeout-n.minElectionTimeout+1) + n.minElectionTimeout
	timeout := time.Duration(durationMs) * time.Millisecond

	if n.electionTimeout != nil {
		n.electionTimeout.Stop()
	}
	n.electionTimeout = n.Clock.NewTimer(timeout)
}

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
		response, err := n.Transport.SendRequestVote(voteCtx, n.ID, peerID, voteRequest)
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

type ProposeRequest struct {
	Command []byte
	errorCh chan error
}

func (n *Node) ProposeCommand(cmdBytes []byte) error {
	if n.GetState() != Leader {
		if n.GetLeaderID() != "" {
			return fmt.Errorf("not leader, current leader is %s", n.leaderID)
		}
		return fmt.Errorf("not leader")
	}

	proposeReq := &ProposeRequest{
		Command: cmdBytes,
		errorCh: make(chan error, 1),
	}
	select {
	case n.clientCommandChan <- proposeReq:
		select {
		case err := <-proposeReq.errorCh:
			return err
		case <-n.Clock.After(500 * time.Millisecond):
			return fmt.Errorf("timed out waiting for propose command")
		}
	case <-n.Clock.After(500 * time.Millisecond):
		return fmt.Errorf("failed to send propose command to raft loop")
	}
}

func (n *Node) RunRaftLoop() {
	log.Printf("Node %s: Starting Raft", n.ID)
	defer func() {
		log.Printf("Node %s: Raft Loop Goroutine stopped", n.ID)
		n.raftLoopWg.Done()
	}()

	n.resetElectionTimeout()
	for {
		//log.Printf("Node %s: Current commitIndex: %v, Current Term: %v, Current Log: %v, Current lastApplied: %v", n.ID, n.commitIndex, n.currentTerm, n.log, n.lastApplied)
		switch n.GetState() {
		case Leader:
			log.Printf("------------------------Leader---------------------------------")
			select {
			case <-n.ctx.Done():
				log.Printf("Leader %s: Shutting down", n.ID)
				return
			case wrappedResp := <-n.appendEntriesResponseChan:
				log.Printf("Leader %s: Received the Append Entries Response %v", n.ID, wrappedResp)
				n.raftMu.Lock()
				oldTerm, oldVotedFor := n.currentTerm, n.votedFor
				oldLogLength := len(n.log)

				grpcResponse := wrappedResp.Response
				if grpcResponse != nil && n.currentTerm < grpcResponse.Term {
					log.Printf("Leader %s %v term is less than follower %s, reverting to follower.", n.ID, n.currentTerm, wrappedResp.PeerID)
					n.state = Follower
					n.leaderID = ""
					n.currentTerm = grpcResponse.Term
					n.votesReceived = make(map[string]bool)
					n.votedFor = ""
					n.resetElectionTimeout()
					n.StopReplicators()
					log.Printf("Leader %s: Stepping down", n.ID)
					n.raftMu.Unlock()
					n.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
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
						select {
						case n.replicatorSendNowChan[wrappedResp.PeerID] <- struct{}{}:
						default:
						}
					}
					continue
				}
				n.raftMu.Unlock()
				// Update Leader's Commit Index
				for i := len(n.GetLog()) - 1; i >= 0; i-- {
					if n.GetCurrentTerm() != n.log[i].GetTerm() {
						break
					}
					majorityCount := 1
					for _, peerID := range n.peers {
						if n.GetMatchIndex()[peerID] >= uint64(i) {
							majorityCount++
						}
					}
					log.Printf("Leader %s: Majority count: %d", n.ID, majorityCount)
					if majorityCount >= (len(n.peers)+1)/2+1 {
						n.raftMu.Lock()
						n.commitIndex = n.log[i].Index
						log.Printf("%s Committed Index is %v", n.ID, n.commitIndex)
						n.raftMu.Unlock()
						n.applierCond.Broadcast()
						break
					}
				}
			case clientReq := <-n.clientCommandChan:
				log.Printf("Client Request Received: %v", clientReq)
				commandBytes := clientReq.Command

				n.raftMu.Lock()
				entry := &LogEntry{
					Term: n.currentTerm,
					Index: func() uint64 {
						if len(n.log) == 0 {
							return 0
						}
						return uint64(n.log[len(n.log)-1].Index) + 1
					}(),
					Command: commandBytes,
				}

				n.pendingCommands[entry.Index] = clientReq.errorCh
				oldTerm, oldVotedFor := n.currentTerm, n.votedFor
				oldLogLength := len(n.log)
				n.log = append(n.log, entry)

				n.matchIndex[n.ID] = entry.Index
				n.nextIndex[n.ID] = entry.Index + 1

				n.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
				n.raftMu.Unlock()

				for peerID, peerSendCh := range n.replicatorSendNowChan {
					select {
					case peerSendCh <- struct{}{}:
						log.Printf("Leader %s: Signal replicator to send log entry to %v", n.ID, peerID)
					default:
						log.Printf("Leader %s: Replicator is blocked while sending to %v.", n.ID, peerID)
					}
				}
			}
		case Candidate:
			log.Printf("------------------------Candidate---------------------------------")
			select {
			case <-n.ctx.Done():
				log.Printf("Candidate %s: Shutting down", n.ID)
				return
			case <-n.resetElectionTimeoutChan:
				n.raftMu.Lock()
				n.resetElectionTimeout()
				n.raftMu.Unlock()

			case <-n.electionTimeout.Chan():
				n.raftMu.Lock()
				n.currentTerm += 1
				n.votedFor = n.ID
				n.votesReceived = make(map[string]bool)
				n.votesReceived[n.ID] = true
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
				oldTerm, oldVotedFor := n.currentTerm, n.votedFor
				oldLogLength := len(n.log)
				n.resetElectionTimeout()
				n.raftMu.Unlock()

				n.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)

				for _, peerID := range n.peers {
					voteCtx, voteCancel := context.WithTimeout(n.ctx, time.Millisecond*50)
					n.raftLoopWg.Add(1)
					go n.sendVoteRequestToPeer(voteCtx, voteCancel, peerID, n.currentTerm, lastLogIndex, lastLogTerm)
				}

			case aeWrapper := <-n.appendEntriesChan:
				log.Printf("Request received in appendEntriesChan")
				response, err := n.raftServer.ProcessAppendEntriesRequest(aeWrapper.Ctx, aeWrapper.Request)
				if err != nil {
					log.Printf("Error appending entries: %v", err)
					if response == nil {
						response = &AppendEntriesResponse{Term: n.currentTerm, Success: false}
					}
				}
				select {
				case <-n.ctx.Done():
					log.Printf("Candidate %s: Parent Context is Cancelled", n.ID)
					return
				case aeWrapper.Response <- response:
					log.Printf("Processing AppendEntries Response: %v", response)
				}

			case reqVoteReq := <-n.requestVoteChan:
				log.Printf("Node %s received a vote request from %s at %v", n.ID, reqVoteReq.Request.CandidateId, n.Clock.Now())
				response, err := n.raftServer.ProcessVoteRequest(reqVoteReq.Ctx, reqVoteReq.Request)
				if err != nil {
					log.Printf("Error requesting vote: %v", err)
				}
				select {
				case <-n.ctx.Done():
					log.Printf("Candidate %s: Parent Context is Cancelled", n.ID)
					return
				case reqVoteReq.Response <- response:
					log.Printf("Node %s: Sending Back Request Vote Response: %v %v at time %v", n.ID, response, response.GetVoteGranted(), n.Clock.Now())
				}

			case reqVoteRespWrapper := <-n.requestVoteResponseChan:
				n.raftServer.ReceiveVote(reqVoteRespWrapper)

				n.raftMu.Lock()
				votesReceivedLen, totalNodes := len(n.votesReceived), len(n.peers)+1

				if uint64(votesReceivedLen) >= uint64(totalNodes/2)+1 {
					n.state = Leader
					n.electionTimeout.Stop()
					n.leaderID = n.ID
					n.nextIndex = make(map[string]uint64)
					n.matchIndex = make(map[string]uint64)
					for _, peerID := range n.peers {
						n.nextIndex[peerID] = func() uint64 {
							if len(n.log) == 0 {
								return 1
							}
							return n.log[len(n.log)-1].Index + 1
						}()
						n.matchIndex[peerID] = 0
					}
					log.Printf("New Leader - %s at time %v. votesReceived %v", n.ID, n.Clock.Now(), n.votesReceived)
					go n.StartReplicators()
					noOpEntry := &LogEntry{
						Term: n.currentTerm,
						Index: func() uint64 {
							if len(n.log) == 0 {
								return 0
							}
							return n.log[len(n.log)-1].Index + 1
						}(),
						Command: []byte("NO_OP_ENTRY"),
					}
					n.log = append(n.log, noOpEntry)
					n.dirtyPersistenceState = true
					select {
					case n.persistStateChan <- struct{}{}:
					default:
					}
					select {
					case n.replicatorSendNowChan[n.ID] <- struct{}{}:
					default:
					}
				}
				n.raftMu.Unlock()
			}

		case Follower:
			log.Printf("------------------------Follower---------------------------------")
			select {
			case <-n.ctx.Done():
				log.Printf("Follower %s: Shutting down", n.ID)
				return
			case <-n.resetElectionTimeoutChan:
				n.raftMu.Lock()
				n.resetElectionTimeout()
				n.raftMu.Unlock()
			case <-n.electionTimeout.Chan():
				n.raftMu.Lock()
				//log.Printf("%s: Follower -> Candidate: Time %v", n.ID, n.Clock.Now())
				log.Printf("%s: Follower -> Candidate", n.ID)
				n.state = Candidate
				n.currentTerm += 1
				n.votedFor = n.ID
				n.votesReceived = make(map[string]bool)
				n.votesReceived[n.ID] = true
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
				oldTerm, oldVotedFor := n.currentTerm, n.votedFor
				oldLogLength := len(n.log)
				n.resetElectionTimeout()
				n.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
				n.raftMu.Unlock()

				go n.sendVoteRequestToPeers(n.currentTerm, lastLogIndex, lastLogTerm)

			case <-n.electionTimeoutCh:
				n.raftMu.Lock()
				//log.Printf("%s: Follower -> Candidate: Time %v", n.ID, n.Clock.Now())
				log.Printf("%s: Follower -> Candidate", n.ID)
				runtime.Gosched()
				n.state = Candidate
				n.currentTerm += 1
				n.votedFor = n.ID
				n.votesReceived = make(map[string]bool)
				n.votesReceived[n.ID] = true
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
				oldTerm, oldVotedFor := n.currentTerm, n.votedFor
				oldLogLength := len(n.log)
				n.resetElectionTimeout()
				n.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
				n.raftMu.Unlock()

				go n.sendVoteRequestToPeers(n.currentTerm, lastLogIndex, lastLogTerm)

			case aeReq := <-n.appendEntriesChan:
				response, err := n.raftServer.ProcessAppendEntriesRequest(aeReq.Ctx, aeReq.Request)
				if err != nil {
					log.Printf("Error appending entries: %v", err)
					if response == nil {
						response = &AppendEntriesResponse{Term: n.GetCurrentTerm(), Success: false}
					}
				}
				select {
				case <-n.ctx.Done():
					log.Printf("Follower %s: Parent Context is Cancelled", n.ID)
					return
				case aeReq.Response <- response:
					log.Printf("Processing AppendEntries Request Response")
				}

			case reqVoteReq := <-n.requestVoteChan:
				//log.Printf("Node %s: Received a vote request from %s at time %v", n.ID, reqVoteReq.Request.CandidateId, n.Clock.Now())
				response, err := n.raftServer.ProcessVoteRequest(reqVoteReq.Ctx, reqVoteReq.Request)
				if err != nil {
					log.Printf("Error requesting vote: %v", err)
				}
				select {
				case <-n.ctx.Done():
					log.Printf("Follower %s: Parent Context is Cancelled", n.ID)
					return
				case reqVoteReq.Response <- response:
					log.Printf("Node %s: Sending Back Request Vote Response: %v %v at time %v", n.ID, response, response.GetVoteGranted(), n.Clock.Now())
				}
			}
		}
	}
}

func (n *Node) sendVoteRequestToPeers(currentTerm uint64, lastLogIndex uint64, lastLogTerm uint64) {
	for _, peerID := range n.peers {
		voteCtx, voteCancel := context.WithTimeout(n.ctx, time.Millisecond*50)
		n.raftLoopWg.Add(1)
		go n.sendVoteRequestToPeer(voteCtx, voteCancel, peerID, currentTerm, lastLogIndex, lastLogTerm)
	}
}
