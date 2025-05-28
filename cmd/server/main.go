package main

import (
	"distributed_kv_store/internal/cluster"
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	Node struct {
		ID       string `yaml:"id"`
		Address  string `yaml:"address"`
		Port     string `yaml:"port"`
		GrpcPort string `yaml:"grpc_port"`
		DataDir  string `yaml:"data_dir"`
	} `yaml:"node"`
	Cluster struct {
		Peers []*cluster.Node `yaml:"peers"`
	} `yaml:"cluster"`
}

func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	yamlFile, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func main() {
	configFile := flag.String("config", "config-node1.yaml", "Path to config file")
	flag.Parse()
	fmt.Printf("Starting server with configuration file: %s\n", *configFile) // Print the config file name

	cfg, err := LoadConfig(*configFile)
	if err != nil {
		log.Fatal(err)
	}
	port, err := strconv.Atoi(cfg.Node.Port)
	if err != nil {
		log.Fatal(err)
	}
	gRpcPort, err := strconv.Atoi(cfg.Node.GrpcPort)
	if err != nil {
		log.Fatal(err)
	}

	node := &cluster.Node{
		ID:                      cfg.Node.ID,
		Address:                 cfg.Node.Address,
		Port:                    port,
		GrpcPort:                gRpcPort,
		Data:                    make(map[string][]byte),
		VotedFor:                "",
		CurrentTerm:             0,
		DataDir:                 cfg.Node.DataDir,
		Mu:                      sync.Mutex{},
		RaftMu:                  sync.Mutex{},
		Log:                     make([]*cluster.LogEntry, 0),
		CommitIndex:             0,
		State:                   cluster.Follower,
		LeaderID:                "",
		VotesReceived:           make(map[string]bool),
		NextIndex:               make(map[string]uint64),
		MatchIndex:              make(map[string]uint64),
		ElectionTimeout:         nil,
		HeartbeatTimeout:        nil,
		AppendEntriesChan:       make(chan *cluster.AppendEntriesRequestWrapper),
		RequestVoteChan:         make(chan *cluster.RequestVoteRequestWrapper),
		RequestVoteResponseChan: make(chan *cluster.RequestVoteResponse),
	}

	var peers cluster.NodeMap
	peers.Nodes = make([]*cluster.Node, 0, len(cfg.Cluster.Peers))
	for _, peer := range cfg.Cluster.Peers {
		peers.Nodes = append(peers.Nodes, &cluster.Node{
			ID:       peer.ID,
			Address:  peer.Address,
			Port:     peer.Port,
			GrpcPort: peer.GrpcPort,
			DataDir:  peer.DataDir,
		})
	}

	grpcServer := grpc.NewServer()
	raftServer := cluster.NewRaftServer(node)
	cluster.RegisterRaftServiceServer(grpcServer, raftServer)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", node.GrpcPort))
	if err != nil {
		log.Fatalf("Error listening on port %d: %v", node.GrpcPort, err)
	}

	go func() {
		log.Printf("Listening on port %d", node.GrpcPort)
		node.State = cluster.Follower
		serveErr := grpcServer.Serve(listener)
		if serveErr != nil {
			log.Fatalf("Error serving on port %d: %v", node.GrpcPort, serveErr)
		}
		for {
			node.RaftMu.Lock()
			currentState := node.State
			node.RaftMu.Unlock()

			switch currentState {
			case cluster.Leader:
				select {
				case <-node.ElectionTimeout.C:
					node.RaftMu.Lock()
					log.Printf("Leader election timeout")
					node.State = cluster.Follower
					node.LeaderID = ""
					node.ResetElectionTimeout()
					err := node.SaveRaftState()
					if err != nil {
						return
					}
					node.RaftMu.Unlock()
				case <-node.HeartbeatTimeout.C:
					log.Printf("Heartbeat timeout")
					node.RaftMu.Lock()
					requestCopy := &cluster.AppendEntriesRequest{
						Term:     node.CurrentTerm,
						LeaderId: node.LeaderID,
						PrevLogIndex: func() uint64 {
							if len(node.Log) == 0 {
								return 0
							}
							return node.Log[len(node.Log)-1].Index
						}(),
						PrevLogTerm: func() uint64 {
							if len(node.Log) == 0 {
								return 0
							}
							return node.Log[len(node.Log)-1].Term
						}(),
						Entries:      make([]*cluster.LogEntry, 0),
						LeaderCommit: node.CommitIndex,
					}
					node.RaftMu.Unlock()
					node.ResetHeartbeatTimeout()

					for _, peer := range peers.Nodes {
						go func(peer *cluster.Node) {
							conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", peer.Address, peer.GrpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
							defer func(conn *grpc.ClientConn) {
								err := conn.Close()
								if err != nil {
									log.Printf("Error closing peer %s:%d %v", peer.Address, peer.Port, err)
								}
							}(conn)
							if err != nil {
								log.Printf("Error connecting to peer %s:%d %v", peer.Address, peer.Port, err)
								return
							}
							peerClient := cluster.NewRaftServiceClient(conn)

							ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
							defer cancel()
							appendEntriesRequest := &cluster.AppendEntriesRequest{
								Term:         requestCopy.Term,
								LeaderId:     requestCopy.LeaderId,
								PrevLogIndex: requestCopy.PrevLogIndex,
								PrevLogTerm:  requestCopy.PrevLogTerm,
								Entries:      requestCopy.Entries,
								LeaderCommit: requestCopy.LeaderCommit,
							}
							_, err = peerClient.AppendEntries(ctx, appendEntriesRequest)
							if err != nil {
								log.Printf("Error sending heartbeat: %v", err)
							}
						}(peer)
					}
				case <-node.AppendEntriesChan:

				}

			case cluster.Candidate:
				select {
				case <-node.ElectionTimeout.C:
					node.RaftMu.Lock()
					log.Printf("Election timeout")

					node.CurrentTerm += 1
					node.VotedFor = node.ID
					node.VotesReceived = make(map[string]bool)
					node.VotesReceived[node.ID] = true
					node.SaveRaftState()
					defer node.RaftMu.Unlock()

					node.ResetElectionTimeout()
					for _, peer := range peers.Nodes {
						go func(peer *cluster.Node) {
							node.RaftMu.Lock()
							defer node.RaftMu.Unlock()
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
							peerClient := cluster.NewRaftServiceClient(conn)

							ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
							defer cancel()
							voteRequest := &cluster.RequestVoteRequest{
								Term:        node.CurrentTerm,
								CandidateId: node.ID,
								LastLogIndex: func() uint64 {
									if len(node.Log) == 0 {
										return 0
									}
									return node.Log[len(node.Log)-1].Index
								}(),
								LastLogTerm: func() uint64 {
									if len(node.Log) == 0 {
										return 0
									}
									return node.Log[len(node.Log)-1].Term
								}(),
							}
							response, err := peerClient.RequestVote(ctx, voteRequest)
							if err != nil {
								log.Printf("Error requesting vote: %v", err)
							}
							node.RequestVoteResponseChan <- response
						}(peer)
					}

				case aeWrapper := <-node.AppendEntriesChan:
					node.RaftMu.Lock()
					ctx, cancel := context.WithTimeout(aeWrapper.Ctx, time.Millisecond*50)

					response, err := raftServer.AppendEntries(ctx, aeWrapper.Request)
					if err != nil {
						log.Printf("Error appending entries: %v", err)
						if response == nil {
							response = &cluster.AppendEntriesResponse{Term: node.CurrentTerm, Success: false}
						}
					}
					cancel()
					node.RaftMu.Unlock()
					aeWrapper.Response <- response

				case reqVoteReq := <-node.RequestVoteChan:
					node.RaftMu.Lock()
					ctx, cancel := context.WithTimeout(reqVoteReq.Ctx, 50*time.Millisecond)

					response, err := raftServer.RequestVote(ctx, reqVoteReq.Request)
					if err != nil {
						log.Printf("Error requesting vote: %v", err)
					}
					cancel()
					node.RaftMu.Unlock()
					reqVoteReq.Response <- response
				case recVoteReq := <-node.RequestVoteResponseChan:
					node.RaftMu.Lock()
					raftServer.ReceiveVote(recVoteReq)

					if uint64(len(node.VotesReceived)) > uint64(len(peers.Nodes)/2+1) {
						node.State = cluster.Leader
						node.LeaderID = node.ID

						node.NextIndex = make(map[string]uint64)
						node.MatchIndex = make(map[string]uint64)
						lastLogIndex := uint64(0)
						if len(node.Log) > 0 {
							lastLogIndex = node.Log[len(node.Log)-1].Index
						}
						for _, peer := range peers.Nodes {
							node.NextIndex[peer.ID] = lastLogIndex + 1
							node.MatchIndex[peer.ID] = 0
						}
					}
					node.RaftMu.Unlock()
				}
			case cluster.Follower:
				select {
				case <-node.ElectionTimeout.C:
					node.RaftMu.Lock()
					node.State = cluster.Candidate
					node.CurrentTerm += 1
					node.VotedFor = node.ID
					node.ResetElectionTimeout()
					node.RaftMu.Unlock()

				case aeReq := <-node.AppendEntriesChan:
					node.RaftMu.Lock()
					ctx, cancel := context.WithTimeout(aeReq.Ctx, time.Millisecond*50)

					response, err := raftServer.AppendEntries(ctx, aeReq.Request)
					if err != nil {
						log.Printf("Error appending entries: %v", err)
						if response == nil {
							response = &cluster.AppendEntriesResponse{Term: node.CurrentTerm, Success: false}
						}
					}
					cancel()
					node.RaftMu.Unlock()
					aeReq.Response <- response

				case reqVoteReq := <-node.RequestVoteChan:
					node.RaftMu.Lock()
					ctx, cancel := context.WithTimeout(reqVoteReq.Ctx, 50*time.Millisecond)

					response, err := raftServer.RequestVote(ctx, reqVoteReq.Request)
					if err != nil {
						log.Printf("Error requesting vote: %v", err)
					}
					cancel()
					node.RaftMu.Unlock()
					reqVoteReq.Response <- response
				}
			}
		}
	}()

	//http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	//	if r.Method == http.MethodPut {
	//		key := r.URL.Path[1:]
	//		body, err := io.ReadAll(r.Body)
	//		if err != nil {
	//			http.Error(w, err.Error(), http.StatusBadRequest)
	//			return
	//		}
	//		defer r.Body.Close()
	//		putCmd := cluster.Command{
	//			Type:  cluster.CommandPut,
	//			Key:   key,
	//			Value: body,
	//		}
	//
	//		commandBytes, err := yaml.Marshal(putCmd)
	//		testLogEntry := &cluster.LogEntry{
	//			Term:    1,
	//			Index:   1,
	//			Command: commandBytes,
	//		}
	//		conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:8080"),
	//			grpc.WithTransportCredentials(insecure.NewCredentials()),
	//		)
	//		peerClient := cluster.NewRaftServiceClient(conn)
	//
	//		req := &cluster.AppendEntriesRequest{
	//			Term:         1,
	//			LeaderId:     node.ID,
	//			PrevLogIndex: 0,
	//			PrevLogTerm:  0,
	//			Entries:      []*cluster.LogEntry{testLogEntry},
	//			LeaderCommit: 0,
	//		}
	//
	//		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	//		defer cancel()
	//
	//		resp, err := peerClient.AppendEntries(ctx, req)
	//		if err != nil {
	//			log.Printf("Error replicating: 127.0.0.1:8080: %v", err)
	//		} else {
	//			log.Printf("Replicated to 127.0.0.1:8080. Success: %v", resp.GetSuccess())
	//		}
	//
	//		err = conn.Close()
	//		fmt.Printf("GET successful for key: %s\n", key)
	//	}
	//})
	fmt.Printf("Listening on port %d\n", node.Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil))
}
