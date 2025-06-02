package main

import (
	"distributed_kv_store/internal/cluster"
	"encoding/json"
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
		ID:                        cfg.Node.ID,
		Address:                   cfg.Node.Address,
		Port:                      port,
		GrpcPort:                  gRpcPort,
		Data:                      make(map[string][]byte),
		VotedFor:                  "",
		Peers:                     make(map[string]*cluster.Node),
		CurrentTerm:               0,
		DataDir:                   cfg.Node.DataDir,
		Mu:                        sync.Mutex{},
		RaftMu:                    sync.Mutex{},
		Log:                       make([]*cluster.LogEntry, 0),
		CommitIndex:               0,
		State:                     cluster.Follower,
		LastApplied:               0,
		LeaderID:                  "",
		VotesReceived:             make(map[string]bool),
		NextIndex:                 make(map[string]uint64),
		MatchIndex:                make(map[string]uint64),
		ElectionTimeout:           nil,
		AppendEntriesChan:         make(chan *cluster.AppendEntriesRequestWrapper),
		AppendEntriesResponseChan: make(chan *cluster.AppendEntriesResponseWrapper),
		ClientCommandChan:         make(chan *cluster.Command, 5),
		RequestVoteChan:           make(chan *cluster.RequestVoteRequestWrapper),
		RequestVoteResponseChan:   make(chan *cluster.RequestVoteResponse),
	}

	for _, peer := range cfg.Cluster.Peers {
		if peer.ID == node.ID {
			continue
		}
		node.Peers[peer.ID] = &cluster.Node{
			ID:       peer.ID,
			Address:  peer.Address,
			Port:     peer.Port,
			GrpcPort: peer.GrpcPort,
			DataDir:  peer.DataDir,
		}
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
				case clientReq := <-node.ClientCommandChan:
					node.RaftMu.Lock()
					commandBytes, err := json.Marshal(clientReq)
					if err != nil {
						log.Printf("Error marshalling command to bytes: %v", err)
						node.RaftMu.Unlock()
						continue
					}
					entry := &cluster.LogEntry{
						Term:    node.CurrentTerm,
						Index:   uint64(len(node.Log)) + 1,
						Command: commandBytes,
					}
					node.Log = append(node.Log, entry)

					if err := node.SaveLogEntry(entry); err != nil {
						log.Fatalf("Node %s: Error appending log entry: %v. Crashing node.", node.ID, err)
					}
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
					err := node.SaveRaftState()
					if err != nil {
						log.Fatalf("Error updating node state: %v", err)
					}
					node.RaftMu.Unlock()

					node.ResetElectionTimeout()
					for _, peer := range node.Peers {
						go func(peer *cluster.Node) {
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
					if node.CommitIndex > node.LastApplied {
						node.ApplyCommittedEntries()
					}
					cancel()
					node.RaftMu.Unlock()
					aeWrapper.Response <- response

				case reqVoteWrapper := <-node.RequestVoteChan:
					node.RaftMu.Lock()
					ctx, cancel := context.WithTimeout(reqVoteWrapper.Ctx, 50*time.Millisecond)

					response, err := raftServer.RequestVote(ctx, reqVoteWrapper.Request)
					if err != nil {
						log.Printf("Error requesting vote: %v", err)
					}
					cancel()
					node.RaftMu.Unlock()
					reqVoteWrapper.Response <- response

				case reqVoteRespWrapper := <-node.RequestVoteResponseChan:
					node.RaftMu.Lock()
					raftServer.ReceiveVote(reqVoteRespWrapper)

					if uint64(len(node.VotesReceived)) > uint64((len(node.Peers)+1)/2+1) {
						node.State = cluster.Leader
						node.LeaderID = node.ID
						node.NextIndex = make(map[string]uint64)
						node.MatchIndex = make(map[string]uint64)
						for _, peer := range node.Peers {
							node.NextIndex[peer.ID] = 1
							node.MatchIndex[peer.ID] = 0
						}

						node.StartReplicators()
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
					if node.CommitIndex > node.LastApplied {
						node.ApplyCommittedEntries()
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

	fmt.Printf("Listening on port %d\n", node.Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil))
}
