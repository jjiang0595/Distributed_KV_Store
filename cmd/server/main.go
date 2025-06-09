package main

import (
	"distributed_kv_store/internal/cluster"
	"flag"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
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
		ClientCommandChan:         make(chan *cluster.Command),
		RequestVoteChan:           make(chan *cluster.RequestVoteRequestWrapper),
		RequestVoteResponseChan:   make(chan *cluster.RequestVoteResponse),
	}

	//err = node.LoadRaftState()
	//if err != nil {
	//	log.Fatalf("Error loading raft state: %v", err)
	//}
	//err = node.LoadRaftLog()

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
	go node.RunRaftLoop()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			key := r.URL.Path[1:]
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer r.Body.Close()

			node.RaftMu.Lock()
			leaderId := node.LeaderID
			node.RaftMu.Unlock()
			switch node.State {
			case cluster.Leader:
				putCmd := &cluster.Command{
					Type:  cluster.CommandPut,
					Key:   key,
					Value: body,
				}

				node.ClientCommandChan <- putCmd
				w.WriteHeader(http.StatusCreated)
				_, err := fmt.Fprintf(w, "Sent a PUT request for %s", key)
				if err != nil {
					log.Printf("Error writing response: %v", err)
				}
				return
			default:
				if leaderId == "" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				http.Redirect(w, r, fmt.Sprintf("http://%s:%v", node.Peers[leaderId].Address, node.Peers[leaderId].Port), http.StatusTemporaryRedirect)
				return
			}

		} else if r.Method == http.MethodGet {
			key := r.URL.Path[1:]

			node.RaftMu.Lock()
			defer node.RaftMu.Unlock()
			if _, ok := node.Data[key]; !ok {
				w.WriteHeader(http.StatusNotFound)
				_, err := fmt.Fprintf(w, "Key %s not found", key)
				if err != nil {
					return
				}
				return
			}
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(node.Data[key])
			if err != nil {
				fmt.Printf("Error writing response: %v", err)
				return
			}
		}
	})
	fmt.Printf("Listening on port %d\n", node.Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil))
}
