package main

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"flag"
	"fmt"
	"github.com/jonboulle/clockwork"
	"gopkg.in/yaml.v3"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
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
	fmt.Printf("Starting server with configuration file: %s\n", *configFile)

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

	peerAddresses := make(map[string]string)
	peerIDs := make([]string, 0)
	for _, peer := range cfg.Cluster.Peers {
		if peer.ID == cfg.Node.ID {
			continue
		}
		peerIDs = append(peerIDs, peer.ID)
		peerAddresses[peer.ID] = fmt.Sprintf("%s:%d", peer.Address, peer.GrpcPort)
	}

	t, err := cluster.NewGRPCTransport(peerAddresses)
	if err != nil {
		log.Fatalf("Failed to create grpc transport: %s", err)
	}
	defer t.Close()

	clk := clockwork.NewRealClock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := cluster.NewNode(ctx, cancel, cfg.Node.ID, cfg.Node.Address, port, gRpcPort, cfg.Node.DataDir, peerIDs, clk, t)
	node.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

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
			leaderId := node.GetLeaderID()
			node.RaftMu.Unlock()
			switch node.GetState() {
			case cluster.Leader:
				putCmd := &cluster.Command{
					Type:  cluster.CommandPut,
					Key:   key,
					Value: body,
				}
				log.Printf("Sending to clientCommandChan %v...", node.ClientCommandChan)
				node.ClientCommandChan <- putCmd
				w.WriteHeader(http.StatusCreated)
				_, err := fmt.Fprintf(w, "Sent a PUT request for %s", key)
				if err != nil {
					log.Printf("Error writing response: %v", err)
				}
			default:
				if leaderId == "" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				http.Redirect(w, r, fmt.Sprintf("http://%s/%s", peerAddresses[leaderId], key), http.StatusTemporaryRedirect)
				return
			}

		} else if r.Method == http.MethodGet {
			key := r.URL.Path[1:]

			node.RaftMu.Lock()
			defer node.RaftMu.Unlock()
			if _, ok := node.GetData()[key]; !ok {
				w.WriteHeader(http.StatusNotFound)
				_, err := fmt.Fprintf(w, "Key %s not found", key)
				if err != nil {
					return
				}
				return
			}
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(node.GetData()[key])
			if err != nil {
				fmt.Printf("Error writing response: %v", err)
				return
			}
		}
	})

	go func() {
		log.Printf("Listening on port %d", node.Port)
		if http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil) != nil {
			log.Fatalf("Error listening on port %d: %v", node.Port, err)
		}
		log.Printf("HTTP Server Goroutine Finished")
	}()

	sig := <-sigChan
	log.Printf("Received signal: %v", sig)

	node.Shutdown()
	node.WaitAllGoroutines()
	os.Exit(0)
}
