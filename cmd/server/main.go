package main

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"encoding/json"
	"errors"
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
	"strings"
	"syscall"
	"time"
)

type HTTPServer struct {
	nodeID            string
	kvStore           *cluster.KVStore
	proposeCommand    func(cmd []byte) error
	server            *http.Server
	port              int
	peerHTTPAddresses map[string]string
}

func NewHTTPServer(nodeID string, kvStore *cluster.KVStore, proposeCmd func(cmd []byte) error, peerHTTPAddresses map[string]string, port int) *HTTPServer {
	return &HTTPServer{
		nodeID:            nodeID,
		kvStore:           kvStore,
		proposeCommand:    proposeCmd,
		port:              port,
		peerHTTPAddresses: peerHTTPAddresses,
	}
}

func (s *HTTPServer) Start() {
	mux := http.NewServeMux()
	mux.HandleFunc("/key/", s.handleKeyRequest)
	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("%s HTTP Server Failed: %v", s.nodeID, err)
		}
	}()
}

func (s *HTTPServer) Stop() {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.server.Shutdown(ctx); err != nil {
			log.Printf("%s HTTP Server Shutdown Failed: %v", s.nodeID, err)
		}
	}
}

func (s *HTTPServer) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/key/"):]

	if key == "" {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case "PUT":
		s.handlePutRequest(w, r, key)
	default:
		s.handleGetRequest(w, r, key)
	}
}

func (s *HTTPServer) handlePutRequest(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var value cluster.PutRequest
	if err := json.Unmarshal(body, &value); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	putCmd := &cluster.Command{
		Type:  cluster.CommandPut,
		Key:   key,
		Value: value.Value,
	}
	var cmdToBytes []byte
	cmdToBytes, err = json.Marshal(putCmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = s.proposeCommand(cmdToBytes)
	if err != nil {
		if strings.Contains(err.Error(), "not leader, current leader is ") {
			leaderID := strings.TrimPrefix(err.Error(), "not leader, current leader is ")
			http.Redirect(w, r, fmt.Sprintf("http://%s/%s", s.peerHTTPAddresses[leaderID], key), http.StatusTemporaryRedirect)
		} else if strings.Contains(err.Error(), "timed out") {
			http.Error(w, fmt.Sprintf("Client request timed out: %v", err), http.StatusRequestTimeout)
		} else {
			http.Error(w, fmt.Sprintf("Internal server error: %v", err), http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(fmt.Sprintf("Successfully put %s %s", key, string(body))))
	if err != nil {
		log.Printf("error logging PUT success message")
	}
}

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

	peerHttpAddresses := make(map[string]string)
	peerAddresses := make(map[string]string)
	peerIDs := make([]string, 0)
	for _, peer := range cfg.Cluster.Peers {
		peerHttpAddresses[peer.ID] = fmt.Sprintf("%s:%d", peer.Address, peer.Port)
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
	node := cluster.NewNode(ctx, cancel, cfg.Node.ID, cfg.Node.Address, port, gRpcPort, cfg.Node.DataDir, peerIDs, clk, cluster.ProdListenerFactory, t)
	serverAddr := NewHTTPServer(node.ID, node.GetKVStore(), node.ProposeCommand, peerHttpAddresses, port)
	node.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	serverAddr.Start()

	sig := <-sigChan
	log.Printf("Received signal: %v", sig)

	go node.Shutdown()
	node.WaitAllGoroutines()
	os.Exit(0)
}
