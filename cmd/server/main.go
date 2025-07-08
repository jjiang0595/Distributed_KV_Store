package main

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"distributed_kv_store/internal/serverapp"
	"flag"
	"fmt"
	"github.com/jonboulle/clockwork"
	"gopkg.in/yaml.v3"
	"io"
	"log"
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
	serverAddr := serverapp.NewHTTPServer(node, node.ProposeCommand, node.GetLeaderInfo, peerHttpAddresses, port)
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
