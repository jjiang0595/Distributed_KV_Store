package main

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"distributed_kv_store/internal/serverapp"
	"flag"
	"fmt"
	"github.com/jonboulle/clockwork"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Config struct {
	Node struct {
		ID       string `mapstructure:"id" yaml:"id"`
		Address  string `mapstructure:"address" yaml:"address"`
		Port     int    `mapstructure:"port" yaml:"port"`
		GrpcPort int    `mapstructure:"grpc_port" yaml:"grpc_port"`
		DataDir  string `mapstructure:"data_dir" yaml:"data_dir"`
	} `mapstructure:"node" yaml:"node"`
	Cluster struct {
		Peers []*cluster.Node `mapstructure:"peers" yaml:"peers"`
	} `mapstructure:"cluster" yaml:"cluster"`
}

func LoadConfig(configName string) (*Config, error) {
	v := viper.New()

	v.SetConfigFile(configName)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file, %s", err)
	}

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

func main() {
	configFile := flag.String("config", "config-node1.yaml", "Config file name")
	flag.Parse()

	fmt.Printf("Starting server with configuration file: %s\n", *configFile)

	cfg, err := LoadConfig(*configFile)
	if err != nil {
		log.Fatal(err)
	}
	port := cfg.Node.Port
	gRpcPort := cfg.Node.GrpcPort
	log.Printf("port: %v\n", cfg.Node.DataDir)
	peerHttpAddresses := make(map[string]string)
	peerAddresses := make(map[string]string)
	peerIDs := make([]string, 0)
	for _, peer := range cfg.Cluster.Peers {
		peerHttpAddresses[peer.ID] = fmt.Sprintf("%s:%d", peer.Address, peer.Port)
		if peer.ID == cfg.Node.ID {
			continue
		}
		peerIDs = append(peerIDs, peer.ID)
		log.Printf("%v", cfg.Cluster.Peers[0])
		peerAddresses[peer.ID] = fmt.Sprintf("%s:%d", peer.Address, peer.GrpcPort)
	}
	log.Printf("Peer http addresses: %v", peerAddresses)
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
