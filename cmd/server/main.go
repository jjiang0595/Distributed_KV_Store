package main

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"distributed_kv_store/internal/serverapp"
	"flag"
	"fmt"
	"github.com/jonboulle/clockwork"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var k = koanf.New(".")

type Config struct {
	Node struct {
		ID       string `mapstructure:"id" yaml:"id"`
		Address  string `mapstructure:"address" yaml:"address"`
		Port     int    `mapstructure:"http_port" yaml:"http_port"`
		GrpcPort int    `mapstructure:"grpc_port" yaml:"grpc_port"`
		DataDir  string `mapstructure:"data_dir" yaml:"data_dir"`
	} `mapstructure:"node" yaml:"node"`
	Cluster struct {
		Peers []*cluster.Node `mapstructure:"peers" yaml:"peers"`
	} `mapstructure:"cluster" yaml:"cluster"`
}

func LoadConfig(configPath string) (*Config, error) {
	if err := k.Load(file.Provider(configPath), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("failed to load config from %s: %w", configPath, err)
	}

	if err := k.Load(env.Provider(".", env.Opt{
		TransformFunc: func(k, v string) (string, any) {
			log.Printf("Koanf Transform: %s, %s", k, v)
			trimmedKey := strings.TrimPrefix(k, "RAFT_")
			parts := strings.Split(trimmedKey, "_")
			if parts[0] != "NODE" {
				return k, v
			}

			if parts[0] == "NODE" {
				if len(parts) > 1 {
					k = strings.ToLower(parts[0] + "." + strings.Join(parts[1:], "_"))
				} else {
					return "", fmt.Errorf("invalid format: %s", k)
				}
			}
			log.Printf("After Koanf Transform: %s, %s", k, v)
			return k, v
		},
	}), nil); err != nil {
		return nil, fmt.Errorf("failed to load env: %w", err)
	}

	var config Config
	if err := k.UnmarshalWithConf("", &config, koanf.UnmarshalConf{Tag: "mapstructure"}); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

func main() {
	time.Sleep(2 * time.Second)
	configPath := flag.String("config", "/etc/config.yaml", "Path to config file")
	flag.Parse()

	fmt.Printf("Starting server with configuration file: %s\n", *configPath)

	cfg, err := LoadConfig(*configPath)
	log.Printf("Loaded config: %+v\n", cfg)
	if err != nil {
		log.Fatal(err)
	}
	port := cfg.Node.Port
	gRpcPort := cfg.Node.GrpcPort

	peerHttpAddresses := make(map[string]string)
	peerAddresses := make(map[string]string)
	peerIDs := make([]string, 0)
	for _, peer := range cfg.Cluster.Peers {
		if peer.ID == cfg.Node.ID {
			continue
		}
		peerHttpAddresses[peer.ID] = fmt.Sprintf("%s:%d", peer.Address, peer.Port)
		peerIDs = append(peerIDs, peer.ID)
		peerAddresses[peer.ID] = fmt.Sprintf("%s:%d", peer.Address, peer.GrpcPort)
	}
	t, err := cluster.NewGRPCTransport(peerAddresses)
	if err != nil {
		log.Fatalf("Failed to create grpc transport: %s", err)
	}
	defer t.Close()
	log.Printf("PEER ADDRESSES: %v", peerAddresses)
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
