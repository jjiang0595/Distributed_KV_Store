package main

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"distributed_kv_store/pkg/client"
	"flag"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"net/http"
	"os"
	"time"
)

type Config struct {
	Node struct {
		ID       string `mapstructure:"id" yaml:"id"`
		Address  string `mapstructure:"address" yaml:"address"`
		Port     string `mapstructure:"port" yaml:"port"`
		GrpcPort string `mapstructure:"grpc_port" yaml:"grpc_port"`
		DataDir  string `mapstructure:"data_dir" yaml:"data_dir"`
	} `mapstructure:"node" yaml:"node"`
	Cluster struct {
		Peers []*cluster.Node `mapstructure:"peers" yaml:"peers"`
	} `mapstructure:"cluster" yaml:"cluster"`
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
	configPath := flag.String("config", "config-node1.yaml", "path to config file")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		panic(err)
	}
	addresses := make(map[string]string)
	address := fmt.Sprintf("%v:%v", cfg.Node.Address, cfg.Node.Port)
	addresses[cfg.Node.ID] = address
	for i := 0; i < len(cfg.Cluster.Peers); i++ {
		peerAddress := fmt.Sprintf("%v:%v", cfg.Node.Address, cfg.Node.Port)
		addresses[cfg.Cluster.Peers[i].ID] = peerAddress
	}

	c := client.NewClient(addresses, client.WithTimeout(time.Second*5), client.WithMaxRetries(5), client.WithHTTPTransport(&http.Transport{
		MaxIdleConns:    5,
		IdleConnTimeout: time.Second * 30,
	}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	err = c.PUT(ctx, "key", "value")
}
