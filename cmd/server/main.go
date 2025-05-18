package main

import (
	"distributed_kv_store/internal/cluster"
	"distributed_kv_store/internal/cluster/types"
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
	"time"
)

type Config struct {
	Node struct {
		ID       string `yaml:"id"`
		Address  string `yaml:"address"`
		Port     string `yaml:"port"`
		GrpcPort string `yaml:"grpc_port"`
	} `yaml:"node"`
	Cluster struct {
		Peers []types.Node `yaml:"peers"`
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

	node := &types.Node{
		ID:       cfg.Node.ID,
		Address:  cfg.Node.Address,
		Port:     port,
		GrpcPort: gRpcPort,
		Data:     make(map[string][]byte),
	}

	var peers types.NodeMap
	peers.Nodes = make([]types.Node, 0, len(cfg.Cluster.Peers))
	for _, peer := range cfg.Cluster.Peers {
		peers.Nodes = append(peers.Nodes, types.Node{
			ID:       peer.ID,
			Address:  peer.Address,
			Port:     peer.Port,
			GrpcPort: peer.GrpcPort,
		})
	}

	grpcServer := grpc.NewServer()
	replicationServer := cluster.NewReplicationServer(node)
	cluster.RegisterReplicationServiceServer(grpcServer, replicationServer)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", node.GrpcPort))
	if err != nil {
		log.Fatalf("Error listening on port %d: %v", node.GrpcPort, err)
	}

	go func() {
		log.Printf("Listening on port %d", node.GrpcPort)
		serveErr := grpcServer.Serve(listener)
		if serveErr != nil {
			log.Fatalf("Error serving on port %d: %v", node.GrpcPort, serveErr)
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			key := r.URL.Path[1:]
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer r.Body.Close()
			node.Put(key, body)
			for _, peer := range peers.Nodes {
				log.Printf("%s:%d", peer.Address, peer.GrpcPort)
				conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", peer.Address, peer.GrpcPort),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if err != nil {
					log.Printf("Error connecting to peer %s:%d %v", peer.Address, peer.Port, err)
					if conn != nil {
						conn.Close()
					}
					continue
				}
				defer conn.Close()
				peerClient := cluster.NewReplicationServiceClient(conn)

				req := &cluster.ReplicationRequest{
					Key:   key,
					Value: body,
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				resp, err := peerClient.Replicate(ctx, req)
				if err != nil {
					log.Printf("Error replicating: %s:%d: %v", peer.Address, peer.Port, err)
				} else {
					log.Printf("Replicated to %s:%d. Success: %v", peer.Address, peer.Port, resp.GetSuccess())
				}

				err = conn.Close()
				if err != nil {
					log.Printf("Error closing connection to peer %s:%d: %v", peer.Address, peer.Port, err)
				}
				cancel()
			}
			w.WriteHeader(http.StatusOK)
			_, printErr := fmt.Fprintf(w, "PUT successful for key: %s\n", key)
			if printErr != nil {
				log.Printf("Error writing response: %s\n", printErr)
			}
		} else if r.Method == http.MethodGet {
			key := r.URL.Path[1:]
			value, err := node.Get(key)
			if err != nil {
				http.Error(w, "Key not found", http.StatusNotFound)
				return
			}
			_, writeErr := w.Write(value)
			if writeErr != nil {
				log.Printf("Error writing response: %s\n", writeErr)
				return
			}
			fmt.Printf("GET successful for key: %s\n", key)
		}
	})
	fmt.Printf("Listening on port %d\n", node.Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil))
}
