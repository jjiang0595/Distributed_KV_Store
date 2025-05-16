package main

import (
	"distributed_kv_store/internal/cluster/types"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

type Config struct {
	Node struct {
		ID      string `yaml:"id"`
		Address string `yaml:"address"`
		Port    string `yaml:"port"`
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
	cfg, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	port, err := strconv.Atoi(cfg.Node.Port)
	if err != nil {
		log.Fatal(err)
	}
	node := &types.Node{
		ID:      cfg.Node.ID,
		Address: cfg.Node.Address,
		Port:    port,
		Data:    make(map[string][]byte),
	}

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
				return
			}
			fmt.Printf("GET successful for key: %s\n", key)
		}
	})
	fmt.Printf("Listening on port %d\n", node.Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil))
}
