package serverapp

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type HTTPServer struct {
	nodeID            string
	kvStore           *cluster.KVStore
	proposeCommand    func(cmd []byte) error
	getLeaderInfo     func() (string, string, bool)
	server            *http.Server
	port              int
	peerHTTPAddresses map[string]string
	mu                sync.RWMutex
}

type LeaderInfo struct {
	NodeID   string `json:"node_id"`
	LeaderID string `json:"leader_id"`
	IsLead   bool   `json:"is_leader"`
}

func (s *HTTPServer) GetServer() *http.Server {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.server
}

func NewHTTPServer(nodeID string, kvStore *cluster.KVStore, proposeCmd func(cmd []byte) error, getLeaderInfo func() (string, string, bool), peerHTTPAddresses map[string]string, port int) *HTTPServer {
	mux := http.NewServeMux()
	s := &HTTPServer{
		nodeID:            nodeID,
		kvStore:           kvStore,
		proposeCommand:    proposeCmd,
		getLeaderInfo:     getLeaderInfo,
		server:            &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux},
		peerHTTPAddresses: peerHTTPAddresses,
		port:              port,
	}
	mux.HandleFunc("/key/", s.handleKeyRequest)
	mux.HandleFunc("/status", s.handleStatusRequest)
	return s
}

func (s *HTTPServer) Start() {
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

func (s *HTTPServer) handleStatusRequest(w http.ResponseWriter, r *http.Request) {
	nodeId, leaderID, isLead := s.getLeaderInfo()
	leaderInfo := &LeaderInfo{
		NodeID:   nodeId,
		LeaderID: leaderID,
		IsLead:   isLead,
	}

	var infoToBytes []byte
	infoToBytes, err := json.Marshal(leaderInfo)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err = w.Write(infoToBytes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(infoToBytes)
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
			http.Redirect(w, r, fmt.Sprintf("http://%s/key/%s", s.peerHTTPAddresses[leaderID], key), http.StatusTemporaryRedirect)
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

func (s *HTTPServer) handleGetRequest(w http.ResponseWriter, r *http.Request, key string) {
	value, found := s.kvStore.Get(key)
	if !found {
		http.Error(w, "value not found", http.StatusNotFound)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	_, err = w.Write(jsonBytes)
	if err != nil {
		_ = fmt.Errorf("error writing response: %v", err)
		return
	}
	log.Printf("Sent a GET request for key %s", key)
}
