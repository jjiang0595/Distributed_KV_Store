package client

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"distributed_kv_store/internal/serverapp"
	"fmt"
	"github.com/jonboulle/clockwork"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func testSetup(t *testing.T) (map[string]string, map[string]*cluster.Node, map[string]*cluster.KVStore, *cluster.MockNetworkTransport, *clockwork.FakeClock) {
	clk := clockwork.NewFakeClock()
	mockTransport := cluster.NewMockNetworkTransport(context.Background())
	kvStores := make(map[string]*cluster.KVStore)
	testDir := t.TempDir()

	peerHTTPAddresses := make(map[string]string)
	nodeIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		nodeIDs[i] = fmt.Sprintf("node%d", i+1)
		peerHTTPAddresses[nodeIDs[i]] = fmt.Sprintf("127.0.0.1:808%d", i)
	}

	tempNodes := make(map[string]*cluster.Node)
	for i := 0; i < 3; i++ {
		nodeCtx, nodeCancel := context.WithCancel(context.Background())
		nodeDataDir := filepath.Join(testDir, nodeIDs[i])
		newNode := cluster.NewNode(nodeCtx, nodeCancel, nodeIDs[i], "127.0.0.1", 8080+i, 0, nodeDataDir, filterSelfID(nodeIDs[i], nodeIDs), clk, cluster.MockListenerFactory, mockTransport)
		tempNodes[nodeIDs[i]] = newNode
		mockTransport.RegisterRPCServer(newNode, newNode.GetRPCServer())
	}

	testNodes := make(map[string]*cluster.Node)
	for i := 0; i < 3; i++ {
		newNode := tempNodes[nodeIDs[i]]
		testNodes[nodeIDs[i]] = newNode
		kvStores[nodeIDs[i]] = newNode.GetKVStore()
	}

	testNodesWg := sync.WaitGroup{}
	for _, node := range testNodes {
		testNodesWg.Add(1)
		go func(n *cluster.Node) {
			defer testNodesWg.Done()
			n.Start()
		}(node)
	}

	testNodesWg.Wait()
	return peerHTTPAddresses, testNodes, kvStores, mockTransport, clk
}

type MockHTTPRoundTripper struct {
	mu       sync.RWMutex
	handlers map[string]http.Handler
}

func NewMockHTTPRoundTripper() *MockHTTPRoundTripper {
	return &MockHTTPRoundTripper{
		handlers: make(map[string]http.Handler),
	}
}

func (m *MockHTTPRoundTripper) RegisterHandler(address string, handler http.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[address] = handler
}

func (m *MockHTTPRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	handler, ok := m.handlers[req.URL.Host]
	if !ok {
		return nil, fmt.Errorf("no handler for %s", req.URL.Path)
	}

	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	resp := &http.Response{
		StatusCode: recorder.Code,
		Header:     recorder.Header(),
		Body:       io.NopCloser(recorder.Body),
		Request:    req,
	}

	return resp, nil
}

func cleanup(t *testing.T, testNodes map[string]*cluster.Node, httpServers map[string]*serverapp.HTTPServer) {
	t.Cleanup(func() {
		for _, node := range testNodes {
			go node.Shutdown()
		}
		for _, httpServer := range httpServers {
			httpServer.Stop()
		}
		for _, node := range testNodes {
			node.WaitAllGoroutines()
		}

	})
}

func findLeader(t *testing.T, clk *clockwork.FakeClock, testNodes map[string]*cluster.Node, checkTicker clockwork.Ticker, exitTicker clockwork.Ticker) string {
	leaderFound := false
	leaderID := ""

LeaderCheck:
	for {
		select {
		case <-checkTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == cluster.Leader {
					leaderFound = true
					leaderID = node.ID
					break LeaderCheck
				}
			}

		case <-exitTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == cluster.Leader {
					leaderFound = true
				}
			}
			break LeaderCheck

		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if !leaderFound {
		t.Fatalf("Error: Leader not found within time limit")
	}
	return leaderID
}

func filterSelfID(nodeID string, nodes []string) []string {
	filteredList := make([]string, 0)
	for _, id := range nodes {
		if nodeID != id {
			filteredList = append(filteredList, id)
		}
	}
	return filteredList
}
