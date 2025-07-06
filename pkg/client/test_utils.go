package client

import (
	"bytes"
	"context"
	"distributed_kv_store/internal/cluster"
	"distributed_kv_store/internal/serverapp"
	"fmt"
	"github.com/jonboulle/clockwork"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type TestCluster struct {
	T             *testing.T
	Clock         *clockwork.FakeClock
	Client        *Client
	MockHTTPRT    *MockHTTPRoundTripper
	HttpServers   map[string]*serverapp.HTTPServer
	PeerHTTPAddrs map[string]string
	TestNodes     map[string]*cluster.Node
	KvStores      map[string]*cluster.KVStore
	MockTransport *cluster.MockNetworkTransport
}

func testSetup(t *testing.T) *TestCluster {
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

	mockHTTPRT := NewMockHTTPRoundTripper(clk)
	httpServers := make(map[string]*serverapp.HTTPServer, len(testNodes))
	for i := 0; i < len(testNodes); i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		node := testNodes[nodeID]
		httpServers[nodeID] = serverapp.NewHTTPServer(nodeID, node.GetKVStore(), node.ProposeCommand, node.GetLeaderInfo, peerHTTPAddresses, node.Port)
		mockHTTPRT.RegisterHandler(fmt.Sprintf("%s:%v", node.Address, node.Port), httpServers[nodeID].GetServer().Handler)
		httpServers[nodeID].Start()
	}

	c := NewClient(peerHTTPAddresses, WithClock(clk), WithHTTPTransport(mockHTTPRT))

	testNodesWg := sync.WaitGroup{}
	for _, node := range testNodes {
		testNodesWg.Add(1)
		go func(n *cluster.Node) {
			defer testNodesWg.Done()
			n.Start()
		}(node)
	}

	testNodesWg.Wait()
	return &TestCluster{
		T:             t,
		Client:        c,
		PeerHTTPAddrs: peerHTTPAddresses,
		MockHTTPRT:    mockHTTPRT,
		HttpServers:   httpServers,
		TestNodes:     testNodes,
		KvStores:      kvStores,
		MockTransport: mockTransport,
		Clock:         clk,
	}
}

type MockHTTPRoundTripper struct {
	mu       sync.RWMutex
	handlers map[string]http.Handler
	clk      *clockwork.FakeClock

	failCount             int
	currentFails          int
	simulatedHTTPStatus   int
	simulatedNetworkError error
	simulatedDelay        time.Duration
}

func NewMockHTTPRoundTripper(clock *clockwork.FakeClock) *MockHTTPRoundTripper {
	return &MockHTTPRoundTripper{
		clk:            clock,
		handlers:       make(map[string]http.Handler),
		simulatedDelay: 0 * time.Millisecond,
		failCount:      0,
		currentFails:   0,
	}
}

func (m *MockHTTPRoundTripper) RegisterHandler(address string, handler http.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[address] = handler
}

func (m *MockHTTPRoundTripper) DeregisterHandler(address string, handler http.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.handlers, address)
}

func (m *MockHTTPRoundTripper) SetDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.simulatedDelay = delay
}

func (m *MockHTTPRoundTripper) SetTransientError(failCount int, httpStatus int, networkError error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failCount = failCount
	m.simulatedHTTPStatus = httpStatus
	m.simulatedNetworkError = networkError
	m.currentFails = 0
}

func (m *MockHTTPRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.simulatedDelay > 0 && (req.Method == "PUT" || req.Method == "GET") && strings.HasPrefix(req.URL.Path, "/key/") {
		exitTicker := m.clk.NewTicker(m.simulatedDelay)
		defer exitTicker.Stop()
	DelayTest:
		for {
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-exitTicker.Chan():
				break DelayTest
			}
		}
		m.simulatedDelay = 0
	}

	m.mu.Lock()

	if m.currentFails < m.failCount && (req.Method == "PUT" || req.Method == "GET") && strings.HasPrefix(req.URL.Path, "/key/") {
		m.currentFails++
		if m.simulatedHTTPStatus != 0 {
			resp := &http.Response{
				StatusCode: m.simulatedHTTPStatus,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Simulated Network Error: %v", m.simulatedNetworkError))),
				Request:    req,
			}
			log.Printf("Simulated HTTP status code %v", m.simulatedHTTPStatus)
			m.mu.Unlock()
			return resp, nil
		}
		if m.simulatedNetworkError != nil {
			m.mu.Unlock()
			return nil, m.simulatedNetworkError
		}
	}
	m.mu.Unlock()

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
