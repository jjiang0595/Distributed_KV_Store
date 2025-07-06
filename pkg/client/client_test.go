package client

import (
	"context"
	"github.com/jonboulle/clockwork"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestClient_PUT(t *testing.T) {
	peerHTTPAddresses, testNodes, kvStores, _, clk := testSetup(t)

	mockHTTPRT := NewMockHTTPRoundTripper()
	httpServers := make(map[string]*serverapp.HTTPServer, len(testNodes))
	for i := 0; i < len(testNodes); i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		node := testNodes[nodeID]
		httpServers[nodeID] = serverapp.NewHTTPServer(nodeID, node.GetKVStore(), node.ProposeCommand, node.GetLeaderInfo, peerHTTPAddresses, node.Port)
		mockHTTPRT.RegisterHandler(fmt.Sprintf("%s:%v", node.Address, node.Port), httpServers[nodeID].GetServer().Handler)
		httpServers[nodeID].Start()
func TestMain(m *testing.M) {
	logDir, err := os.MkdirTemp("./logs", "client_logs")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(logDir); err != nil {
			log.Fatal(err)
		}
	}()

	logFilePath := filepath.Join(logDir, "test.log")
	globalLogFile, err := os.Create(logFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer globalLogFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, globalLogFile))
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)

	exitCode := m.Run()
	os.Exit(exitCode)
}

	c.leaderAddress.Store(peerHTTPAddresses[leaderID])
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
func TestClient_Put_LeaderDiscovery(t *testing.T) {
	test := testSetup(t)

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	setupLeader(t, test.Clock, test.TestNodes, checkTicker, exitTicker)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()
	testKey := "testKey"
	testValue := "testValue"
	err := test.Client.PUT(ctx, testKey, testValue)
	if err != nil {
		t.Errorf("Error putting key: %v", err)
		return
	}

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for nodeID, _ := range test.TestNodes {
				if value, ok := test.KvStores[nodeID].Get(testKey); !ok || value != testValue {
					replicated = false
					break
				}
			}
			if replicated {
				break ReplicationCheck
			}
		case <-exitTicker.Chan():
			t.Fatalf("Logs not replicated within 10 secs")
		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}
	t.Logf("Success: Client PUT request replicated")
	test.cleanup()
}

func TestClient_GET(t *testing.T) {
	peerHTTPAddresses, testNodes, kvStores, _, clk := testSetup(t)

	mockHTTPRT := NewMockHTTPRoundTripper()
	httpServers := make(map[string]*serverapp.HTTPServer, len(testNodes))
	for i := 0; i < len(testNodes); i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		node := testNodes[nodeID]
		httpServers[nodeID] = serverapp.NewHTTPServer(nodeID, node.GetKVStore(), node.ProposeCommand, node.GetLeaderInfo, peerHTTPAddresses, node.Port)
		mockHTTPRT.RegisterHandler(fmt.Sprintf("%s:%v", node.Address, node.Port), httpServers[nodeID].GetServer().Handler)
		httpServers[nodeID].Start()
func TestClient_Put_FollowerRedirects(t *testing.T) {
	test := testSetup(t)

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	followerID, leaderID := setupLeader(t, test.Clock, test.TestNodes, checkTicker, exitTicker)
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	waitForLeader(t, test.Clock, leaderID, test.TestNodes, checkTicker, exitTicker)

	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[followerID])

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()
	testKey := "testKey"
	testValue := "testValue"
	err := test.Client.PUT(ctx, testKey, testValue)
	if err != nil {
		t.Errorf("Error putting key: %v", err)
		return
	}

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for nodeID, _ := range test.TestNodes {
				t.Logf("%s: %v", nodeID, test.KvStores[nodeID].GetData())
				if value, ok := test.KvStores[nodeID].Get(testKey); !ok || value != testValue {
					replicated = false
					break
				}
			}
			if replicated {
				break ReplicationCheck
			}
		case <-exitTicker.Chan():
			t.Fatalf("Logs not replicated within 10 secs")
		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}
	t.Logf("Success: Successfully redirected & client PUT request replicated")
	test.cleanup()
}

func TestClient_Put_TransientError(t *testing.T) {
	test := testSetup(t)

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := setupLeader(t, test.Clock, test.TestNodes, checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderID])

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	waitForLeader(t, test.Clock, leaderID, test.TestNodes, checkTicker, exitTicker)

	test.MockHTTPRT.SetTransientError(1, http.StatusServiceUnavailable, nil)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()
	testKey := "testKey"
	testValue := "testValue"
	err := test.Client.PUT(ctx, testKey, testValue)
	if err != nil {
		t.Errorf("Error putting key: %v", err)
		return
	}
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for nodeID, _ := range test.TestNodes {
				if value, ok := test.KvStores[nodeID].Get(testKey); !ok || value != testValue {
					replicated = false
					break
				}
			}
			if replicated {
				break ReplicationCheck
			}
		case <-exitTicker.Chan():
			t.Fatalf("Logs not replicated within 10 secs")
		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}
	t.Logf("Success: Handled errors & Client PUT request replicated")
	test.cleanup()
}

func TestClient_Put_MaxRetries(t *testing.T) {
	test := testSetup(t)

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := setupLeader(t, test.Clock, test.TestNodes, checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderID])

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	waitForLeader(t, test.Clock, leaderID, test.TestNodes, checkTicker, exitTicker)

	test.MockHTTPRT.SetTransientError(3, http.StatusServiceUnavailable, nil)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 5*time.Second)
	defer cancel()
	err := test.Client.PUT(ctx, "testKey", "testValue")
	test.cleanup()
	if err != nil {
		if strings.Contains(err.Error(), "Service Unavailable") {
			t.Logf("Success: Graceful completion of maximum retries")
			return
		}
		t.Errorf("Error putting key: %v", err)
		return
	}
}

	}

	c := NewClient(peerHTTPAddresses, WithTimeout(5*time.Second), WithMaxRetries(3), WithHTTPTransport(mockHTTPRT))

	checkTicker := clk.NewTicker(50 * time.Millisecond)
	exitTicker := clk.NewTicker(1 * time.Second)
	leaderID := findLeader(t, clk, testNodes, checkTicker, exitTicker)

	c.leaderAddress.Store(peerHTTPAddresses[leaderID])
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	testKey := "testKey"
	testValue := "testValue"
	err := c.PUT(ctx, testKey, testValue)
	if err != nil {
		t.Fatalf("Error putting key: %v", err)
	}

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for nodeID, _ := range testNodes {
				t.Logf("%s: %v", nodeID, kvStores[nodeID].GetData())
				if value, ok := kvStores[nodeID].Get(testKey); !ok || value != testValue {
					replicated = false
					break
				}
			}
			if replicated {
				break ReplicationCheck
			}
		case <-exitTicker.Chan():
			t.Fatalf("Logs not replicated within 10 secs")
		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	val, err := c.GET(ctx, testKey)
	if err != nil {
		t.Fatalf("Error getting key: %v", err)
	}
	clk.Advance(50 * time.Millisecond)
	runtime.Gosched()

	log.Printf("%v", val)
	if val != testValue {
		t.Fatalf("Error: Invalid value. Expected %v, got %v", len(testValue), len(val))
	}

	t.Logf("Success: Processed Client GET Request")
	cleanup(t, testNodes, httpServers)
}
