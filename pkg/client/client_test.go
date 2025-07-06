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
	t.Logf("Success: Client PUT request replicated")
	cleanup(t, testNodes, httpServers)
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
