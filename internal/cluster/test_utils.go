package cluster

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func testSetup(t *testing.T) (map[string]*Node, map[string]*KVStore, *MockNetworkTransport, *clockwork.FakeClock) {
	clk := clockwork.NewFakeClock()
	mockTransport := NewMockNetworkTransport(context.Background())
	kvStores := make(map[string]*KVStore)
	testDir := t.TempDir()

	nodeIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		nodeIDs[i] = fmt.Sprintf("node%d", i)
	}

	tempNodes := make(map[string]*Node)
	for i := 0; i < 3; i++ {
		nodeCtx, nodeCancel := context.WithCancel(context.Background())
		nodeDataDir := filepath.Join(testDir, nodeIDs[i])
		newNode := NewNode(nodeCtx, nodeCancel, nodeIDs[i], "localhost:", 0, 0, nodeDataDir, filterSelfID(nodeIDs[i], nodeIDs), clk, MockListenerFactory, mockTransport)
		tempNodes[nodeIDs[i]] = newNode
	}

	testNodes := make(map[string]*Node)
	for i := 0; i < 3; i++ {
		newNode := tempNodes[nodeIDs[i]]
		mockTransport.RegisterRPCServer(newNode, newNode.raftServer)
		testNodes[nodeIDs[i]] = newNode
		kvStore := NewKVStore()
		kvStores[nodeIDs[i]] = kvStore
	}
	testNodesWg := sync.WaitGroup{}
	for _, node := range testNodes {
		testNodesWg.Add(1)
		go func(n *Node) {
			defer testNodesWg.Done()
			n.Start()
		}(node)
	}

	testNodesWg.Wait()
	return testNodes, kvStores, mockTransport, clk
}

func cleanup(t *testing.T, testNodes map[string]*Node) {
	t.Cleanup(func() {
		for _, node := range testNodes {
			go node.Shutdown()
		}
		for _, node := range testNodes {
			node.WaitAllGoroutines()
		}
	})
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

func compareLogs(log1 *LogEntry, log2 *LogEntry) bool {
	if log1.Term != log2.Term || log1.Index != log2.Index {
		return false
	}
	if !bytes.Equal(log1.Command, log2.Command) {
		return false
	}
	return true
}

func findLeader(t *testing.T, clk *clockwork.FakeClock, testNodes map[string]*Node, checkTicker clockwork.Ticker, exitTicker clockwork.Ticker) string {
	leaderFound := false
	leaderID := ""

LeaderCheck:
	for {
		select {
		case <-checkTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == Leader {
					leaderFound = true
					leaderID = node.ID
					break LeaderCheck
				}
			}

		case <-exitTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == Leader {
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

func crashAndRecoverNode(followerID string, testNodes map[string]*Node, kvStores map[string]*KVStore, transport *MockNetworkTransport, clk *clockwork.FakeClock) {
	ctx, cancel := context.WithCancel(context.Background())

	deletedNode := testNodes[followerID]
	testNodes[followerID].cancel()
	go testNodes[followerID].Shutdown()
	transport.UnregisterRPCServer(deletedNode)
	deletedNode.WaitAllGoroutines()
	delete(kvStores, followerID)
	testNodes[followerID] = NewNode(
		ctx,
		cancel,
		deletedNode.ID,
		deletedNode.Address,
		deletedNode.Port,
		deletedNode.GrpcPort,
		deletedNode.dataDir,
		deletedNode.peers,
		clk,
		deletedNode.listenerFactory,
		transport,
	)
	transport.RegisterRPCServer(testNodes[followerID], testNodes[followerID].raftServer)
	kvStores[followerID] = testNodes[followerID].kvStore
	testNodes[followerID].Start()
}

func crashNode(followerID string, testNodes map[string]*Node, kvStores map[string]*KVStore, transport *MockNetworkTransport) *Node {
	deletedNode := testNodes[followerID]
	testNodes[followerID].cancel()
	go testNodes[followerID].Shutdown()
	transport.UnregisterRPCServer(deletedNode)
	deletedNode.WaitAllGoroutines()
	delete(kvStores, followerID)

	return deletedNode
}

func recoverNode(deletedNode *Node, followerID string, testNodes map[string]*Node, kvStores map[string]*KVStore, transport *MockNetworkTransport, clk *clockwork.FakeClock) *Node {
	ctx, cancel := context.WithCancel(context.Background())

	testNodes[followerID] = NewNode(
		ctx,
		cancel,
		deletedNode.ID,
		deletedNode.Address,
		deletedNode.Port,
		deletedNode.GrpcPort,
		deletedNode.dataDir,
		deletedNode.peers,
		clk,
		deletedNode.listenerFactory,
		transport,
	)
	transport.RegisterRPCServer(testNodes[followerID], testNodes[followerID].raftServer)
	kvStores[followerID] = testNodes[followerID].kvStore
	testNodes[followerID].Start()

	return testNodes[followerID]
}
