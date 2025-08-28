package cluster

import (
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"google.golang.org/protobuf/proto"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

type TestCluster struct {
	T             *testing.T
	Clock         *clockwork.FakeClock
	TestNodes     map[string]*Node
	KvStores      map[string]*KVStore
	MockTransport *MockNetworkTransport
}

func testSetup(t *testing.T) *TestCluster {
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

	go func() {
		for {
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}()

	return &TestCluster{T: t, Clock: clk, TestNodes: testNodes, KvStores: kvStores, MockTransport: mockTransport}
}

func (test *TestCluster) cleanup() {
	test.T.Cleanup(func() {
		for _, node := range test.TestNodes {
			go node.Shutdown()
		}
		for nodeID, node := range test.TestNodes {
			test.KvStores[nodeID] = nil
			test.MockTransport.UnregisterRPCServer(node)
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
	if !reflect.DeepEqual(log1.Command, log2.Command) {
		return false
	}
	return true
}

func (test *TestCluster) findLeader(testNodes map[string]*Node, checkTicker clockwork.Ticker, exitTicker clockwork.Ticker) string {
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
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if !leaderFound {
		test.T.Fatalf("Error: Leader not found within time limit")
	}
	return leaderID
}

func (test *TestCluster) crashAndRecoverNode(followerID string) {
	testNodes := test.TestNodes
	transport := test.MockTransport
	kvStores := test.KvStores

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
		test.Clock,
		deletedNode.listenerFactory,
		transport,
	)
	transport.RegisterRPCServer(testNodes[followerID], testNodes[followerID].raftServer)
	kvStores[followerID] = testNodes[followerID].kvStore
	testNodes[followerID].Start()
}

func (test *TestCluster) crashNode(followerID string) *Node {
	testNodes := test.TestNodes

	deletedNode := testNodes[followerID]
	testNodes[followerID].cancel()
	go testNodes[followerID].Shutdown()
	test.MockTransport.UnregisterRPCServer(deletedNode)
	deletedNode.WaitAllGoroutines()
	delete(test.KvStores, followerID)

	return deletedNode
}

func (test *TestCluster) recoverNode(deletedNode *Node, followerID string) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	testNodes := test.TestNodes

	testNodes[followerID] = NewNode(
		ctx,
		cancel,
		deletedNode.ID,
		deletedNode.Address,
		deletedNode.Port,
		deletedNode.GrpcPort,
		deletedNode.dataDir,
		deletedNode.peers,
		test.Clock,
		deletedNode.listenerFactory,
		test.MockTransport,
	)
	test.MockTransport.RegisterRPCServer(testNodes[followerID], testNodes[followerID].raftServer)
	test.KvStores[followerID] = testNodes[followerID].kvStore
	testNodes[followerID].Start()

	return testNodes[followerID]
}

func (test *TestCluster) generateReview(recipeId string, title string, stars float32, body string) []byte {
	review := &AddReviewRequest{
		RecipeId: recipeId,
		Title:    title,
		Stars:    stars,
		Body:     body,
	}
	reviewBytes, err := proto.Marshal(review)
	if err != nil {
		return nil
	}
	return reviewBytes
}
