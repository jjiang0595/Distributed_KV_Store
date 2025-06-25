package cluster

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"runtime"
	"sync"
	"testing"
	"time"
)

func testSetup(t *testing.T) (map[string]*Node, *clockwork.FakeClock) {
	clk := clockwork.NewFakeClock()

	nodeIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		nodeIDs[i] = fmt.Sprintf("node%d", i)
	}

	tempNodes := make(map[string]*Node)
	for i := 0; i < 3; i++ {
		newNode := NewNode(nil, nil, nodeIDs[i], "localhost:", 0, 0, t.TempDir(), filterSelfID(nodeIDs[i], nodeIDs), clk, MockListenerFactory, nil)
		tempNodes[nodeIDs[i]] = newNode
	}

	testNodes := make(map[string]*Node)
	for i := 0; i < 3; i++ {
		nodeCtx, nodeCancel := context.WithCancel(context.Background())
		mockTransport := NewMockNetworkTransport(nodeCtx, nodeIDs[i], tempNodes)
		newNode := tempNodes[nodeIDs[i]]
		newNode.Transport = mockTransport
		testNodes[nodeIDs[i]] = newNode
		testNodes[nodeIDs[i]].ctx = nodeCtx
		testNodes[nodeIDs[i]].cancel = nodeCancel
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
	return testNodes, clk
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
		t.Fatalf("Error: Leader not found within 500 ms")
	}
	return leaderID
}
