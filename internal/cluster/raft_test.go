package cluster

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

var globalLogFile *os.File

func TestMain(m *testing.M) {
	logDir, err := os.MkdirTemp("./logs", "raft_logs")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(logDir); err != nil {
			log.Fatal(err)
		}
	}()

	logFilePath := filepath.Join(logDir, "test.log")
	globalLogFile, err = os.Create(logFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer globalLogFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, globalLogFile))
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)

	exitCode := m.Run()
	os.Exit(exitCode)
}

func testSetup(t *testing.T) (context.Context, context.CancelFunc, map[string]*Node, *clockwork.FakeClock) {
	ctx, cancel := context.WithCancel(context.Background())
	clk := clockwork.NewFakeClock()

	nodeIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		nodeIDs[i] = fmt.Sprintf("node%d", i)
	}

	tempNodes := make(map[string]*Node)
	for i := 0; i < 3; i++ {
		newNode := NewNode(ctx, cancel, nodeIDs[i], "localhost:", 0, 0, t.TempDir(), filterSelfID(nodeIDs[i], nodeIDs), clk, MockListenerFactory, nil)
		tempNodes[nodeIDs[i]] = newNode
	}

	testNodes := make(map[string]*Node)
	for i := 0; i < 3; i++ {
		mockTransport := NewMockNetworkTransport(ctx, nodeIDs[i], tempNodes)
		newNode := tempNodes[nodeIDs[i]]
		newNode.Transport = mockTransport
		testNodes[nodeIDs[i]] = newNode
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
	return ctx, cancel, testNodes, clk
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

func TestLeaderElection_SingleLeader(t *testing.T) {
	_, cancel, testNodes, clk := testSetup(t)
	defer cancel()

	clk.Advance(3 * time.Second)
	timeout := 500 * time.Millisecond
	startTime := time.Now()

	leaderCount := false
	for time.Since(startTime) < timeout {
		for _, node := range testNodes {
			if node.State == Leader {
				leaderCount = true
				break
			}
		}
		clk.Advance(3 * time.Millisecond)
		time.Sleep(3 * time.Millisecond)
	}

	if leaderCount {
		t.Logf("Success: Single Leader")
	} else {
		t.Fatalf("followerCount should be 2")
func TestLogReplication_LeaderCommand(t *testing.T) {
	testCommand := &Command{
		Type:  CommandPut,
		Key:   "testKey",
		Value: []byte("testValue"),
	}

	_, cancel, testNodes, clk := testSetup(t)
	defer cancel()

	var leaderNode *Node
	exitTicker := clk.NewTicker(10 * time.Second)
	checkTicker := clk.NewTicker(50 * time.Millisecond)
	defer checkTicker.Stop()

LeaderCheck:
	for {
		select {
		case <-exitTicker.Chan():
			t.Fatalf("Leader not found within 10 secs")
		case <-checkTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == Leader {
					leaderNode = node
					break LeaderCheck
				}
			}
		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if leaderNode != nil {
		select {
		case testNodes[leaderNode.ID].ClientCommandChan <- testCommand:
			t.Logf("Sent Client Command from test")
		case <-clk.After(3 * time.Second):
			t.Fatalf("Client Command not received within 3 seconds")
		}
	} else {
		t.Fatalf("Leader not found within time limit")
	}

	replicationTicker := clk.NewTicker(50 * time.Millisecond)
	defer replicationTicker.Stop()

	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-replicationTicker.Chan():
			replicated = true
			for _, node := range testNodes {
				if value, ok := node.GetData()[testCommand.Key]; !ok || !bytes.Equal(value, testCommand.Value) {
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

	if replicated {
		t.Logf("Success: Replicated leader command")
	} else {
		t.Fatalf("Error: Replicated leader command not replicated to all nodes. Replicated to %v nodes.", replicated)
	}

	t.Cleanup(func() {
		for _, node := range testNodes {
			node.cancel()
			go node.Shutdown()
		}
		clk.Advance(50 * time.Millisecond)
		for _, node := range testNodes {
			node.WaitAllGoroutines()
		}
	})

}
