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

func TestLeaderElection_FindLeader(t *testing.T) {
	testNodes, clk := testSetup(t)

	exitTicker := clk.NewTicker(500 * time.Millisecond)
	checkTicker := clk.NewTicker(25 * time.Millisecond)

	leaderFound := false
LeaderCheck:
	for {
		select {
		case <-checkTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == Leader {
					leaderFound = true
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

	if leaderFound {
		t.Logf("Success: Single Leader")
	} else {
		t.Fatalf("Error: Leader not found within 500 ms")
	}

	t.Cleanup(func() {
		for _, node := range testNodes {
			go node.Shutdown()
		}
		for _, node := range testNodes {
			node.WaitAllGoroutines()
		}
	})
}

func TestLeaderElection_LeaderStability(t *testing.T) {
	testNodes, clk := testSetup(t)

	exitTicker := clk.NewTicker(500 * time.Millisecond)
	checkTicker := clk.NewTicker(25 * time.Millisecond)

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

	checkTicker.Reset(25 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	leaderFound = false

CorrectLeaderCheck:
	for {
		select {
		case <-checkTicker.Chan():
			leaderFound = false
			for _, node := range testNodes {
				if node.GetState() == Leader {
					if leaderID == node.ID {
						leaderFound = true
					}
					if node.ID != leaderID {
						t.Fatalf("Error: Wrong leader detected")
					}
				}
			}
			if !leaderFound {
				t.Fatalf("Error: Leader not found")
			}
		case <-exitTicker.Chan():
			break CorrectLeaderCheck
		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	t.Logf("Success: Leader is stable")

	t.Cleanup(func() {
		for _, node := range testNodes {
			go node.Shutdown()
		}
		for _, node := range testNodes {
			node.WaitAllGoroutines()
		}
	})
}

func TestLeaderElection_SplitVote(t *testing.T) {
	testNodes, clk := testSetup(t)

	for _, node := range testNodes {
		select {
		case node.electionTimeoutCh <- struct{}{}:
		default:
		}
	}

	exitTicker := clk.NewTicker(10 * time.Second)
	checkTicker := clk.NewTicker(20 * time.Millisecond)
	leaderFound := false
LeaderCheck:
	for {
		select {
		case <-checkTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == Leader {
					leaderFound = true
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
			clk.Advance(100 * time.Nanosecond)
			runtime.Gosched()
		}
	}

	if leaderFound {
		t.Logf("Success: Single Leader")
	} else {
		t.Fatalf("Error: Leader not found within 10 secs")
	}

	t.Cleanup(func() {
		for _, node := range testNodes {
			go node.Shutdown()
		}
		for _, node := range testNodes {
			node.WaitAllGoroutines()
		}
	})
}

func TestLeaderElection_LeaderCrashRecovery(t *testing.T) {
	testNodes, clk := testSetup(t)
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()

	checkTicker := clk.NewTicker(50 * time.Millisecond)
	exitTicker := clk.NewTicker(2 * time.Second)

CrashLeader:
	for {
		select {
		case <-checkTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == Leader {
					crashedNode := testNodes[node.ID]
					delete(testNodes, node.ID)
					node.cancel()
					go node.Shutdown()
					node.WaitAllGoroutines()
					runtime.Gosched()
					mockTransport := NewMockNetworkTransport(newCtx, node.ID, testNodes)
					testNodes[node.ID] = NewNode(newCtx, newCancel, crashedNode.ID, crashedNode.Address, crashedNode.Port, crashedNode.GrpcPort, crashedNode.dataDir, crashedNode.peers, crashedNode.Clock, MockListenerFactory, mockTransport)
					testNodes[node.ID].Start()
					break CrashLeader
				}
			}
		case <-exitTicker.Chan():
			log.Fatalf("Error: Leader not found within 2 seconds")
		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	clk.Advance(300 * time.Millisecond)
	runtime.Gosched()
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	var leaderFound bool

LeaderCheck:
	for {
		select {
		case <-checkTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == Leader {
					leaderFound = true
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

	if leaderFound {
		t.Logf("Success: Leader recovered after crash")
	} else {
		t.Fatalf("Error: Leader not found within 5 secs")
	}

	t.Cleanup(func() {
		for _, node := range testNodes {
			go node.Shutdown()
		}
		for _, node := range testNodes {
			node.WaitAllGoroutines()
		}
	})
}

func TestLogReplication_LeaderCommand(t *testing.T) {
	testCommand := &Command{
		Type:  CommandPut,
		Key:   "testKey",
		Value: []byte("testValue"),
	}

	testNodes, clk := testSetup(t)

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
			go node.Shutdown()
		}
		for _, node := range testNodes {
			node.WaitAllGoroutines()
		}
	})

}

func TestLogReplication_FollowerCrashAndRecovery(t *testing.T) {
	testNodes, clk := testSetup(t)
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()

	leaderID := ""
	followerID := ""
	exitTicker := clk.NewTicker(10 * time.Second)
	checkTicker := clk.NewTicker(50 * time.Millisecond)
	defer checkTicker.Stop()

LeaderFollowerSetup:
	for {
		select {
		case <-exitTicker.Chan():
			t.Fatalf("Leader not found within 10 secs")
		case <-checkTicker.Chan():
			for _, node := range testNodes {
				if node.GetState() == Leader {
					leaderID = node.ID
					if followerID != "" {
						break LeaderFollowerSetup
					}
				} else {
					followerID = node.ID
				}
			}
		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	testCommand := &Command{
		Type:  "PUT",
		Key:   "testKey",
		Value: []byte("testValue"),
	}

	select {
	case testNodes[leaderID].ClientCommandChan <- testCommand:
		t.Logf("Sent Client Command from test")
	case <-clk.After(3 * time.Second):
		log.Fatalf("Timed out sending client command")
	}

	exitTicker.Reset(10 * time.Second)
	for len(testNodes[followerID].data) < 1 {
		select {
		case <-exitTicker.Chan():
			t.Fatalf("Leader not found within 5 secs")
		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}
	deletedNode := testNodes[followerID]
	testNodes[followerID].cancel()
	go testNodes[followerID].Shutdown()
	testNodes[followerID].WaitAllGoroutines()
	delete(testNodes, followerID)
	mockTransport := NewMockNetworkTransport(newCtx, followerID, testNodes)
	testNodes[deletedNode.ID] = NewNode(
		newCtx,
		newCancel,
		deletedNode.ID,
		deletedNode.Address,
		deletedNode.Port,
		deletedNode.GrpcPort,
		deletedNode.dataDir,
		deletedNode.peers,
		deletedNode.Clock,
		deletedNode.listenerFactory,
		mockTransport,
	)
	testNodes[deletedNode.ID].Start()

	exitTicker.Reset(5 * time.Second)
	checkTicker.Reset(50 * time.Millisecond)

	if deletedNode.currentTerm != testNodes[followerID].GetCurrentTerm() || deletedNode.votedFor != testNodes[followerID].GetVotedFor() {
		log.Fatal("Error: Current Term Or Voted For Mismatch")
	}

	var recovered bool
FollowerRecoveryCheck:
	for {
		select {
		case <-checkTicker.Chan():
			recovered = true
			if testNodes[followerID].GetCommitIndex() != testNodes[leaderID].GetCommitIndex() {
				recovered = false
				continue
			}
			if len(testNodes[followerID].data) != len(testNodes[deletedNode.ID].data) {
				recovered = false
				continue
			}
			for leaderKey, leaderValue := range testNodes[leaderID].data {
				if value, ok := testNodes[followerID].data[leaderKey]; !ok || !bytes.Equal(leaderValue, value) {
					recovered = false
					break
				}
			}
			if recovered {
				break FollowerRecoveryCheck
			}

		case <-exitTicker.Chan():
			recovered = false
			break FollowerRecoveryCheck
		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if recovered {
		t.Logf("Success: Recovered follower command")
	} else {
		t.Fatalf("Error: Follower not recovered")
	}

	t.Cleanup(func() {
		for _, node := range testNodes {
			go node.Shutdown()
		}
		for _, node := range testNodes {
			node.WaitAllGoroutines()
		}
	})
}
