package cluster

import (
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"log"
	"testing"
	"time"
)

func newTestNode(t *testing.T, ctx context.Context, cancel context.CancelFunc, ID string, Address string, Port int, GrpcPort int, DataDir string, peerIDs []string, clk clockwork.Clock, transport NetworkTransport) *Node {
	newNode := NewNode(ctx, cancel, ID, Address, Port, GrpcPort, DataDir, peerIDs, clk, transport)
	newNode.Start()
	newNode.StartWg.Wait()
	return newNode
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
	}

	t.Cleanup(func() {
		for _, node := range testNodes {
			go node.Shutdown()
		}
		for _, node := range testNodes {
			clk.Advance(1 * time.Second)
			node.WaitAllGoroutines()
		}
	})
	t.Logf("Finish")
}

func TestLeaderElection_LeaderCrashRecovery(t *testing.T) {
	_, cancel, testNodes, clk := testSetup(t)
	defer cancel()
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()

	clk.Advance(3 * time.Second)
	startTime := time.Now()
	timeout := 500 * time.Millisecond

	for time.Since(startTime) < timeout {
		for _, node := range testNodes {
			if node.State == Leader {
				log.Printf("--------------------------------------CRASHING LEADER NODE------------------------------------------------")
				crashedNode := testNodes[node.ID]
				delete(testNodes, node.ID)
				node.Cancel()
				go node.Shutdown()
				clk.Advance(1 * time.Second)
				node.WaitAllGoroutines()
				mockTransport := NewMockNetworkTransport(node.ID, testNodes)
				testNodes[node.ID] = newTestNode(t, newCtx, newCancel, crashedNode.ID, crashedNode.Address, crashedNode.Port, crashedNode.GrpcPort, crashedNode.DataDir, crashedNode.Peers, crashedNode.Clock, mockTransport)
				break
			}
		}
		clk.Advance(3 * time.Millisecond)
		time.Sleep(3 * time.Millisecond)
	}

	clk.Advance(3 * time.Second)
	startTime = time.Now()
	timeout = 500 * time.Millisecond

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
		t.Logf("Success: New leader found after leader node crashed")
	} else {
		t.Fatalf("Error: New node not found after leader node crashed")
	}
	t.Cleanup(func() {
		for _, node := range testNodes {
			log.Printf("%s", node.ID)
			go node.Shutdown()
		}
		for _, node := range testNodes {
			clk.Advance(5 * time.Second)
			node.WaitAllGoroutines()
		}
	})
	t.Logf("Finish")
}
