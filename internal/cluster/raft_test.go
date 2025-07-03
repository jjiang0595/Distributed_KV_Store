package cluster

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
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

func TestLeaderElection_FindLeader(t *testing.T) {
	testNodes, _, _, clk := testSetup(t)
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

	cleanup(t, testNodes)

}

func TestLeaderElection_LeaderStability(t *testing.T) {
	testNodes, _, _, clk := testSetup(t)
	exitTicker := clk.NewTicker(500 * time.Millisecond)
	checkTicker := clk.NewTicker(25 * time.Millisecond)

	leaderID := findLeader(t, clk, testNodes, checkTicker, exitTicker)

	leaderFound := false
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
	cleanup(t, testNodes)
}

func TestLeaderElection_SplitVote(t *testing.T) {
	testNodes, _, _, clk := testSetup(t)
	for _, node := range testNodes {
		select {
		case node.electionTimeoutCh <- struct{}{}:
		default:
		}
	}

	exitTicker := clk.NewTicker(10 * time.Second)
	checkTicker := clk.NewTicker(20 * time.Millisecond)

	findLeader(t, clk, testNodes, checkTicker, exitTicker)

	cleanup(t, testNodes)
}

func TestLeaderElection_LeaderCrashRecovery(t *testing.T) {
	testNodes, _, _, clk := testSetup(t)
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
					mockTransport := NewMockNetworkTransport(newCtx)
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

	findLeader(t, clk, testNodes, checkTicker, exitTicker)

	t.Logf("Success: Leader recovered after crash")
	cleanup(t, testNodes)
}

func TestLogReplication_LeaderCommand(t *testing.T) {
	testCmd := &Command{
		Type:  CommandPut,
		Key:   "testKey",
		Value: "testValue",
	}

	testNodes, _, _, clk := testSetup(t)
	exitTicker := clk.NewTicker(10 * time.Second)
	checkTicker := clk.NewTicker(50 * time.Millisecond)
	defer checkTicker.Stop()

	leaderID := findLeader(t, clk, testNodes, checkTicker, exitTicker)

	cmdToBytes, err := json.Marshal(testCmd)
	if err != nil {
		t.Fatal(err)
	}

	err = testNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	checkTicker.Reset(100 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	replicationTicker := clk.NewTicker(50 * time.Millisecond)
	defer replicationTicker.Stop()

	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-replicationTicker.Chan():
			replicated = true
			for _, node := range testNodes {
				node.kvStore.mu.Lock()
				if value, ok := node.kvStore.store[testCmd.Key]; !ok || value != testCmd.Value {
					replicated = false
					node.kvStore.mu.Unlock()
					break
				}
				node.kvStore.mu.Unlock()
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

	cleanup(t, testNodes)
}

func TestLogReplication_FollowerCrashAndRecovery(t *testing.T) {
	testNodes, kvStores, transport, clk := testSetup(t)
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

	testCmd := &Command{
		Type:  CommandPut,
		Key:   "testKey",
		Value: "testValue",
	}

	cmdToBytes, err := json.Marshal(testCmd)
	if err != nil {
		t.Fatal(err)
	}

	err = testNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(1 * time.Second)
LeaderCheck:
	for {
		select {
		case <-checkTicker.Chan():
			if len(testNodes[followerID].kvStore.GetData()) == 1 {
				//log.Printf("FOLLOWER %v: LOG %v", followerID, testNodes[followerID].GetLog())
				break LeaderCheck
			}
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
	transport.UnregisterRPCServer(deletedNode)
	deletedNode.WaitAllGoroutines()
	delete(kvStores, followerID)
	testNodes[followerID] = NewNode(
		newCtx,
		newCancel,
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

			followerCommitIndex := testNodes[followerID].GetCommitIndex()
			followerData := testNodes[followerID].kvStore.GetData()

			leaderCommitIndex := testNodes[leaderID].GetCommitIndex()
			leaderData := testNodes[leaderID].kvStore.GetData()

			if len(leaderData) != len(followerData) || followerCommitIndex != leaderCommitIndex {
				recovered = false
				continue
			}
			for leaderKey, leaderValue := range leaderData {
				if value, ok := testNodes[followerID].kvStore.Get(leaderKey); !ok || value != leaderValue {
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

	if !recovered {
		t.Fatalf("Error: Follower not recovered")
	}

	t.Logf("Success: Recovered follower command")
	cleanup(t, testNodes)
}

func TestLogReplication_LogDivergence(t *testing.T) {
	testNodes, _, transport, clk := testSetup(t)
	checkTicker := clk.NewTicker(25 * time.Millisecond)
	exitTicker := clk.NewTicker(500 * time.Millisecond)

	leaderID := findLeader(t, clk, testNodes, checkTicker, exitTicker)
	var followerID string
	var followerNode *Node

	for _, node := range testNodes {
		if node.GetState() == Follower {
			followerID = node.ID
			followerNode = node
			break
		}
	}

	leaderNode := testNodes[leaderID]
	testCmdA := &Command{
		Type:  CommandPut,
		Key:   "key_A",
		Value: "value_A",
	}

	cmdToBytes, err := json.Marshal(testCmdA)
	if err != nil {
		t.Fatal(err)
	}

	err = testNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	testCmdB := &Command{
		Type:  CommandPut,
		Key:   "key_B",
		Value: "value_B",
	}

	cmdToBytes, err = json.Marshal(testCmdB)
	if err != nil {
		t.Fatal(err)
	}

	err = testNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	checkTicker.Reset(100 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
LogReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			if len(leaderNode.GetLog()) == len(followerNode.GetLog()) {
				break LogReplicationCheck
			}

		case <-exitTicker.Chan():
			log.Fatalf("Error: Logs not replicated within time limit")

		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	for _, peerID := range testNodes[followerID].peers {
		transport.SetPartition(followerID, peerID, true)
		transport.SetPartition(peerID, followerID, true)
	}

	logEntryC := &LogEntry{
		Term: followerNode.currentTerm + 1,
		Index: func() uint64 {
			if len(followerNode.log) == 0 {
				return 0
			}
			return followerNode.log[len(followerNode.log)-1].Index + 1
		}(),
		Command: []byte("testCmdC"),
	}
	followerNode.raftMu.Lock()
	followerNode.log = append(followerNode.log, logEntryC)
	followerNode.raftMu.Unlock()

	logEntryD := &LogEntry{
		Term: logEntryC.Term,
		Index: func() uint64 {
			if len(followerNode.log) == 0 {
				return 0
			}
			return followerNode.log[len(followerNode.log)-1].Index + 1
		}(),
		Command: []byte("testCmdD"),
	}

	followerNode.raftMu.Lock()
	followerNode.log = append(followerNode.log, logEntryD)
	followerNode.raftMu.Unlock()

	for _, peerID := range testNodes[followerID].peers {
		transport.SetPartition(followerID, peerID, false)
		transport.SetPartition(peerID, followerID, false)
	}

	testCmdE := &Command{
		Type:  CommandPut,
		Key:   "key_E",
		Value: "value_E",
	}
	marshalledCommandE, err := json.Marshal(testCmdE)
	if err != nil {
		return
	}

	err = testNodes[followerID].ProposeCommand(marshalledCommandE)
	if err != nil {
		return
	}

	for i := 0; i < 5; i++ {
		clk.Advance(50 * time.Millisecond)
		runtime.Gosched()
	}

	checkTicker.Reset(100 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	resolved := false

ConflictingLogsCheck:
	for {
		select {
		case <-checkTicker.Chan():
			resolved = true
			followerLog := followerNode.GetLog()
			leaderLog := leaderNode.GetLog()
			leaderData := leaderNode.kvStore.GetData()
			followerData := followerNode.kvStore.GetData()

			if len(followerLog) != len(leaderLog) || len(followerData) != len(leaderData) {
				resolved = false
				continue
			}
			for index, leaderEntry := range leaderLog {
				if !compareLogs(leaderEntry, followerLog[index]) {
					resolved = false
					break
				}
			}
			for leaderKey, leaderValue := range leaderData {
				if value, ok := followerData[leaderKey]; !ok || leaderValue != value {
					resolved = false
					break
				}
			}
			break ConflictingLogsCheck

		case <-exitTicker.Chan():
			resolved = false
			break ConflictingLogsCheck

		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if !resolved {
		t.Fatalf("Error: Follower's diverging log not resolved")
	}

	t.Logf("Success: Resolved diverging log entries")
	cleanup(t, testNodes)
}

func TestNodePartition_LeaderPartition(t *testing.T) {
	testNodes, _, transport, clk := testSetup(t)
	exitTicker := clk.NewTicker(500 * time.Millisecond)
	checkTicker := clk.NewTicker(25 * time.Millisecond)

	oldLeaderID := findLeader(t, clk, testNodes, checkTicker, exitTicker)

	for _, peerID := range testNodes[oldLeaderID].peers {
		transport.SetPartition(oldLeaderID, peerID, true)
	}

	majorityNodes := make(map[string]*Node)
	for _, node := range testNodes {
		if node.ID != oldLeaderID {
			majorityNodes[node.ID] = node
		}
	}

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(1 * time.Second)
	newLeaderID := findLeader(t, clk, majorityNodes, checkTicker, exitTicker)

	for _, peerID := range testNodes[oldLeaderID].peers {
		transport.SetPartition(oldLeaderID, peerID, false)
	}

	exitTicker.Reset(5 * time.Second)
	checkTicker.Reset(50 * time.Millisecond)
	stepDown := false

ReintroduceOldLeader:
	for {
		select {
		case <-checkTicker.Chan():
			if testNodes[newLeaderID].GetState() == Leader && testNodes[oldLeaderID].GetState() == Follower {
				stepDown = true
				break ReintroduceOldLeader
			}

		case <-exitTicker.Chan():
			if testNodes[newLeaderID].GetState() == Leader && testNodes[oldLeaderID].GetState() == Follower {
				stepDown = true
				break ReintroduceOldLeader
			}
			break ReintroduceOldLeader

		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if !stepDown {
		t.Fatalf("Error: Old leader didn't step down.")
	}

	t.Logf("Success: Old leader step down")
	cleanup(t, testNodes)
}

func TestNodesCrash_CrashAndRecovery(t *testing.T) {
	t.Parallel()
	testNodes, kvStore, transport, clk := testSetup(t)

	exitTicker := clk.NewTicker(500 * time.Millisecond)
	checkTicker := clk.NewTicker(25 * time.Millisecond)

	leaderID := findLeader(t, clk, testNodes, checkTicker, exitTicker)

	testCmdA := &Command{
		Type:  CommandPut,
		Key:   "key_A",
		Value: "value_A",
	}

	cmdToBytes, err := json.Marshal(testCmdA)
	if err != nil {
		t.Fatal(err)
	}

	err = testNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	testCmdB := &Command{
		Type:  CommandPut,
		Key:   "key_B",
		Value: "value_B",
	}

	cmdToBytes, err = json.Marshal(testCmdB)
	if err != nil {
		t.Fatal(err)
	}

	err = testNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	exitTicker.Reset(2 * time.Second)
	checkTicker.Reset(50 * time.Millisecond)

	var persisted bool
DataPersistedCheck:
	for {
		select {
		case <-checkTicker.Chan():
			persisted = true
			for nodeID, _ := range testNodes {
				if nodeID == leaderID {
					continue
				}
				log.Printf("follower: %v, leader: %v", len(kvStore[nodeID].GetData()), len(kvStore[leaderID].GetData()))
				if len(kvStore[nodeID].GetData()) == 2 {
					persisted = false
					break
				}
			}
			if persisted {
				log.Printf("PERSISTED")
				break DataPersistedCheck
			}
		case <-exitTicker.Chan():
			log.Fatalf("Error: Timed out waiting for persisted data")
		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	clk.Advance(50 * time.Millisecond)
	runtime.Gosched()

	crashedTestNodes := make(map[string]*Node)
	for nodeID := range testNodes {
		deletedNode := crashNode(nodeID, testNodes, kvStore, transport)
		crashedTestNodes[nodeID] = deletedNode
		log.Printf("AAAA %v", crashedTestNodes[nodeID].kvStore.GetData())
	}

	for nodeID := range testNodes {
		recoverNode(crashedTestNodes[nodeID], nodeID, testNodes, kvStore, transport, clk)
	}

	checkTicker.Reset(100 * time.Millisecond)
	exitTicker.Reset(2 * time.Second)
	findLeader(t, clk, crashedTestNodes, checkTicker, exitTicker)

	var recovered bool
RecoveredCheck:
	for {
		select {
		case <-checkTicker.Chan():
			recovered = false
			for nodeID, _ := range crashedTestNodes {
				for key, oldVal := range crashedTestNodes[nodeID].kvStore.GetData() {
					if newVal, ok := testNodes[nodeID].kvStore.Get(key); ok && oldVal == newVal {
						recovered = true
					}
				}
			}
			if recovered {
				break RecoveredCheck
			}

		case <-exitTicker.Chan():
			break RecoveredCheck

		default:
			clk.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if !recovered {
		t.Fatalf("Error: New nodes' data not consistent with old nodes' data")
	}
	cleanup(t, testNodes)
}
