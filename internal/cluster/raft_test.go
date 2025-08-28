package cluster

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
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
	test := testSetup(t)
	defer test.cleanup()

	exitTicker := test.Clock.NewTicker(10 * time.Second)
	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)

	leaderFound := false
LeaderCheck:
	for {
		select {
		case <-checkTicker.Chan():
			for _, node := range test.TestNodes {
				if node.GetState() == Leader {
					leaderFound = true
					break LeaderCheck
				}
			}

		case <-exitTicker.Chan():
			for _, node := range test.TestNodes {
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
		t.Fatalf("Error: Leader not found within 500 ms")
	}
}

func TestLeaderElection_LeaderStability(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	exitTicker := test.Clock.NewTicker(10 * time.Second)
	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)

	leaderID := test.findLeader(test.TestNodes, checkTicker, exitTicker)

	leaderFound := false
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	leaderFound = false

CorrectLeaderCheck:
	for {
		select {
		case <-checkTicker.Chan():
			leaderFound = false
			for _, node := range test.TestNodes {
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
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}
}

func TestLeaderElection_SplitVote(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()
	for _, node := range test.TestNodes {
		select {
		case node.electionTimeoutCh <- struct{}{}:
		default:
		}
	}

	exitTicker := test.Clock.NewTicker(10 * time.Second)
	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)

	test.findLeader(test.TestNodes, checkTicker, exitTicker)
}

func TestLeaderElection_LeaderCrashRecovery(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)

CrashLeader:
	for {
		select {
		case <-checkTicker.Chan():
			for _, node := range test.TestNodes {
				if node.GetState() == Leader {
					crashedNode := test.TestNodes[node.ID]
					delete(test.TestNodes, node.ID)
					node.cancel()
					go node.Shutdown()
					node.WaitAllGoroutines()
					runtime.Gosched()
					mockTransport := NewMockNetworkTransport(newCtx)
					test.TestNodes[node.ID] = NewNode(newCtx, newCancel, crashedNode.ID, crashedNode.Address, crashedNode.Port, crashedNode.GrpcPort, crashedNode.dataDir, crashedNode.peers, crashedNode.Clock, MockListenerFactory, mockTransport)
					test.TestNodes[node.ID].Start()
					break CrashLeader
				}
			}
		case <-exitTicker.Chan():
			t.Fatalf("Error: Leader not found within 2 seconds")
		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	test.Clock.Advance(300 * time.Millisecond)
	runtime.Gosched()
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	test.findLeader(test.TestNodes, checkTicker, exitTicker)
}

func TestLogReplication_LeaderCommand(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	testCmd := &Command{
		Type:  CommandPut,
		Key:   "52877",
		Value: test.generateReview("52877", "Test Review", 5, "This is a test review"),
	}

	exitTicker := test.Clock.NewTicker(10 * time.Second)
	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	defer checkTicker.Stop()

	leaderID := test.findLeader(test.TestNodes, checkTicker, exitTicker)

	cmdToBytes, err := json.Marshal(testCmd)
	if err != nil {
		t.Fatal(err)
	}

	err = test.TestNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	leaderData := test.TestNodes[leaderID].kvStore.GetData("52877")
	leaderCommitIndex := test.TestNodes[leaderID].GetCommitIndex()
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for _, node := range test.TestNodes {
				if !reflect.DeepEqual(node.kvStore.GetData("52877"), leaderData) || node.GetCommitIndex() != leaderCommitIndex {
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

	if !replicated {
		t.Fatalf("Error: Replicated leader command not replicated to all nodes. Replicated to %v nodes.", replicated)
	}
}

func TestLogReplication_FollowerCrashAndRecovery(t *testing.T) {

	test := testSetup(t)
	defer test.cleanup()

	leaderID := ""
	followerID := ""
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	defer checkTicker.Stop()

LeaderFollowerSetup:
	for {
		select {
		case <-exitTicker.Chan():
			t.Fatalf("Leader not found within 10 secs")
		case <-checkTicker.Chan():
			for _, node := range test.TestNodes {
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
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	testCmd := &Command{
		Type:  CommandPut,
		Key:   "52877",
		Value: test.generateReview("52877", "Test Review", 5, "This is a test review"),
	}

	cmdToBytes, err := json.Marshal(testCmd)
	if err != nil {
		t.Fatal(err)
	}

	err = test.TestNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	leaderCommitIndex := test.TestNodes[leaderID].GetCommitIndex()
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
ReplicatedCheck:
	for {
		select {
		case <-checkTicker.Chan():
			if leaderCommitIndex == test.TestNodes[followerID].GetCommitIndex() {
				leaderData := test.TestNodes[leaderID].kvStore.GetData("52877")
				followerData := test.TestNodes[followerID].kvStore.GetData("52877")
				if reflect.DeepEqual(leaderData, followerData) {
					break ReplicatedCheck
				}
			}
		case <-exitTicker.Chan():
			t.Fatalf("Data is not replicated within 10 secs")
		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	deletedNode := test.TestNodes[followerID]
	test.crashAndRecoverNode(followerID)

	exitTicker.Reset(10 * time.Second)
	checkTicker.Reset(200 * time.Millisecond)

	if deletedNode.currentTerm != test.TestNodes[followerID].GetCurrentTerm() || deletedNode.votedFor != test.TestNodes[followerID].GetVotedFor() {
		t.Fatal("Error: Current Term Or Voted For Mismatch")
	}

	var recovered bool
FollowerRecoveryCheck:
	for {
		select {
		case <-checkTicker.Chan():
			recovered = true

			followerCommitIndex := test.TestNodes[followerID].GetCommitIndex()
			followerData := test.TestNodes[followerID].kvStore.GetData("52877")

			leaderCommitIndex := test.TestNodes[leaderID].GetCommitIndex()
			leaderData := test.TestNodes[leaderID].kvStore.GetData("52877")

			if len(leaderData) != len(followerData) || followerCommitIndex != leaderCommitIndex {
				recovered = false
				continue
			}
			for leaderKey, leaderValue := range leaderData {
				followerValue, ok := followerData[leaderKey]
				if !ok {
					recovered = false
					break FollowerRecoveryCheck
				}

				if !reflect.DeepEqual(leaderValue, followerValue) {
					recovered = false
					break FollowerRecoveryCheck
				}
			}
			if recovered {
				break FollowerRecoveryCheck
			}

		case <-exitTicker.Chan():
			recovered = false
			break FollowerRecoveryCheck
		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if !recovered {
		t.Fatalf("Error: Follower not recovered")
	}
}

func TestLogReplication_LogDivergence(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)

	leaderID := test.findLeader(test.TestNodes, checkTicker, exitTicker)
	var followerID string
	var followerNode *Node

	for _, node := range test.TestNodes {
		if node.GetState() == Follower {
			followerID = node.ID
			followerNode = node
			break
		}
	}

	leaderNode := test.TestNodes[leaderID]
	testCmdA := test.generateReview("52877", "Test Review", 5, "This is a test review")

	cmdToBytes, err := json.Marshal(testCmdA)
	if err != nil {
		t.Fatal(err)
	}

	err = test.TestNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	testCmdB := test.generateReview("52877", "Test Review B", 5, "This is the second test review")

	cmdToBytes, err = json.Marshal(testCmdB)
	if err != nil {
		t.Fatal(err)
	}

	err = test.TestNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
LogReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			if len(leaderNode.GetLog()) == len(followerNode.GetLog()) {
				break LogReplicationCheck
			}

		case <-exitTicker.Chan():
			t.Fatalf("Error: Logs not replicated within time limit")

		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	for _, peerID := range test.TestNodes[followerID].peers {
		test.MockTransport.SetPartition(followerID, peerID, true)
		test.MockTransport.SetPartition(peerID, followerID, true)
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

	for _, peerID := range test.TestNodes[followerID].peers {
		test.MockTransport.SetPartition(followerID, peerID, false)
		test.MockTransport.SetPartition(peerID, followerID, false)
	}

	testCmdE := test.generateReview("52877", "Test Review E", 5, "This is the third test review")
	marshalledCommandE, err := json.Marshal(testCmdE)
	if err != nil {
		return
	}

	err = test.TestNodes[followerID].ProposeCommand(marshalledCommandE)
	if err != nil {
		return
	}

	for i := 0; i < 5; i++ {
		test.Clock.Advance(50 * time.Millisecond)
		runtime.Gosched()
	}

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	resolved := false

ConflictingLogsCheck:
	for {
		select {
		case <-checkTicker.Chan():
			resolved = true
			followerLog := followerNode.GetLog()
			leaderLog := leaderNode.GetLog()
			leaderData := leaderNode.kvStore.GetData("52877")
			followerData := followerNode.kvStore.GetData("52877")

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
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if !resolved {
		t.Fatalf("Error: Follower's diverging log not resolved")
	}
}

func TestNodePartition_LeaderPartition(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	oldLeaderID := test.findLeader(test.TestNodes, checkTicker, exitTicker)

	for _, peerID := range test.TestNodes[oldLeaderID].peers {
		test.MockTransport.SetPartition(oldLeaderID, peerID, true)
	}

	majorityNodes := make(map[string]*Node)
	for _, node := range test.TestNodes {
		if node.ID != oldLeaderID {
			majorityNodes[node.ID] = node
		}
	}

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	newLeaderID := test.findLeader(majorityNodes, checkTicker, exitTicker)

	for _, peerID := range test.TestNodes[oldLeaderID].peers {
		test.MockTransport.SetPartition(oldLeaderID, peerID, false)
	}

	exitTicker.Reset(10 * time.Second)
	checkTicker.Reset(200 * time.Millisecond)
	stepDown := false

ReintroduceOldLeader:
	for {
		select {
		case <-checkTicker.Chan():
			if test.TestNodes[newLeaderID].GetState() == Leader && test.TestNodes[oldLeaderID].GetState() == Follower {
				stepDown = true
				break ReintroduceOldLeader
			}

		case <-exitTicker.Chan():
			if test.TestNodes[newLeaderID].GetState() == Leader && test.TestNodes[oldLeaderID].GetState() == Follower {
				stepDown = true
			}
			break ReintroduceOldLeader

		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if !stepDown {
		t.Fatalf("Error: Old leader didn't step down.")
	}

	t.Cleanup(func() {
		for _, node := range majorityNodes {
			go node.Shutdown()
		}
		for nodeID, node := range majorityNodes {
			test.KvStores[nodeID] = nil
			test.MockTransport.UnregisterRPCServer(node)
			node.WaitAllGoroutines()
		}
	})
}

func TestNodesCrash_CrashAndRecovery(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	exitTicker := test.Clock.NewTicker(10 * time.Second)
	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)

	leaderID := test.findLeader(test.TestNodes, checkTicker, exitTicker)

	testCmdA := test.generateReview("52877", "Test Review", 5, "This is a test review")

	cmdToBytes, err := json.Marshal(testCmdA)
	if err != nil {
		t.Fatal(err)
	}

	err = test.TestNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	testCmdB := test.generateReview("52877", "Test Review", 5, "This is the second test review")

	cmdToBytes, err = json.Marshal(testCmdB)
	if err != nil {
		t.Fatal(err)
	}

	err = test.TestNodes[leaderID].ProposeCommand(cmdToBytes)
	if err != nil {
		t.Fatal(err)
	}

	exitTicker.Reset(10 * time.Second)
	checkTicker.Reset(200 * time.Millisecond)

	var persisted bool
DataPersistedCheck:
	for {
		select {
		case <-checkTicker.Chan():
			persisted = true
			for nodeID, _ := range test.TestNodes {
				if nodeID == leaderID {
					continue
				}
				if len(test.KvStores[nodeID].GetData("52877")) == 2 {
					persisted = false
					break
				}
			}
			if persisted {
				break DataPersistedCheck
			}
		case <-exitTicker.Chan():
			t.Fatalf("Error: Timed out waiting for persisted data")
		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	leaderData := test.TestNodes[leaderID].kvStore.GetData("52877")
	leaderCommitIndex := test.TestNodes[leaderID].GetCommitIndex()

	crashedTestNodes := make(map[string]*Node)
	for nodeID := range test.TestNodes {
		if nodeID != leaderID {
			deletedNode := test.crashNode(nodeID)
			crashedTestNodes[nodeID] = deletedNode
		}
	}

	for nodeID := range crashedTestNodes {
		test.recoverNode(test.TestNodes[nodeID], nodeID)
	}

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	var recovered bool
RecoveredCheck:
	for {
		select {
		case <-checkTicker.Chan():
			recovered = true
			for _, node := range test.TestNodes {
				nodeData := node.kvStore.GetData("52877")
				nodeCommitIndex := node.GetCommitIndex()

				if !reflect.DeepEqual(leaderData, nodeData) || leaderCommitIndex != nodeCommitIndex {
					recovered = false
					break
				}
			}

			if recovered {
				break RecoveredCheck
			}

		case <-exitTicker.Chan():
			break RecoveredCheck

		default:
			test.Clock.Advance(1 * time.Microsecond)
			runtime.Gosched()
		}
	}

	if !recovered {
		t.Fatalf("Error: New nodes' data not consistent with old nodes' data")
	}
}
