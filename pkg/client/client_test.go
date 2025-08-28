package client

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"errors"
	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"google.golang.org/protobuf/testing/protocmp"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

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

func TestClient_Put_LeaderDiscovery(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()

	err := test.Client.PUT(ctx, "52877", "Test Review", 5, "This is my first test review.")
	if err != nil {
		t.Errorf("Error putting key: %v", err)
		return
	}

	leaderData := test.TestNodes[leaderId].GetKVStore().GetData("52877")
	leaderCommitIndex := test.TestNodes[leaderId].GetCommitIndex()
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for _, node := range test.TestNodes {
				nodeData := node.GetKVStore().GetData("52877")
				nodeCommitIndex := node.GetCommitIndex()
				log.Printf("LEADER %d, NODE %d", leaderCommitIndex, nodeCommitIndex)
				if nodeCommitIndex != leaderCommitIndex || !reflect.DeepEqual(nodeData, leaderData) {
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
}

func TestClient_Put_FollowerRedirects(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	followerID, leaderId := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	test.waitForLeader(leaderId, checkTicker, exitTicker)

	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[followerID])

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()

	err := test.Client.PUT(ctx, "52877", "Test Review", 5, "This is my first test review.")
	if err != nil {
		t.Errorf("Error putting key: %v", err)
		return
	}
	leaderData := test.TestNodes[leaderId].GetKVStore().GetData("52877")
	leaderCommitIndex := test.TestNodes[leaderId].GetCommitIndex()

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for _, node := range test.TestNodes {
				nodeData := node.GetKVStore().GetData("52877")
				nodeCommitIndex := node.GetCommitIndex()
				if nodeCommitIndex != leaderCommitIndex || !reflect.DeepEqual(nodeData, leaderData) {
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
}

func TestClient_Put_TransientError(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderId])

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	test.waitForLeader(leaderId, checkTicker, exitTicker)

	test.MockHTTPRT.SetTransientError(1, http.StatusServiceUnavailable, nil)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()

	err := test.Client.PUT(ctx, "52877", "Test Review", 5, "This is my first test review.")
	if err != nil {
		t.Errorf("Error putting key: %v", err)
		return
	}

	leaderData := test.TestNodes[leaderId].GetKVStore().GetData("52877")
	leaderCommitIndex := test.TestNodes[leaderId].GetCommitIndex()
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for _, node := range test.TestNodes {
				nodeData := node.GetKVStore().GetData("52877")
				nodeCommitIndex := node.GetCommitIndex()
				if nodeCommitIndex != leaderCommitIndex || !reflect.DeepEqual(nodeData, leaderData) {
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
}

func TestClient_Put_MaxRetries(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderId])

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	test.waitForLeader(leaderId, checkTicker, exitTicker)

	test.MockHTTPRT.SetTransientError(3, http.StatusServiceUnavailable, nil)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 5*time.Second)
	defer cancel()
	err := test.Client.PUT(ctx, "52877", "Test Review", 5, "This is my first test review.")
	test.cleanup()
	if err != nil {
		var maxRetriesErr ErrMaxRetries
		if errors.As(err, &maxRetriesErr) {
			if maxRetriesErr.Type != http.MethodPut {
				t.Errorf("Error: Wrong HTTP method %s", maxRetriesErr.Type)
			}
			if maxRetriesErr.Retries != test.Client.maxRetries {
				t.Errorf("Error: Didn't receive max retries")
			}
			var serviceUnavailableErr ErrServiceUnavailable
			if errors.As(maxRetriesErr.LastErr, &serviceUnavailableErr) {
				if serviceUnavailableErr.StatusCode != http.StatusServiceUnavailable {
					t.Errorf("Error: Wrong status error code %d", serviceUnavailableErr.StatusCode)
				}
			}
		}
	} else {
		t.Fatalf("Expected max retries error to be returned")
	}
}

func TestClient_Put_ContextTimeout(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderId])

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	test.waitForLeader(leaderId, checkTicker, exitTicker)

	test.MockHTTPRT.SetDelay(1 * time.Second)
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer cancel()
	err := test.Client.PUT(ctx, "52877", "Test Review", 5, "This is my first test review.")

	if err != nil {
		var maxRetriesErr ErrMaxRetries
		if errors.As(err, &maxRetriesErr) {
			if maxRetriesErr.Type != http.MethodPut {
				t.Errorf("Error: Wrong HTTP method %s", maxRetriesErr.Type)
			}
			if maxRetriesErr.Retries != test.Client.maxRetries {
				t.Error("Error: Didn't receive max retries")
			}
			if !errors.Is(maxRetriesErr.LastErr, context.DeadlineExceeded) {
				t.Error("Error: Latest error is not DeadlineExceeded")
			}
		} else {
			t.Fatal("Error: Wrong error code")
		}
	} else {
		t.Fatal("Error: No response from server to context timeout exceeded")
	}
}

func TestClient_Get_Success(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)

	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderId])
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()

	err := test.Client.PUT(ctx, "52877", "Test Review", 5, "This is my first test review.")
	if err != nil {
		t.Fatalf("Error putting key: %v", err)
	}

	leaderData := test.TestNodes[leaderId].GetKVStore().GetData("52877")

	var reviewsSlice []*cluster.ReviewCommand
	for _, review := range leaderData {
		reviewsSlice = append(reviewsSlice, review)
	}
	leaderCommitIndex := test.TestNodes[leaderId].GetCommitIndex()

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)

	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for _, node := range test.TestNodes {
				nodeData := node.GetKVStore().GetData("52877")
				nodeCommitIndex := node.GetCommitIndex()
				if nodeCommitIndex != leaderCommitIndex || !reflect.DeepEqual(nodeData, leaderData) {
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

	getCtx, getCancel := clockwork.WithTimeout(context.Background(), test.Clock, 2*time.Second)
	defer getCancel()
	reviews, err := test.Client.GET(getCtx, "52877")
	if err != nil {
		t.Fatalf("Error getting key: %v", err)
	}

	if diff := cmp.Diff(reviewsSlice, reviews, protocmp.Transform()); diff != "" {
		t.Errorf("Values were not equal (-expected +got):\n%s", diff)
	}
}

func TestClient_Get_NotFound(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	test.waitForLeader(leaderId, checkTicker, exitTicker)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer cancel()
	value, err := test.Client.GET(ctx, "52877")
	if value != nil {
		t.Error("Error: Expected empty object")
	}
	if err != nil {
		var notFoundErr ErrNotFound
		if errors.As(err, &notFoundErr) {
			if notFoundErr.StatusCode != http.StatusNotFound {
				t.Fatalf("Error: Unexpected status code: %d", notFoundErr.StatusCode)
			}
		}
	} else {
		t.Fatal("Error: Unexpected error while handling missing key")
	}
}

func TestClient_Get_LeaderAddressUpdate(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)

	_, leaderId := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	test.waitForLeader(leaderId, checkTicker, exitTicker)

	test.Client.leaderAddress.Store("")
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer cancel()
	_, err := test.Client.GET(ctx, "testKey")
	if err != nil {
		if test.Client.leaderAddress.Load() != test.PeerHTTPAddrs[leaderId] {
			t.Errorf("Error: Unexpected leader address update")
		}

		var notFoundErr ErrNotFound
		if errors.As(err, &notFoundErr) {
			if notFoundErr.StatusCode != http.StatusNotFound {
				t.Fatalf("Error: Unexpected status code: %d", notFoundErr.StatusCode)
			}
		}
	} else {
		t.Errorf("Error: Expected leader address update & status not found")
	}
}

func TestClient_Get_FollowerRedirect(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	followerID, leaderId := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	test.waitForLeader(leaderId, checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[followerID])
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer cancel()
	_, err := test.Client.GET(ctx, "testKey")
	if err != nil {
		var redirectErr ErrRedirect
		if test.Client.leaderAddress.Load() != test.PeerHTTPAddrs[leaderId] {
			t.Errorf("Error: Expected client's leader addresss to be updated")
		}
		if errors.As(err, &redirectErr) {
			if redirectErr.StatusCode != http.StatusTemporaryRedirect {
				t.Errorf("Error: Expected StatusTemporaryRedirect, got %d", redirectErr.StatusCode)
			}
		}
	} else {
		t.Fatalf("Error: Expected StatusTemporaryRedirect")
	}
}

func TestClient_Get_TransientError(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	test.waitForLeader(leaderId, checkTicker, exitTicker)

	putCtx, putCancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer putCancel()
	err := test.Client.PUT(putCtx, "52877", "Test Review", 5, "This is my first test review.")
	if err != nil {
		t.Fatalf("Error putting key: %v", err)
	}

	leaderData := test.TestNodes[leaderId].GetKVStore().GetData("52877")
	var reviewsSlice []*cluster.ReviewCommand
	for _, review := range leaderData {
		reviewsSlice = append(reviewsSlice, review)
	}

	test.MockHTTPRT.SetTransientError(2, http.StatusRequestTimeout, nil)
	getCtx, getCancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer getCancel()
	val, err := test.Client.GET(getCtx, "52877")
	if diff := cmp.Diff(val, reviewsSlice, protocmp.Transform()); diff != "" {
		t.Errorf("Values were not equal (-expected +got):\n%s", diff)
	}
	if err != nil {
		t.Fatalf("Error: Fail to get key %s, %s", "52877", err.Error())
	}
}

func TestClient_Get_MaxRetries(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	test.waitForLeader(leaderId, checkTicker, exitTicker)

	putCtx, putCancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer putCancel()
	err := test.Client.PUT(putCtx, "52877", "Test Review", 5, "This is my first test review.")
	if err != nil {
		t.Fatalf("Error putting key: %v", err)
	}

	test.MockHTTPRT.SetTransientError(3, http.StatusRequestTimeout, nil)
	getCtx, getCancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer getCancel()
	val, err := test.Client.GET(getCtx, "52877")
	if val != nil {
		t.Errorf("Error: Expected no value for key %s, got %s", "52877", val)
	}
	if err != nil {
		var statusTimeoutErr ErrRequestTimeout
		if errors.As(err, &statusTimeoutErr) {
			if statusTimeoutErr.StatusCode != http.StatusRequestTimeout {
				t.Errorf("Error: Expected StatusRequestTimeout, got %d", statusTimeoutErr.StatusCode)
			}
			if statusTimeoutErr.Key != "52877" {
				t.Errorf("Error: Expected Key %s, got %s", "52877", val)
			}
		}
	} else {
		t.Fatalf("Error: Invalid error while injecting maximum number of errors")
	}
}

func TestClient_Get_ContextTimeout(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)

	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	test.waitForLeader(leaderId, checkTicker, exitTicker)

	putCtx, putCancel := clockwork.WithTimeout(context.Background(), test.Clock, 150*time.Millisecond)
	defer putCancel()
	err := test.Client.PUT(putCtx, "52877", "Test Review", 5, "This is my first test review.")
	if err != nil {
		t.Fatalf("Error putting key: %v", err)
	}

	test.MockHTTPRT.SetDelay(1 * time.Second)
	getCtx, getCancel := clockwork.WithTimeout(context.Background(), test.Clock, 150*time.Millisecond)
	defer getCancel()
	_, err = test.Client.GET(getCtx, "52877")
	if err != nil {
		var maxRetriesErr ErrMaxRetries
		if errors.As(err, &maxRetriesErr) {
			if maxRetriesErr.Type != http.MethodGet {
				log.Printf("%v", maxRetriesErr.Type)
				t.Error("Error: Expected HTTP method to be of type GET")
			}
			if maxRetriesErr.Retries != test.Client.maxRetries {
				t.Error("Error: Max retries didn't match")
			}
			if !errors.Is(maxRetriesErr.LastErr, context.DeadlineExceeded) {
				t.Errorf("Error: Expected error %v to be of type DeadlineExceeded", maxRetriesErr.LastErr)
			}
		} else {
			t.Error("Error: Expected HTTP error to be ErrMaxRetries")
		}
	} else {
		t.Fatalf("Error: Incorrect response after context timeout")
	}
}

func TestClient_Get_NonRetryableError(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(200 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(10 * time.Second)
	_, leaderId := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(200 * time.Millisecond)
	exitTicker.Reset(10 * time.Second)
	test.waitForLeader(leaderId, checkTicker, exitTicker)

	test.MockHTTPRT.SetTransientError(1, http.StatusNotFound, nil)
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 150*time.Millisecond)
	defer cancel()
	val, err := test.Client.GET(ctx, "52877")
	if val != nil {
		t.Errorf("Error: Expected value to be empty")
	}
	if err != nil {
		var statusNotFoundError ErrNotFound
		if errors.As(err, &statusNotFoundError) {
			if statusNotFoundError.Key != "52877" {
				t.Errorf("Error: Expected key to be 52877")
			}
			if statusNotFoundError.StatusCode != http.StatusNotFound {
				t.Errorf("Error: Expected status code to be 404")
			}
		} else {
			t.Fatalf("Error: Fail to get key %s, %s", statusNotFoundError.Key, err.Error())
		}
	} else {
		t.Fatalf("Error: Expected GET to fail with Status Not Found error")
	}
}
