package client

import (
	"context"
	"errors"
	"github.com/jonboulle/clockwork"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	test.setupLeader(checkTicker, exitTicker)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()
	testKey := "testKey"
	testValue := "testValue"
	err := test.Client.PUT(ctx, testKey, testValue)
	if err != nil {
		t.Errorf("Error putting key: %v", err)
		return
	}

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for nodeID := range test.TestNodes {
				if value, ok := test.KvStores[nodeID].Get(testKey); !ok || value != testValue {
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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	followerID, leaderID := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	test.waitForLeader(leaderID, checkTicker, exitTicker)

	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[followerID])

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()
	testKey := "testKey"
	testValue := "testValue"
	err := test.Client.PUT(ctx, testKey, testValue)
	if err != nil {
		t.Errorf("Error putting key: %v", err)
		return
	}

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for nodeID := range test.TestNodes {
				t.Logf("%s: %v", nodeID, test.KvStores[nodeID].GetData())
				if value, ok := test.KvStores[nodeID].Get(testKey); !ok || value != testValue {
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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderID])

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	test.waitForLeader(leaderID, checkTicker, exitTicker)

	test.MockHTTPRT.SetTransientError(1, http.StatusServiceUnavailable, nil)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()
	testKey := "testKey"
	testValue := "testValue"
	err := test.Client.PUT(ctx, testKey, testValue)
	if err != nil {
		t.Errorf("Error putting key: %v", err)
		return
	}
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	var replicated bool
ReplicationCheck:
	for {
		select {
		case <-checkTicker.Chan():
			replicated = true
			for nodeID := range test.TestNodes {
				if value, ok := test.KvStores[nodeID].Get(testKey); !ok || value != testValue {
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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderID])

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	test.waitForLeader(leaderID, checkTicker, exitTicker)

	test.MockHTTPRT.SetTransientError(3, http.StatusServiceUnavailable, nil)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 5*time.Second)
	defer cancel()
	err := test.Client.PUT(ctx, "testKey", "testValue")
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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderID])

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(2 * time.Second)

	test.waitForLeader(leaderID, checkTicker, exitTicker)

	test.MockHTTPRT.SetDelay(1 * time.Second)
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer cancel()
	err := test.Client.PUT(ctx, "testKey", "testValue")

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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(1 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)

	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderID])
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()
	testKey := "testKey"
	testValue := "testValue"
	err := test.Client.PUT(ctx, testKey, testValue)
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
			for nodeID := range test.TestNodes {
				t.Logf("%s: %v", nodeID, test.KvStores[nodeID].GetData())
				if value, ok := test.KvStores[nodeID].Get(testKey); !ok || value != testValue {
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

	val, err := test.Client.GET(ctx, testKey)
	if val != testValue {
		t.Errorf("Wrong value: %s, expected %s", val, testValue)
	}
	if err != nil {
		t.Errorf("Error getting key: %v", err)
	}
}

func TestClient_Get_NotFound(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	test.waitForLeader(leaderID, checkTicker, exitTicker)

	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer cancel()
	value, err := test.Client.GET(ctx, "missingKey")
	if value != "" {
		t.Error("Error: Expected empty string")
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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)

	_, leaderID := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	test.waitForLeader(leaderID, checkTicker, exitTicker)

	test.Client.leaderAddress.Store("")
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer cancel()
	_, err := test.Client.GET(ctx, "testKey")
	if err != nil {
		if test.Client.leaderAddress.Load() != test.PeerHTTPAddrs[leaderID] {
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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	followerID, leaderID := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	test.waitForLeader(leaderID, checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[followerID])
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer cancel()
	_, err := test.Client.GET(ctx, "testKey")
	if err != nil {
		var redirectErr ErrRedirect
		if test.Client.leaderAddress.Load() != test.PeerHTTPAddrs[leaderID] {
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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	test.waitForLeader(leaderID, checkTicker, exitTicker)

	testKey := "testKey"
	testVal := "testValue"

	putCtx, putCancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer putCancel()
	err := test.Client.PUT(putCtx, testKey, testVal)
	if err != nil {
		t.Fatalf("Error putting key: %v", err)
	}

	test.MockHTTPRT.SetTransientError(2, http.StatusRequestTimeout, nil)
	getCtx, getCancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer getCancel()
	val, err := test.Client.GET(getCtx, testKey)
	if val != testVal {
		t.Errorf("Error: Expected value: %s, got: %s", testVal, val)
	}
	if err != nil {
		t.Fatalf("Error: Fail to get key %s, %s", testKey, err.Error())
	}
}

func TestClient_Get_MaxRetries(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	test.waitForLeader(leaderID, checkTicker, exitTicker)

	key := "testKey"
	value := "testValue"
	putCtx, putCancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer putCancel()
	err := test.Client.PUT(putCtx, key, value)
	if err != nil {
		t.Fatalf("Error putting key: %v", err)
	}

	test.MockHTTPRT.SetTransientError(3, http.StatusRequestTimeout, nil)
	getCtx, getCancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer getCancel()
	val, err := test.Client.GET(getCtx, key)
	if val != "" {
		t.Errorf("Error: Expected no value for key %s, got %s", key, val)
	}
	if err != nil {
		var statusTimeoutErr ErrRequestTimeout
		if errors.As(err, &statusTimeoutErr) {
			if statusTimeoutErr.StatusCode != http.StatusRequestTimeout {
				t.Errorf("Error: Expected StatusRequestTimeout, got %d", statusTimeoutErr.StatusCode)
			}
			if statusTimeoutErr.Key != key {
				t.Errorf("Error: Expected Key %s, got %s", key, val)
			}
		}
	} else {
		t.Fatalf("Error: Invalid error while injecting maximum number of errors")
	}
}

func TestClient_Get_ContextTimeout(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	test.waitForLeader(leaderID, checkTicker, exitTicker)

	key := "testKey"
	value := "testValue"
	putCtx, putCancel := clockwork.WithTimeout(context.Background(), test.Clock, 150*time.Millisecond)
	defer putCancel()
	err := test.Client.PUT(putCtx, key, value)
	if err != nil {
		t.Fatalf("Error putting key: %v", err)
	}

	test.MockHTTPRT.SetDelay(1 * time.Second)
	getCtx, getCancel := clockwork.WithTimeout(context.Background(), test.Clock, 150*time.Millisecond)
	defer getCancel()
	_, err = test.Client.GET(getCtx, key)
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

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)
	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)
	test.waitForLeader(leaderID, checkTicker, exitTicker)

	test.MockHTTPRT.SetTransientError(1, http.StatusNotFound, nil)
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 150*time.Millisecond)
	defer cancel()
	val, err := test.Client.GET(ctx, "testKey")
	if val != "" {
		t.Errorf("Error: Expected value to be empty")
	}
	if err != nil {
		var statusNotFoundError ErrNotFound
		if errors.As(err, &statusNotFoundError) {
			if statusNotFoundError.Key != "testKey" {
				t.Errorf("Error: Expected key to be testKey")
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
