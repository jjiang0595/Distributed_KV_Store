package client

import (
	"context"
	"github.com/jonboulle/clockwork"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
			for nodeID, _ := range test.TestNodes {
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
	t.Logf("Success: Client PUT request replicated")
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
			for nodeID, _ := range test.TestNodes {
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
	t.Logf("Success: Successfully redirected & client PUT request replicated")
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
			for nodeID, _ := range test.TestNodes {
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
	t.Logf("Success: Handled errors & Client PUT request replicated")
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
		if strings.Contains(err.Error(), "Service Unavailable") {
			t.Logf("Success: Graceful completion of maximum retries")
			return
		}
		t.Errorf("Error putting key: %v", err)
		return
	}
}

func TestClient_Put_ContextTimeout(t *testing.T) {
	test := testSetup(t)
	test.Client = NewClient(test.PeerHTTPAddrs, WithMaxRetries(1), WithTimeout(100*time.Millisecond), WithHTTPTransport(test.MockHTTPRT))

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(2 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)
	test.Client.leaderAddress.Store(test.PeerHTTPAddrs[leaderID])

	checkTicker.Reset(50 * time.Millisecond)
	exitTicker.Reset(5 * time.Second)

	test.waitForLeader(leaderID, checkTicker, exitTicker)

	test.MockHTTPRT.SetDelay(200 * time.Millisecond)
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 100*time.Millisecond)
	defer cancel()
	err := test.Client.PUT(ctx, "testKey", "testValue")

	if err != nil {
		log.Printf("Error putting key: %v", err.Error())
		if strings.Contains(err.Error(), "context deadline exceeded") {
			t.Logf("Success: Context timeout exceeded")
			return
		}
		t.Fatal("Error: Wrong error code")
	}
	t.Fatal("Error: No response from server to context timeout exceeded")

}

func TestClient_Get_Success(t *testing.T) {
	test := testSetup(t)
	defer test.cleanup()

	test.Client = NewClient(test.PeerHTTPAddrs, WithTimeout(5*time.Second), WithMaxRetries(3), WithHTTPTransport(test.MockHTTPRT))
	c := test.Client

	checkTicker := test.Clock.NewTicker(50 * time.Millisecond)
	exitTicker := test.Clock.NewTicker(1 * time.Second)
	_, leaderID := test.setupLeader(checkTicker, exitTicker)

	c.leaderAddress.Store(test.PeerHTTPAddrs[leaderID])
	ctx, cancel := clockwork.WithTimeout(context.Background(), test.Clock, 3*time.Second)
	defer cancel()
	testKey := "testKey"
	testValue := "testValue"
	err := c.PUT(ctx, testKey, testValue)
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
			for nodeID, _ := range test.TestNodes {
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

	if val, ok := c.GET(ctx, testKey); ok == nil && (val == testValue) {
		t.Logf("Success: Retreived correct GET value. Key %s -> %s", testKey, val)
		return
	}
	t.Fatalf("Error: Invalid or missing value")
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
	if value, err := test.Client.GET(ctx, "missingKey"); value == "" && err != nil {
		if err.Error() != "" && strings.Contains(err.Error(), "Resource Not Found") {
			t.Logf("Error getting key: %v", err.Error())
			t.Logf("Success: Handled missing key successfully")
			return
		}
	}
	t.Fatalf("Error: Unexpected error while handling missing key")
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
		if err.Error() != "" && strings.Contains(err.Error(), "Resource Not Found") && test.Client.leaderAddress.Load() == test.PeerHTTPAddrs[leaderID] {
			t.Logf("Success: Correctly updated leader address")
			return
		}
	}
	t.Fatalf("Error: Didn't update leader address")
}

}
