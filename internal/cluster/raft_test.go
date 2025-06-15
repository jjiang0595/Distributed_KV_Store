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
}

func testSetup(t *testing.T) (context.Context, context.CancelFunc, map[string]*Node, *clockwork.FakeClock) {
	ctx, cancel := context.WithCancel(context.Background())
	clk := clockwork.NewFakeClock()

	nodeIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		nodeIDs[i] = fmt.Sprintf("node%d", i)
	}

	testNodes := make(map[string]*Node)
	for i := 0; i < 3; i++ {
		mockTransport := NewMockNetworkTransport(nodeIDs[i], testNodes)
		newNode := newTestNode(t, ctx, cancel, nodeIDs[i], "localhost:", 0, 0, t.TempDir(), filterSelfID(nodeIDs[i], nodeIDs), clk, mockTransport)
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
