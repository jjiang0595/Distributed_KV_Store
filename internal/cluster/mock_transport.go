package cluster

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

type MockNetworkTransport struct {
	ctx       context.Context
	nodeID    string
	peerNodes map[string]*Node

	mu               sync.Mutex
	partitionedNodes map[string]struct{}
}

func NewMockNetworkTransport(ctx context.Context, nodeID string, peerNodes map[string]*Node) NetworkTransport {
	return &MockNetworkTransport{
		ctx:              ctx,
		nodeID:           nodeID,
		peerNodes:        peerNodes,
		partitionedNodes: make(map[string]struct{}),
	}
}

func (m *MockNetworkTransport) PartitionNode(nodeID string, isPartitioned bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if isPartitioned {
		m.partitionedNodes[nodeID] = struct{}{}
	} else {
		delete(m.partitionedNodes, nodeID)
	}
}

func (m *MockNetworkTransport) SendAppendEntries(appendCtx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	m.mu.Lock()
	_, isPartitioned := m.partitionedNodes[peerID]
	m.mu.Unlock()

	if isPartitioned {
		return nil, fmt.Errorf("node %s is partitioned", peerID)
	}
	peer, ok := m.peerNodes[peerID]
	if !ok {
		return nil, fmt.Errorf("peer not found %s", peerID)
	}

	mockServer := NewRaftServer(peer)
	res, err := mockServer.AppendEntries(appendCtx, req)
	runtime.Gosched()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *MockNetworkTransport) SendRequestVote(requestCtx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	m.mu.Lock()
	_, isPartitioned := m.partitionedNodes[peerID]
	m.mu.Unlock()

	if isPartitioned {
		return nil, fmt.Errorf("node %s is partitioned", peerID)
	}
	peer, ok := m.peerNodes[peerID]
	if !ok {
		return nil, fmt.Errorf("peer not found %s", peerID)
	}

	mockServer := NewRaftServer(peer)
	res, err := mockServer.RequestVote(requestCtx, req)
	runtime.Gosched()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *MockNetworkTransport) Close() error {
	return nil
}
