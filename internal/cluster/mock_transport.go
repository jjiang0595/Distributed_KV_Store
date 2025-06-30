package cluster

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

type MockNetworkTransport struct {
	networkCtx    context.Context
	networkCancel context.CancelFunc
	rpcServers    map[string]RaftRPCServer
	mu            sync.Mutex
	rwMu          sync.RWMutex
	partitions    map[string]map[string]bool
}

func NewMockNetworkTransport(ctx context.Context) *MockNetworkTransport {
	networkCtx, networkCancel := context.WithCancel(ctx)
	return &MockNetworkTransport{
		networkCtx:    networkCtx,
		networkCancel: networkCancel,
		rpcServers:    make(map[string]RaftRPCServer),
		partitions:    make(map[string]map[string]bool),
	}
}

func (m *MockNetworkTransport) RegisterRPCServer(node *Node, server RaftRPCServer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rpcServers[node.ID] = server
}

func (m *MockNetworkTransport) UnregisterRPCServer(node *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rpcServers, node.ID)
}

func (m *MockNetworkTransport) SetPartition(startId string, endId string, isPartitioned bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.partitions[startId]; !ok {
		m.partitions[startId] = make(map[string]bool)
	}
	m.partitions[startId][endId] = isPartitioned

	if _, ok := m.partitions[endId]; !ok {
		m.partitions[endId] = make(map[string]bool)
	}
	m.partitions[endId][startId] = isPartitioned
}

func (m *MockNetworkTransport) IsPartitioned(startId string, endId string) bool {
	m.rwMu.RLock()
	defer m.rwMu.RUnlock()
	if innerMap, ok := m.partitions[startId]; !ok || innerMap != nil {
		if isPartitioned, ok := innerMap[endId]; ok {
			return isPartitioned
		}
	}
	return false
}

func (m *MockNetworkTransport) SendAppendEntries(ctx context.Context, startId string, endId string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if m.IsPartitioned(startId, endId) {
		return nil, fmt.Errorf("node %s to node %s is partitioned", startId, endId)
	}
	m.mu.Lock()
	rpcServer, ok := m.rpcServers[endId]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%s's rpc server not found", endId)
	}

	res, err := rpcServer.AppendEntries(ctx, req)
	runtime.Gosched()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *MockNetworkTransport) SendRequestVote(ctx context.Context, startId string, endId string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if m.IsPartitioned(startId, endId) {
		return nil, fmt.Errorf("node %s to node %s is partitioned", startId, endId)
	}
	m.mu.Lock()
	rpcServer, ok := m.rpcServers[endId]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%s's rpc server not found", endId)
	}

	res, err := rpcServer.RequestVote(ctx, req)
	runtime.Gosched()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *MockNetworkTransport) Close() error {
	m.networkCancel()
	return nil
}
