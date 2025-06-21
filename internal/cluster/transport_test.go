package cluster

import (
	"context"
	"fmt"
)

type MockNetworkTransport struct {
	ctx       context.Context
	nodeID    string
	peerNodes map[string]*Node
}

func NewMockNetworkTransport(ctx context.Context, nodeID string, peerNodes map[string]*Node) NetworkTransport {
	return &MockNetworkTransport{
		ctx:       ctx,
		nodeID:    nodeID,
		peerNodes: peerNodes,
	}
}

func (m *MockNetworkTransport) SendAppendEntries(appendCtx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	peer, ok := m.peerNodes[peerID]
	if !ok {
		return nil, fmt.Errorf("peer not found %s", peerID)
	}

	mockServer := NewRaftServer(peer)
	res, err := mockServer.AppendEntries(appendCtx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *MockNetworkTransport) SendRequestVote(requestCtx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	peer, ok := m.peerNodes[peerID]
	if !ok {
		return nil, fmt.Errorf("peer not found %s", peerID)
	}

	mockServer := NewRaftServer(peer)
	res, err := mockServer.RequestVote(requestCtx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *MockNetworkTransport) Close() error {
	return nil
}
