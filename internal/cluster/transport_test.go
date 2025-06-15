package cluster

import (
	"context"
	"fmt"
)

type MockNetworkTransport struct {
	nodeID    string
	peerNodes map[string]*Node
}

func NewMockNetworkTransport(nodeID string, peerNodes map[string]*Node) NetworkTransport {
	return &MockNetworkTransport{
		nodeID:    nodeID,
		peerNodes: peerNodes,
	}
}

func (m *MockNetworkTransport) SendAppendEntries(peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	peer, ok := m.peerNodes[peerID]
	if !ok {
		return nil, fmt.Errorf("peer not found %s", peerID)
	}

	mockServer := NewRaftServer(peer)
	res, err := mockServer.ProcessAppendEntriesRequest(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *MockNetworkTransport) SendRequestVote(peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	peer, ok := m.peerNodes[peerID]
	if !ok {
		return nil, fmt.Errorf("peer not found %s", peerID)
	}

	mockServer := NewRaftServer(peer)
	res, err := mockServer.ProcessVoteRequest(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *MockNetworkTransport) Close() error {
	return nil
}
