package cluster

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strings"
	"sync"
	"time"
)

type NetworkTransport interface {
	SendAppendEntries(ctx context.Context, startId string, endId string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	SendRequestVote(ctx context.Context, startId string, endId string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	Close() error
}

type gRPCTransport struct {
	clients    map[string]RaftServiceClient
	conns      map[string]*grpc.ClientConn
	rpcTimeout time.Duration
	mu         sync.Mutex
	rwMu       sync.RWMutex
}

func NewGRPCTransport(peerAddresses map[string]string) (NetworkTransport, error) {
	t := &gRPCTransport{
		clients:    make(map[string]RaftServiceClient),
		conns:      make(map[string]*grpc.ClientConn),
		rpcTimeout: 100 * time.Millisecond,
	}

	for peerID, peerAddress := range peerAddresses {
		conn, err := grpc.NewClient(fmt.Sprintf("%s", peerAddress), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		t.conns[peerID] = conn
		t.clients[peerID] = NewRaftServiceClient(conn)
	}
	return t, nil
}

func (t *gRPCTransport) SendAppendEntries(ctx context.Context, startId string, endId string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	t.rwMu.RLock()
	client, ok := t.clients[endId]
	t.rwMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("invalid PeerID %s", endId)
	}

	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error sending AppendEntries to %s: %w", endId, err)
	}
	return resp, nil
}

func (t *gRPCTransport) SendRequestVote(ctx context.Context, startId string, endId string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	t.rwMu.RLock()
	client, ok := t.clients[endId]
	t.rwMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("invalid PeerID %s", endId)
	}

	res, err := client.RequestVote(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error sending RequestVote to %s: %w", endId, err)
	}
	return res, nil
}

func (t *gRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	var errs []string
	for _, conn := range t.conns {
		if conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, fmt.Sprintf("error closing connection: %s", err.Error()))
			}
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}
