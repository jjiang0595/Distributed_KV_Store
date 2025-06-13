package cluster

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strings"
	"time"
)

type Transport interface {
	SendAppendEntries(peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	SendRequestVote(peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	Close() error
}

type gRPCTransport struct {
	clients    map[string]RaftServiceClient
	conns      map[string]*grpc.ClientConn
	rpcTimeout time.Duration
}

func NewGRPCTransport(peerAddresses map[string]string) (Transport, error) {
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

func (t *gRPCTransport) SendAppendEntries(peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	client, ok := t.clients[peerID]
	if !ok {
		return nil, fmt.Errorf("invalid PeerID %s", peerID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.rpcTimeout)
	defer cancel()

	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error sending AppendEntries to %s: %w", peerID, err)
	}
	return resp, nil
}

func (t *gRPCTransport) SendRequestVote(peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	client, ok := t.clients[peerID]
	if !ok {
		return nil, fmt.Errorf("invalid PeerID %s", peerID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.rpcTimeout)
	defer cancel()

	res, err := client.RequestVote(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error sending RequestVote to %s: %w", peerID, err)
	}
	return res, nil
}

func (t *gRPCTransport) Close() error {
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
