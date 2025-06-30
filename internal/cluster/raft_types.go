package cluster

import (
	"context"
	"net"
)

type RaftState string

type CommandType string

type RaftRPCServer interface {
	AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error)
	Close() error
}

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []*LogEntry
}

type Command struct {
	Type  CommandType
	Key   string
	Value string
}

type PutRequest struct {
	Value string `json:"value"`
}

type ListenerFactory func(address string) (net.Listener, error)

type ProposeRequest struct {
	Command []byte
	errorCh chan error
}

type AppendEntriesRequestWrapper struct {
	Ctx      context.Context
	Request  *AppendEntriesRequest
	Response chan *AppendEntriesResponse
}

type AppendEntriesResponseWrapper struct {
	Response     *AppendEntriesResponse
	Error        error
	PeerID       string
	PrevLogIndex uint64
	SentEntries  []*LogEntry
}

type RequestVoteRequestWrapper struct {
	Ctx      context.Context
	Request  *RequestVoteRequest
	Response chan *RequestVoteResponse
}
