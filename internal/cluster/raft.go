package cluster

import (
	"context"
	"distributed_kv_store/internal/cluster/types"
	"log"
)

type RaftServer struct {
	UnimplementedRaftServiceServer
	mainNode *types.Node
}

func NewRaftServer(mainNode *types.Node) *RaftServer {
	return &RaftServer{
		mainNode: mainNode,
	}
}

func (s *RaftServer) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	s.mainNode.RaftMu.Lock()
	defer s.mainNode.RaftMu.Unlock()
	if req.Term > s.mainNode.CurrentTerm {
		s.mainNode.VotedFor = ""
		s.mainNode.CurrentTerm = req.Term
		err := s.mainNode.SaveRaftState()
		if err != nil {
			return nil, err
		}
		s.mainNode.State = types.Follower
	}
	var lastLogTerm uint64 = 0
	if len(s.mainNode.Log) > 0 {
		lastLogTerm = s.mainNode.Log[len(s.mainNode.Log)-1].Term
	}
	var lastLogIndex uint64 = 0
	if len(s.mainNode.Log) > 0 {
		lastLogIndex = s.mainNode.Log[len(s.mainNode.Log)-1].Index
	}

	logOk := (req.LastLogTerm > lastLogTerm) ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if req.Term == s.mainNode.CurrentTerm && logOk && (s.mainNode.VotedFor == req.CandidateId || s.mainNode.VotedFor == "") {
		s.mainNode.VotedFor = req.CandidateId
		err := s.mainNode.SaveRaftState()
		if err != nil {
			return nil, err
		}
		return &RequestVoteResponse{Term: s.mainNode.CurrentTerm, VoteGranted: true, VoterId: req.CandidateId}, nil
	}
	return &RequestVoteResponse{Term: s.mainNode.CurrentTerm, VoteGranted: false, VoterId: req.CandidateId}, nil
}

func (s *RaftServer) ReceiveVote(req *RequestVoteResponse) {
	s.mainNode.RaftMu.Lock()
	defer s.mainNode.RaftMu.Unlock()
	term, voteGranted, voterId := req.Term, req.VoteGranted, req.VoterId
	if term > s.mainNode.CurrentTerm {
		s.mainNode.VotedFor = ""
		s.mainNode.CurrentTerm = req.Term
		s.mainNode.State = types.Follower
		s.mainNode.LeaderID = ""
		if err := s.mainNode.SaveRaftState(); err != nil {
			log.Fatalf("error saving raft state: %v", err)
		}
		return
	}

	if term == s.mainNode.CurrentTerm && voteGranted {
		s.mainNode.VotesReceived[voterId] = true
		log.Printf("Received vote from follower %s to leader %s", voterId, s.mainNode.LeaderID)
	}
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	s.mainNode.RaftMu.Lock()
	defer s.mainNode.RaftMu.Unlock()

	if req.Term > s.mainNode.CurrentTerm {
		s.mainNode.VotedFor = ""
		s.mainNode.CurrentTerm = req.Term
		err := s.mainNode.SaveRaftState()
		if err != nil {
			return nil, err
		}
		s.mainNode.State = types.Follower
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < s.mainNode.CurrentTerm {
		return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if req.PrevLogIndex > uint64(len(s.mainNode.Log)) {
		log.Printf("Missing entries in the %s's log, append canceled from %s", req.GetLeaderId(), s.mainNode.ID)
		return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
	}
	if req.PrevLogIndex > 0 && req.GetPrevLogTerm() != s.mainNode.Log[req.PrevLogIndex-1].Term {
		log.Printf("PrevLogTerm of receiver %s doesn't match PrevLogTerm of sender %s", req.GetLeaderId(), s.mainNode.ID)
		return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
	}

	if uint64(len(req.Entries)) > 0 && uint64(len(s.mainNode.Log)) > req.GetPrevLogIndex() {
		// Check for a term mismatch at the index, and if so, truncate
		index := min(uint64(len(req.Entries))+req.GetPrevLogIndex(), uint64(len(s.mainNode.Log))) - 1
		if s.mainNode.Log[index].Term != req.Entries[index-req.PrevLogIndex].Term {
			s.mainNode.Log = s.mainNode.Log[:req.PrevLogIndex-1]
		}
	}

	originalLength := len(s.mainNode.Log)
	// Find and truncate if a potential conflicting entry is found, otherwise append all entries
	for i, entry := range req.Entries {
		leaderIndex := req.PrevLogIndex + uint64(i) + 1

		if leaderIndex > uint64(len(s.mainNode.Log)) {
			s.mainNode.Log = append(s.mainNode.Log, entry)
		} else {
			if s.mainNode.Log[leaderIndex-1].Term != entry.Term {
				s.mainNode.Log = s.mainNode.Log[:leaderIndex-1]
			}
			s.mainNode.Log = append(s.mainNode.Log, entry)
		}
	}
	err := s.mainNode.AppendLogEntries(s.mainNode.Log[originalLength-1:])
	if err != nil {
		log.Printf("AppendLogEntries failed: %v", err)
		return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: false}, nil
	}

	if req.LeaderCommit > s.mainNode.CommitIndex {
		lastEntryIndex := uint64(0)
		if len(s.mainNode.Log) > 0 {
			lastEntryIndex = s.mainNode.Log[len(s.mainNode.Log)-1].Index
		}
		s.mainNode.CommitIndex = min(req.LeaderCommit, lastEntryIndex)
	}
	return &AppendEntriesResponse{Term: s.mainNode.CurrentTerm, Success: true}, nil
}
