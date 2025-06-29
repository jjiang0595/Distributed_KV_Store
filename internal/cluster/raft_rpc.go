package cluster

import (
	"context"
	"log"
)

func (s *RaftServer) ProcessAppendEntriesRequest(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	//log.Printf("Processing AppendEntries Request at time %v", s.mainNode.Clock.Now())
	s.mainNode.RaftMu.Lock()
	defer s.mainNode.RaftMu.Unlock()

	oldTerm, oldVotedFor := s.mainNode.currentTerm, s.mainNode.votedFor
	oldLogLength := len(s.mainNode.log)

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < s.mainNode.currentTerm {
		return &AppendEntriesResponse{Term: s.mainNode.currentTerm, Success: false}, nil
	}

	if req.Term > s.mainNode.currentTerm {
		s.mainNode.votedFor = ""
		s.mainNode.currentTerm = req.Term
		s.mainNode.state = Follower
		s.mainNode.votesReceived = make(map[string]bool)
		s.mainNode.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
	}

	select {
	case s.mainNode.resetElectionTimeoutChan <- struct{}{}:
		log.Printf("Sending Resetting Election timeout")
	default:
		log.Printf("Election timeout channel full")
	}
	s.mainNode.leaderID = req.LeaderId

	if s.mainNode.state == Candidate {
		s.mainNode.currentTerm = req.Term
		s.mainNode.votedFor = ""
		s.mainNode.votesReceived = make(map[string]bool)
		s.mainNode.state = Follower
		s.mainNode.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if req.PrevLogIndex > uint64(len(s.mainNode.log)) {
		log.Printf("Missing entries in the %s's log, append canceled from %s", req.GetLeaderId(), s.mainNode.ID)
		return &AppendEntriesResponse{Term: s.mainNode.currentTerm, Success: false}, nil
	}
	if req.PrevLogIndex > 0 && req.GetPrevLogTerm() != s.mainNode.log[req.PrevLogIndex-1].Term {
		log.Printf("PrevlogTerm of receiver %s doesn't match PrevlogTerm of sender %s", req.GetLeaderId(), s.mainNode.ID)
		return &AppendEntriesResponse{Term: s.mainNode.currentTerm, Success: false}, nil
	}

	// Find and truncate if a potential conflicting entry is found. Then append all entries
	if len(req.Entries) == 0 {
		log.Printf("Node %s: Received heartbeat from leader %s", s.mainNode.ID, s.mainNode.leaderID)
	} else {
		log.Printf("Entries: %v", req.Entries)
	}
	for i, entry := range req.Entries {
		leaderIndex := req.PrevLogIndex + uint64(i) + 1

		followerIndex := leaderIndex - 1
		log.Printf("%v, %v", leaderIndex, followerIndex)
		if followerIndex < uint64(len(s.mainNode.log)) && s.mainNode.log[followerIndex].Term != entry.Term {
			s.mainNode.log = s.mainNode.log[:followerIndex]
			s.mainNode.log = append(s.mainNode.log, req.Entries[i:]...)
			s.mainNode.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
			break
		}
		if followerIndex >= uint64(len(s.mainNode.log)) {
			s.mainNode.log = append(s.mainNode.log, req.Entries[i:]...)
			s.mainNode.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
			break
		}
	}
	log.Printf("Leader Commit: %v, commitIndex: %v", req.LeaderCommit, s.mainNode.commitIndex)

	if req.LeaderCommit > s.mainNode.commitIndex {
		lastEntryIndex := uint64(0)
		if len(s.mainNode.log) > 0 {
			lastEntryIndex = s.mainNode.log[len(s.mainNode.log)-1].Index
		}
		s.mainNode.commitIndex = min(req.LeaderCommit, lastEntryIndex)
		s.mainNode.applierCond.Broadcast()
	}
	return &AppendEntriesResponse{Term: s.mainNode.currentTerm, Success: true}, nil
}

func (s *RaftServer) ProcessVoteRequest(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	//log.Printf("Candidate %s: Processing vote request for %s at time %v", req.CandidateId, s.mainNode.ID, s.mainNode.Clock.Now())
	s.mainNode.RaftMu.Lock()
	defer s.mainNode.RaftMu.Unlock()

	oldTerm, oldVotedFor := s.mainNode.currentTerm, s.mainNode.votedFor
	oldLogLength := len(s.mainNode.log)

	if req.Term < s.mainNode.currentTerm {
		return &RequestVoteResponse{Term: s.mainNode.currentTerm, VoteGranted: false, VoterId: s.mainNode.ID}, nil
	}

	if req.Term > s.mainNode.currentTerm {
		s.mainNode.votedFor = ""
		s.mainNode.currentTerm = req.Term
		s.mainNode.state = Follower
		s.mainNode.votesReceived = make(map[string]bool)
		s.mainNode.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
	}
	select {
	case s.mainNode.resetElectionTimeoutChan <- struct{}{}:
		log.Printf("Receiver %s: Sent to Election Timeout Buffer at time %v", s.mainNode.ID, s.mainNode.Clock.Now())
	default:
		log.Printf("Election timeout channel full")
	}

	var lastLogTerm uint64 = 0
	if len(s.mainNode.log) > 0 {
		lastLogTerm = s.mainNode.log[len(s.mainNode.log)-1].Term
	}
	var lastLogIndex uint64 = 0
	if len(s.mainNode.log) > 0 {
		lastLogIndex = s.mainNode.log[len(s.mainNode.log)-1].Index
	}

	logOk := (req.LastLogTerm > lastLogTerm) ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if req.Term == s.mainNode.currentTerm && logOk && (s.mainNode.votedFor == req.CandidateId || s.mainNode.votedFor == "") {
		s.mainNode.votedFor = req.CandidateId
		s.mainNode.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
		select {
		case s.mainNode.resetElectionTimeoutChan <- struct{}{}:
			log.Printf("Receiver %s: Sent to Election Timeout Buffer", s.mainNode.ID)
		default:
			log.Printf("Election timeout channel full")
		}
		return &RequestVoteResponse{Term: s.mainNode.currentTerm, VoteGranted: true, VoterId: s.mainNode.ID}, nil
	}
	return &RequestVoteResponse{Term: s.mainNode.currentTerm, VoteGranted: false, VoterId: s.mainNode.ID}, nil
}

func (s *RaftServer) ReceiveVote(req *RequestVoteResponse) {
	s.mainNode.RaftMu.Lock()
	defer s.mainNode.RaftMu.Unlock()
	candidateTerm, voterTerm, voteGranted := s.mainNode.currentTerm, req.Term, req.VoteGranted
	oldTerm, oldVotedFor := s.mainNode.currentTerm, s.mainNode.votedFor
	oldLogLength := len(s.mainNode.log)

	if voterTerm > candidateTerm {
		s.mainNode.votedFor = ""
		s.mainNode.currentTerm = voterTerm
		s.mainNode.state = Follower
		s.mainNode.votesReceived = make(map[string]bool)
		select {
		case s.mainNode.resetElectionTimeoutChan <- struct{}{}:
			log.Printf("Sending Resetting Election timeout")
		default:
			log.Printf("Election timeout channel full")
		}
		s.mainNode.SendPersistRaftStateRequest(oldTerm, oldVotedFor, oldLogLength)
		return
	}

	if candidateTerm == voterTerm && voteGranted {
		s.mainNode.votesReceived[req.VoterId] = true
		log.Printf("Node %s: Received vote from %s. Total of %v votes, %v", s.mainNode.ID, req.VoterId, len(s.mainNode.votesReceived), s.mainNode.votesReceived)
	}
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	respChan := make(chan *AppendEntriesResponse, 1)
	wrapper := &AppendEntriesRequestWrapper{
		Ctx:      ctx,
		Request:  req,
		Response: respChan,
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s.mainNode.appendEntriesChan <- wrapper:
		log.Printf("Node %s: Sent AppendEntries wrapper to RPC handler at time %v", req.LeaderId, s.mainNode.Clock.Now())
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-respChan:
		return resp, nil
	}
}

func (s *RaftServer) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	respChan := make(chan *RequestVoteResponse, 1)
	wrapper := &RequestVoteRequestWrapper{
		Ctx:      ctx,
		Request:  req,
		Response: respChan,
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s.mainNode.requestVoteChan <- wrapper:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-respChan:
		return resp, nil
	}
}
