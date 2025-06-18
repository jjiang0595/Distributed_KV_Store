package cluster

func (n *Node) GetData() map[string][]byte {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	dataCopy := make(map[string][]byte)
	for k, v := range n.data {
		dataCopy[k] = v
	}
	return dataCopy
}

func (n *Node) GetPeers() []string {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	return n.peers
}

func (n *Node) GetCurrentTerm() uint64 {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	return n.currentTerm
}

func (n *Node) GetVotedFor() string {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	return n.votedFor
}

func (n *Node) GetLog() []*LogEntry {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	logCopy := make([]*LogEntry, len(n.log))
	copy(logCopy, n.log)
	return logCopy
}

func (n *Node) GetCommitIndex() uint64 {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	return n.commitIndex
}

func (n *Node) GetState() RaftState {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	return n.state
}

func (n *Node) GetLastApplied() uint64 {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	return n.lastApplied
}

func (n *Node) GetLeaderID() string {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	return n.leaderID
}

func (n *Node) GetVotesReceived() map[string]bool {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	return n.votesReceived
}

func (n *Node) GetNextIndex() map[string]uint64 {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	nextIndexCopy := make(map[string]uint64)
	for k, v := range n.nextIndex {
		nextIndexCopy[k] = v
	}
	return nextIndexCopy
}

func (n *Node) GetMatchIndex() map[string]uint64 {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	matchIndexCopy := make(map[string]uint64)
	for k, v := range n.matchIndex {
		matchIndexCopy[k] = v
	}
	return matchIndexCopy
}

func (n *Node) GetDirtyPersistenceState() bool {
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()
	return n.dirtyPersistenceState
}
