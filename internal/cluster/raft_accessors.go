package cluster

func (n *Node) GetPeers() []string {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.peers
}

func (n *Node) GetCurrentTerm() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.currentTerm
}

func (n *Node) GetVotedFor() string {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.votedFor
}

func (n *Node) GetLog() []*LogEntry {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	logCopy := make([]*LogEntry, 0, len(n.log))
	for _, logEntry := range n.log {
		if logEntry != nil {
			newEntry := &LogEntry{
				Term:    logEntry.Term,
				Index:   logEntry.Index,
				Command: make([]byte, len(logEntry.Command)),
			}
			copy(newEntry.Command, logEntry.Command)
			logCopy = append(logCopy, newEntry)
		}
	}
	return logCopy
}

func (n *Node) GetCommitIndex() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.commitIndex
}

func (n *Node) GetState() RaftState {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.state
}

func (n *Node) GetLastApplied() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.lastApplied
}

func (n *Node) GetLeaderID() string {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.leaderID
}

func (n *Node) GetVotesReceived() map[string]bool {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.votesReceived
}

func (n *Node) GetNextIndex() map[string]uint64 {
	n.rwMu.RLock()
	defer n.rwMu.RUnlock()
	nextIndexCopy := make(map[string]uint64)
	for k, v := range n.nextIndex {
		nextIndexCopy[k] = v
	}
	return nextIndexCopy
}

func (n *Node) GetMatchIndex() map[string]uint64 {
	n.rwMu.RLock()
	defer n.rwMu.RUnlock()
	matchIndexCopy := make(map[string]uint64)
	for k, v := range n.matchIndex {
		matchIndexCopy[k] = v
	}
	return matchIndexCopy
}

func (n *Node) GetDirtyPersistenceState() bool {
	n.rwMu.RLock()
	defer n.rwMu.RUnlock()
	return n.dirtyPersistenceState
}

func (n *Node) GetKVStore() *KVStore {
	return n.kvStore
}

func (n *Node) GetRPCServer() *RaftServer {
	return n.raftServer
}
