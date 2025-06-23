package cluster

import (
	"context"
	"log"
	"time"
)

func (n *Node) StartReplicators() {
	log.Printf("Starting replicators...")
	n.replicatorCancel = make(map[string]context.CancelFunc)

	for _, peerID := range n.peers {
		if n.ID == peerID {
			continue
		}
		replicatorCtx, replicatorCancel := context.WithCancel(n.ctx)
		n.replicatorCancel[peerID] = replicatorCancel
		n.replicatorWg.Add(1)
		log.Printf("Leader %s: Starting replicator for peer %s", n.ID, peerID)
		go n.ReplicateToFollower(replicatorCtx, peerID)
	}
}

func (n *Node) StopReplicators() {
	for _, replicatorCancel := range n.replicatorCancel {
		replicatorCancel()
	}
	n.replicatorCancel = nil
}

func (n *Node) sendAppendEntriesToPeers(stopCtx context.Context, followerID string) (bool, error) {
	n.RaftMu.Lock()
	term, leaderID, commitIndex := n.currentTerm, n.leaderID, n.commitIndex
	logSnapshot := make([]*LogEntry, 0, len(n.log))
	for _, entry := range n.log {
		if entry != nil {
			logEntry := &LogEntry{
				Term:    entry.Term,
				Index:   entry.Index,
				Command: make([]byte, len(entry.Command)),
			}
			copy(logEntry.Command, entry.Command)
			logSnapshot = append(logSnapshot, logEntry)
		}
	}
	peerNextIndex, ok := n.nextIndex[followerID]
	if !ok {
		peerNextIndex = 1
	}
	n.RaftMu.Unlock()
	prevLogIndex, prevLogTerm := uint64(0), uint64(0)
	if peerNextIndex > 1 {
		if peerNextIndex-2 < uint64(len(logSnapshot)) {
			prevLogIndex = peerNextIndex - 1
			prevLogTerm = logSnapshot[peerNextIndex-2].Term
		} else {
			log.Print("Indexes out of bound. Resetting to prevLogIndex & prevLogTerm to 0")
			prevLogIndex = 0
			prevLogTerm = 0
		}
	}

	entries := make([]*LogEntry, 0)

	if peerNextIndex > 0 && peerNextIndex <= uint64(len(logSnapshot)) {
		entries = logSnapshot[peerNextIndex-1:]
	} else if len(logSnapshot) > 0 && peerNextIndex == 0 {
		entries = logSnapshot
	} else if len(logSnapshot) > 0 && peerNextIndex > uint64(len(logSnapshot)) {
		entries = []*LogEntry{}
	}
	replicateCtx, replicateCancel := context.WithTimeout(stopCtx, 200*time.Millisecond)
	//log.Printf("Leader %s: ReplicateToFollower sending logs to %v at time %v", leaderID, followerID, n.Clock.Now())
	response, err := n.Transport.SendAppendEntries(replicateCtx, followerID, &AppendEntriesRequest{
		Term:         term,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	})
	replicateCancel()
	if err != nil {
		return false, nil
	}
	wrappedResp := &AppendEntriesResponseWrapper{
		Response:     response,
		Error:        err,
		PeerID:       followerID,
		PrevLogIndex: prevLogIndex,
		SentEntries:  entries,
	}
	select {
	case <-stopCtx.Done():
		return response.GetSuccess(), nil
	case n.appendEntriesResponseChan <- wrappedResp:
		log.Printf("Leader %v: Processing Replication Response", n.ID)
	}
	return response.GetSuccess(), nil
}

	defer func() {
		n.replicatorWg.Done()
		log.Printf("Node %s: Replicator goroutine stopped", n.ID)
	}()
	retryTime := time.Millisecond * 50
	maxRetryTime := time.Second * 2

	for {
		select {
		case <-n.ctx.Done():
			log.Printf("Leader %s: ReplicateToFollower stopped", n.ID)
			return

			}
				retryTime = min(maxRetryTime, retryTime*2)
			}
		}
	}
}
