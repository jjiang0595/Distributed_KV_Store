package cluster

import (
	"context"
	"fmt"
	"log"
	"time"
)

func (n *Node) StartReplicators() {
	log.Printf("Starting replicators at time %v", n.Clock.Now())
	n.RaftMu.Lock()
	n.replicatorCancel = make(map[string]context.CancelFunc)
	n.replicatorSendNowChan = make(map[string]chan struct{})
	n.RaftMu.Unlock()

	for _, peerID := range n.peers {
		n.RaftMu.Lock()
		replicatorCtx, replicatorCancel := context.WithCancel(n.ctx)
		n.replicatorCancel[peerID] = replicatorCancel
		n.replicatorSendNowChan[peerID] = make(chan struct{}, 1)
		n.replicatorWg.Add(1)
		n.RaftMu.Unlock()
		go n.replicateToFollower(replicatorCtx, peerID)
	}
}

func (n *Node) StopReplicators() {
	for _, replicatorCancel := range n.replicatorCancel {
		replicatorCancel()
	}
	n.replicatorSendNowChan = nil
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
	n.RaftMu.Unlock()
	if !ok {
		if len(logSnapshot) == 0 {
			peerNextIndex = 1
		} else {
			peerNextIndex = logSnapshot[len(logSnapshot)-1].Index + 1
		}
	}

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
	log.Printf("peerNextIndex: %v, log length: %v", peerNextIndex, len(logSnapshot))
	if peerNextIndex > uint64(len(logSnapshot)) {
		entries = []*LogEntry{}
	} else {
		entries = logSnapshot[peerNextIndex-1:]
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
		return false, fmt.Errorf("error sending append entries request to follower: %v", err)
	}
	wrappedResp := &AppendEntriesResponseWrapper{
		Response:     response,
		Error:        err,
		PeerID:       followerID,
		PrevLogIndex: prevLogIndex,
		SentEntries:  entries,
	}
	select {
	case <-n.ctx.Done():
		return response.GetSuccess(), nil
	case n.appendEntriesResponseChan <- wrappedResp:
		log.Printf("Leader %v: Processing Replication Response", n.ID)
	}
	return response.GetSuccess(), nil
}

func (n *Node) replicateToFollower(stopCtx context.Context, followerID string) {
	defer func() {
		n.replicatorWg.Done()
	}()
	retryTime := time.Millisecond * 10
	maxRetryTime := time.Second * 2
	retryTimer := n.Clock.NewTimer(retryTime)
	retryTimer.Stop()

	ticker := n.Clock.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.RaftMu.Lock()
		replicatorChan, ok := n.replicatorSendNowChan[followerID]
		n.RaftMu.Unlock()
		if !ok {
			return
		}

		select {
		case <-stopCtx.Done():
			log.Printf("Node %s: Replicator goroutine stopped", n.ID)
			return
		case <-replicatorChan:
			log.Printf("Node %s: Immediate retry", n.ID)
			ticker.Stop()
			retryTimer.Stop()

			success, err := n.sendAppendEntriesToPeers(stopCtx, followerID)
			if err != nil || !success {
				ticker.Stop()
				retryTimer.Reset(retryTime)
			} else {
				retryTimer.Stop()
				retryTimer.Reset(retryTime)
			}

		case <-ticker.Chan():
			log.Printf("Node %s: Replicator goroutine tick", n.ID)
			_, err := n.sendAppendEntriesToPeers(stopCtx, followerID)
			if err != nil {
				ticker.Stop()
				retryTimer.Reset(retryTime)
			} else {
				retryTimer.Stop()
			}

		case <-retryTimer.Chan():
			log.Printf("Node %s: Retry Append Entries", n.ID)
			_, err := n.sendAppendEntriesToPeers(stopCtx, followerID)
			if err != nil {
				retryTime = min(maxRetryTime, retryTime*2)
				retryTimer.Reset(retryTime)
			} else {
				retryTime = time.Millisecond * 25
				ticker = n.Clock.NewTicker(retryTime)
				retryTimer.Stop()
			}
		}
	}
}
