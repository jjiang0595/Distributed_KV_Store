package cluster

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
)

func (n *Node) StartReplicators() {
	log.Printf("Starting replicators at time %v", n.Clock.Now())
	n.raftMu.Lock()
	n.replicatorCancel = make(map[string]context.CancelFunc)
	n.replicatorSendNowChan = make(map[string]chan struct{})
	n.raftMu.Unlock()

	for _, peerID := range n.peers {
		n.raftMu.Lock()
		replicatorCtx, replicatorCancel := context.WithCancel(n.ctx)
		n.replicatorCancel[peerID] = replicatorCancel
		ch := make(chan struct{}, 1)
		n.replicatorSendNowChan[peerID] = ch
		n.replicatorWg.Add(1)
		n.raftMu.Unlock()
		go n.replicateToFollower(replicatorCtx, peerID, ch)
	}
}

func (n *Node) StopReplicators() {
	if n.replicatorCancel == nil {
		return
	}
	for _, replicatorCancel := range n.replicatorCancel {
		replicatorCancel()
	}
	n.replicatorCancel = nil
}

func (n *Node) sendAppendEntriesToPeers(stopCtx context.Context, followerID string) (bool, error) {
	logSnapshot := n.GetLog()
	n.raftMu.Lock()
	term, leaderID, commitIndex := n.currentTerm, n.leaderID, n.commitIndex
	peerNextIndex, ok := n.nextIndex[followerID]
	n.raftMu.Unlock()
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
	defer replicateCancel()
	//log.Printf("Leader %s: ReplicateToFollower sending logs to %v at time %v", leaderID, followerID, n.Clock.Now())
	response, err := n.Transport.SendAppendEntries(replicateCtx, n.ID, followerID, &AppendEntriesRequest{
		Term:         term,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	})
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

func (n *Node) replicateToFollower(stopCtx context.Context, followerID string, replicatorSendNowChan chan struct{}) {
	defer func() {
		n.replicatorWg.Done()
	}()
	followerOnline := true

	baseRetryTime := time.Millisecond * 150
	currentRetryTime := baseRetryTime
	maxRetryTime := time.Second * 3
	retryTimer := n.Clock.NewTimer(baseRetryTime)
	retryTimer.Stop()

	ticker := n.Clock.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stopCtx.Done():
			retryTimer.Stop()
			log.Printf("Node %s: Replicator goroutine stopped", n.ID)
			return

		case <-replicatorSendNowChan:
			log.Printf("Node %s: Immediate retry", n.ID)
			_, err := n.sendAppendEntriesToPeers(stopCtx, followerID)
			if err != nil {
				followerOnline = false
				ticker.Stop()
				retryTimer.Reset(50 * time.Millisecond)
			} else {
				followerOnline = true
				ticker.Reset(50 * time.Millisecond)
				retryTimer.Stop()
				if !retryTimer.Stop() {
					<-retryTimer.Chan()
				}
			}

		case <-ticker.Chan():
			if followerOnline {
				_, err := n.sendAppendEntriesToPeers(stopCtx, followerID)
				if err != nil {
					followerOnline = false
					ticker.Stop()
					retryTimer.Reset(baseRetryTime)
				} else {
					followerOnline = true
				}
			}

		case <-retryTimer.Chan():
			if !followerOnline {
				_, err := n.sendAppendEntriesToPeers(stopCtx, followerID)
				if err != nil {
					followerOnline = false
					currentRetryTime = min(maxRetryTime, (currentRetryTime*2)+time.Duration(rand.Int63n(int64(100*time.Millisecond))))
					retryTimer.Reset(currentRetryTime)
				} else {
					followerOnline = true
					currentRetryTime = baseRetryTime
					retryTimer.Stop()
					ticker.Reset(50 * time.Millisecond)
					if !retryTimer.Stop() {
						<-retryTimer.Chan()
					}
				}
			}
		}
	}
}
