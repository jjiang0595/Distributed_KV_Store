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

func (n *Node) ReplicateToFollower(stopCtx context.Context, followerID string) {
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
		default:
			n.RaftMu.Lock()
			if n.state != Leader {
				n.RaftMu.Unlock()
				return
			}
			term, leaderID, commitIndex := n.currentTerm, n.leaderID, n.commitIndex
			logSnapshot := make([]*LogEntry, len(n.log))
			copy(logSnapshot, n.log)
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

			log.Printf("Leader %s: ReplicateToFollower sending logs to %v", leaderID, followerID)
			response, err := n.Transport.SendAppendEntries(followerID, &AppendEntriesRequest{
				Term:         term,
				LeaderId:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			})
			if err != nil {
				log.Printf("Error appending raft log: %v", err)
				retryTime = min(maxRetryTime, retryTime*2)
				n.Clock.Sleep(retryTime)
				continue
			}
			wrappedResp := &AppendEntriesResponseWrapper{
				Response:     response,
				Error:        err,
				PeerID:       followerID,
				PrevLogIndex: prevLogIndex,
				SentEntries:  entries,
			}
			n.appendEntriesResponseChan <- wrappedResp

			retryTime = 50 * time.Millisecond
			sleepDuration := 10 * time.Millisecond
			if response.Success {
				sleepDuration = 50 * time.Millisecond
			}
			n.RaftMu.Lock()
			log.Printf("MatchIndex: %v, nextIndex: %v", n.matchIndex[followerID], n.nextIndex[followerID])
			log.Printf("log: %v", n.log)
			log.Printf("Leader %s: Sleep %v", n.ID, sleepDuration)
			n.RaftMu.Unlock()
			n.Clock.Sleep(sleepDuration)
		}
	}
}
