package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func (n *Node) LoadRaftState() error {
	filePath := filepath.Join(n.dataDir, "raft_state.gob")
	info, err := os.Stat(filePath)

	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Raft state file not found for n %s", n.ID)
			return nil
		}
		return fmt.Errorf("error stating raft state file %s", filePath)
	}

	if info.Size() == 0 {
		log.Printf("Node %s: Raft State File Empty", n.ID)
		return nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("raft state file not found for n %s", n.ID)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()
	decoder := gob.NewDecoder(file)
	var savedState PersistentState
	if err := decoder.Decode(&savedState); err != nil {
		return fmt.Errorf("error decoding raft state: %v", err)
	}
	log.Printf("Saved State: %v", savedState)
	n.RaftMu.Lock()
	n.currentTerm = savedState.CurrentTerm
	n.votedFor = savedState.VotedFor
	n.log = savedState.Log
	n.RaftMu.Unlock()
	return nil
}

func (n *Node) SendPersistRaftStateRequest(oldTerm uint64, oldVotedFor string, oldLogLength int) {
	if !n.dirtyPersistenceState {
		if oldTerm != n.currentTerm || oldVotedFor != n.votedFor || oldLogLength != len(n.log) {
			n.dirtyPersistenceState = true
			select {
			case n.persistStateChan <- struct{}{}:
				log.Printf("Persisting %s's state to persistent state...", n.ID)
			default:
				log.Printf("Persistence channel is full.")
			}
		}
	}
}

func (n *Node) PersistRaftState() {
	if !n.GetDirtyPersistenceState() {
		return
	}

	n.RaftMu.Lock()
	logCopy := make([]*LogEntry, 0, len(n.log))
	for _, entry := range n.log {
		if entry != nil {
			entryCopy := &LogEntry{
				Term:    entry.Term,
				Index:   entry.Index,
				Command: make([]byte, len(entry.Command)),
			}
			copy(entryCopy.Command, entry.Command)

			logCopy = append(logCopy, entryCopy)
		}
	}

	savedState := &PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         logCopy,
	}
	n.RaftMu.Unlock()

	err := n.WriteToDisk(savedState)
	n.RaftMu.Lock()
	if err != nil {
		log.Printf("PersistRaftState: Error writing saved state to disk: %v", err)
	} else {
		log.Printf("PersistRaftState: Saved state to disk")
		n.dirtyPersistenceState = false
	}
	n.RaftMu.Unlock()
}

func (n *Node) WriteToDisk(savedState *PersistentState) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(savedState); err != nil {
		return fmt.Errorf("error encoding saved state: %v", err)
	}

	filePath := filepath.Join(n.dataDir, "raft_state.gob")
	tmpFilePath := filePath + ".tmp"

	file, err := os.OpenFile(tmpFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	if _, err := file.Write(buffer.Bytes()); err != nil {
		log.Fatalf("error saving raft state: %v", err)
	}
	if err := file.Sync(); err != nil {
		log.Fatalf("error saving raft state: %v", err)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("error closing file: %v", err)
	}
	if err := os.Rename(tmpFilePath, filePath); err != nil {
		os.Remove(tmpFilePath)
		return fmt.Errorf("node %s: error renaming saved state: %v", n.ID, err)
	}
	log.Printf("Node %s: Saved to Raft State %s", n.ID, filePath)
	return nil
}

func (n *Node) PersistStateGoroutine() {
	ticker := n.Clock.NewTicker(50 * time.Millisecond)
	defer func() {
		n.persistWg.Done()
	}()
	for {
		select {
		case <-n.ctx.Done():
			log.Printf("Node %s: PersistStateGoroutine stopped through ctx.Done()", n.ID)
			if n.GetDirtyPersistenceState() {
				n.PersistRaftState()
			}
			return
		case <-ticker.Chan():
			if n.GetDirtyPersistenceState() {
				n.PersistRaftState()
			}
		case <-n.persistStateChan:
			n.PersistRaftState()
		}
	}
}
