package cluster

import (
	"encoding/json"
	"log"
)

func (n *Node) ApplierGoroutine() {
	defer func() {
		log.Printf("Node %s: Applier goroutine stopped", n.ID)
		n.applierWg.Done()
	}()
	n.RaftMu.Lock()
	defer n.RaftMu.Unlock()

	for {
		for n.commitIndex <= n.lastApplied {
			select {
			case <-n.ctx.Done():
				log.Printf("Node %s: ApplierGoroutine stopped through ctx.Done()", n.ID)
				return
			default:
				n.applierCond.Wait()
				log.Printf("commitIndex: %v, lastApplied: %v", n.commitIndex, n.lastApplied)
			}
		}

		for n.lastApplied < n.commitIndex {
			log.Printf("Increasing n.lastApplied: %v", n.lastApplied)
			n.lastApplied++
			logEntry := n.log[n.lastApplied-1]
			var cmd Command
			err := json.Unmarshal(logEntry.Command, &cmd)
			if err != nil {
				log.Fatalf("Error unmarshalling command: %v", err)
			}

			n.data[cmd.Key] = cmd.Value
			go n.PersistRaftState()
			log.Printf("Node %s: PUT %s -> %s", n.ID, cmd.Key, string(cmd.Value))
		}
	}
}
