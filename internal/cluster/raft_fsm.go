package cluster

import (
	"bytes"
	"log"
)

func (n *Node) ApplierGoroutine() {
	defer func() {
		log.Printf("Node %s: Applier goroutine stopped", n.ID)
		n.applierWg.Done()
	}()

	for {
		n.raftMu.Lock()
		for n.commitIndex <= n.lastApplied {
			select {
			case <-n.ctx.Done():
				n.raftMu.Unlock()
				return
			default:
			}
			n.applierCond.Wait()
		}
		n.raftMu.Unlock()

		select {
		case <-n.ctx.Done():
			return
		default:
			n.raftMu.Lock()
			startIndex := n.lastApplied + 1
			endIndex := n.commitIndex

			msgList := make([]ApplyMsg, 0, endIndex-startIndex+1)
			//log.Printf("LOG: %v", n.log)
			//log.Printf("Start Index: %v", startIndex)
			//log.Printf("End Index: %v", endIndex)
			var applyMsg ApplyMsg
			for i := startIndex; i <= endIndex; i++ {
				if i >= uint64(len(n.log)) {
					log.Fatalf("Node %s: Commit Index greater than length of log", n.ID)
				}

				applyMsg = ApplyMsg{
					Index: n.log[i].Index,
					Term:  n.log[i].Term,
					Valid: func() bool {
						return !bytes.Equal(n.log[i].Command, []byte("NO_OP_ENTRY"))
					}(),
					Command: n.log[i].Command,
				}
				msgList = append(msgList, applyMsg)
			}
			n.lastApplied = endIndex
			n.raftMu.Unlock()

			for _, msg := range msgList {
				if msg.Valid {
					n.kvStore.ApplyCommand(msg.Command)
				}
				if errCh, ok := n.pendingCommands[msg.Index]; ok {
					n.raftMu.Lock()
					delete(n.pendingCommands, msg.Index)
					n.raftMu.Unlock()
					select {
					case errCh <- nil:
					default:
					}
				}
			}
		}
	}
}
