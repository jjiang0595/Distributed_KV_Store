package cluster

import (
	"context"
	"distributed_kv_store/internal/cluster/types"
	"log"
)

type ReplicationServer struct {
	UnimplementedReplicationServiceServer
	mainNode *types.Node
}

func NewReplicationServer(node *types.Node) *ReplicationServer {
	return &ReplicationServer{
		mainNode: node,
	}
}

func (s *ReplicationServer) Replicate(ctx context.Context, req *ReplicationRequest) (*ReplicationResponse, error) {
	key := req.GetKey()
	value := req.GetValue()

	log.Printf("Replicating key: %s, value: %s", key, value)
	s.mainNode.Mu.Lock()
	s.mainNode.Data[key] = value
	s.mainNode.Mu.Unlock()
	
	log.Printf("Replicated key '%s' to node %s", key, s.mainNode.ID)
	return &ReplicationResponse{Success: true}, nil
}
