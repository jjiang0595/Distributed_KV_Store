package cluster

import (
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/proto"
	"log"
	"strings"
	"sync"
)

type KVStore struct {
	mu    sync.Mutex
	store map[string]*ReviewCommand
}

type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Valid   bool
	Command []byte
}

func NewKVStore() *KVStore {
	kvStore := &KVStore{
		store: make(map[string]*ReviewCommand),
	}
	return kvStore
}

func (kv *KVStore) ApplyCommand(commandBytes []byte) {
	var cmd Command

	err := json.Unmarshal(commandBytes, &cmd)
	if err != nil {
		log.Printf("Error unmarshalling command, %v", err)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Type {
	case CommandPut:
		reviewCmd := &ReviewCommand{}
		if err := proto.Unmarshal(cmd.Value, reviewCmd); err != nil {
			log.Printf("Error unmarshalling command, %v", err)
			return
		}
		key := fmt.Sprintf("recipe:%s:reviews:%s", reviewCmd.RecipeId, reviewCmd.ReviewId)
		kv.store[key] = reviewCmd
		log.Printf("Command: %+v", kv.store[key])
	default:
		log.Printf("Invalid Command Type: %v", cmd.Type)
	}
}

func (kv *KVStore) GetData(recipeId string) map[string]*ReviewCommand {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	dataCopy := make(map[string]*ReviewCommand, len(kv.store))
	for k, v := range kv.store {
		if strings.HasPrefix(k, fmt.Sprintf("recipe:%s:reviews:", recipeId)) {
			dataCopy[k] = v
		}
	}
	return dataCopy
}

//func (kv *KVStore) Get(key string) (string, bool) {
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//
//	if data, ok := kv.store[key]; ok {
//		return data, true
//	} else {
//		return "", false
//	}
//}
