package cluster

import (
	"encoding/json"
	"log"
	"sync"
)

type KVStore struct {
	mu    sync.Mutex
	store map[string]string
}

type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Valid   bool
	Command []byte
}

func NewKVStore() *KVStore {
	kvStore := &KVStore{
		store: make(map[string]string),
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
		kv.store[cmd.Key] = cmd.Value
	default:
		log.Printf("Invalid Command Type: %v", cmd.Type)
	}
	log.Printf("Command: %v", kv.store[cmd.Key])
}

func (kv *KVStore) GetData() map[string]string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	dataCopy := make(map[string]string, len(kv.store))
	for k, v := range kv.store {
		dataCopy[k] = v
	}
	return dataCopy
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data, ok := kv.store[key]; ok {
		return data, true
	} else {
		return "", false
	}
}
