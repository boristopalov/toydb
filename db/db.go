package db

import (
	"errors"
	"sync"
)

// ErrKeyNotFound is returned when a key is not found in the store
var ErrKeyNotFound = errors.New("key not found")

// KVStore is a simple in-memory key-value store
type KVStore struct {
	mu    sync.RWMutex
	store map[string]string
}

// NewKVStore creates a new KVStore
func NewKVStore() *KVStore {
	return &KVStore{
		store: make(map[string]string),
	}
}

// Get retrieves a value for a given key
func (kv *KVStore) Get(key string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, ok := kv.store[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	return value, nil
}

// Put stores a value for a given key
func (kv *KVStore) Put(key, value string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.store[key] = value
	return nil
}
