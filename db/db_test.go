package db

import (
	"testing"
)

func TestKVStore(t *testing.T) {
	store := NewKVStore()

	// Test PUT operation
	err := store.Put("key1", "value1")
	if err != nil {
		t.Errorf("Failed to put key1: %v", err)
	}

	// Test GET operation
	val, err := store.Get("key1")
	if err != nil {
		t.Errorf("Failed to get key1: %v", err)
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	// Test GET for non-existent key
	_, err = store.Get("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent key, got nil")
	}

	// Test overwriting existing key
	err = store.Put("key1", "newvalue")
	if err != nil {
		t.Errorf("Failed to update key1: %v", err)
	}

	val, err = store.Get("key1")
	if err != nil {
		t.Errorf("Failed to get updated key1: %v", err)
	}
	if val != "newvalue" {
		t.Errorf("Expected newvalue, got %s", val)
	}
}
