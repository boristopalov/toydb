package raft

import (
	"log/slog"
	"testing"
)

// MockStorage implements the Storage interface for testing
type MockStorage struct {
	term     int
	votedFor string
	log      []LogEntry
}

func (ms *MockStorage) SaveState(term int, votedFor string) error {
	ms.term = term
	ms.votedFor = votedFor
	return nil
}

func (ms *MockStorage) LoadState() (int, string, error) {
	return ms.term, ms.votedFor, nil
}

func (ms *MockStorage) AppendLogEntries(entries []LogEntry) error {
	ms.log = append(ms.log, entries...)
	return nil
}

func (ms *MockStorage) GetLogEntries(startIndex, endIndex int) ([]LogEntry, error) {
	if startIndex >= len(ms.log) || endIndex > len(ms.log) {
		return []LogEntry{}, nil
	}
	return ms.log[startIndex:endIndex], nil
}

func TestNewRaftNode(t *testing.T) {
	storage := &MockStorage{}
	logger := slog.Default()
	peers := []string{"node2", "node3"}
	node := NewRaftNode("node1", peers, storage, logger)

	if node.id != "node1" {
		t.Errorf("Expected node ID to be 'node1', got '%s'", node.id)
	}

	if node.role != Follower {
		t.Errorf("New nodes should start as followers")
	}

	if len(node.peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(node.peers))
	}
}
