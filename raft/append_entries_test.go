package raft

import (
	"log/slog"
	"testing"
)

func TestAppendEntriesBasic(t *testing.T) {
	// Create a follower node
	storage := &MockStorage{}
	logger := slog.Default()
	follower := NewRaftNode("follower1", []string{"leader1"}, storage, logger)
	follower.currentTerm = 1

	// Create a simple AppendEntries request (heartbeat)
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	reply := &AppendEntriesReply{}

	// Process the request
	err := follower.AppendEntries(args, reply)

	// Verify results
	if !reply.Success {
		t.Errorf("AppendEntries returned error: %v", err)
	}

	if !reply.Success {
		t.Errorf("AppendEntries should succeed for valid heartbeat")
	}

	if follower.role != Follower {
		t.Errorf("Node should remain a follower after AppendEntries")
	}
}

func TestAppendEntriesWithEntries(t *testing.T) {
	// Create a follower node
	storage := &MockStorage{}
	logger := slog.Default()
	follower := NewRaftNode("follower1", []string{"leader1"}, storage, logger)
	follower.currentTerm = 1

	// Create an AppendEntries request with entries
	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("command1")},
		{Term: 1, Index: 2, Command: []byte("command2")},
	}

	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 2,
	}

	reply := &AppendEntriesReply{}

	// Process the request
	err := follower.AppendEntries(args, reply)

	// Verify results
	if !reply.Success {
		t.Errorf("AppendEntries returned error: %v", err)
	}

	if !reply.Success {
		t.Errorf("AppendEntries should succeed for valid entries")
	}

	if len(follower.log) != 2 {
		t.Errorf("Expected 2 log entries, got %d", len(follower.log))
	}

	if follower.commitIndex != 2 {
		t.Errorf("Expected commitIndex to be 2, got %d", follower.commitIndex)
	}
}

func TestAppendEntriesLogInconsistency(t *testing.T) {
	// Create a follower node with existing log
	storage := &MockStorage{}
	logger := slog.Default()
	follower := NewRaftNode("follower1", []string{"leader1"}, storage, logger)
	follower.currentTerm = 2
	follower.log = []LogEntry{
		{Term: 1, Index: 0, Command: []byte("command0")},
		{Term: 1, Index: 1, Command: []byte("command1")},
		{Term: 2, Index: 2, Command: []byte("command2-follower")}, // Conflicting entry
	}

	// Create an AppendEntries request with conflicting entries
	entries := []LogEntry{
		{Term: 2, Index: 2, Command: []byte("command2-leader")}, // Different command at same index
		{Term: 2, Index: 3, Command: []byte("command3")},
	}

	args := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     "leader1",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      entries,
		LeaderCommit: 3,
	}

	reply := &AppendEntriesReply{}

	// Process the request
	err := follower.AppendEntries(args, reply)

	// Verify results
	if !reply.Success {
		t.Errorf("AppendEntries returned error: %v", err)
	}

	if len(follower.log) != 4 {
		t.Errorf("Expected 4 log entries after conflict resolution, got %d", len(follower.log))
	}

	// Verify the conflicting entry was replaced
	if string(follower.log[2].Command) != "command2-leader" {
		t.Errorf("Conflicting entry was not replaced correctly, got %s", string(follower.log[2].Command))
	}

	if follower.commitIndex != 3 {
		t.Errorf("Expected commitIndex to be 3, got %d", follower.commitIndex)
	}
}
