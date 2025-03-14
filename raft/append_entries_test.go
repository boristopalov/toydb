package raft

import (
	"log/slog"
	"testing"
	"time"
)

func TestAppendEntriesBasic(t *testing.T) {
	// Create a follower node
	storage := &MockStorage{}
	logger := slog.Default()
	follower := NewRaftNode("follower1", "8080", []string{":8081"}, storage, logger)
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
	node1 := NewRaftNode("node1", "8080", []string{":8081"}, storage, logger)
	node2 := NewRaftNode("node2", "8081", []string{":8080"}, storage, logger)

	node1.Start()
	node2.Start()
	defer node1.Stop()
	defer node2.Stop()

	node1.ConnectToPeers()
	node2.ConnectToPeers()

	// Subscribe to leader's committed entries

	// Start election on node1
	node1.StartElection()

	// Wait for election to complete
	time.Sleep(200 * time.Millisecond)

	// Submit a command to the leader
	cmd1 := []byte("command1")
	cmd2 := []byte("command2")
	node1.SubmitCommand(cmd1)
	node1.SubmitCommand(cmd2)

	// // Wait for the command to be committed
	time.Sleep(300 * time.Millisecond)

	// Verify results
	if len(node1.log) != 2 {
		t.Errorf("Expected 2 log entries on leader, got %d", len(node1.log))
	}

	if len(node2.log) != 2 {
		t.Errorf("Expected 2 log entries on follower, got %d", len(node2.log))
	}

	// our log is zero-indexed so the second entry is index 1
	if node1.commitIndex != 1 {
		t.Errorf("Expected commitIndex to be 1, got %d", node1.commitIndex)
	}

	select {
	case entry := <-node1.committedValuesChan:
		if string(entry.Command) != string(cmd1) {
			t.Errorf("Expected command %s, got %s", cmd1, entry.Command)
		}
	case <-time.After(200 * time.Millisecond):
		t.Errorf("Expected entry 1 to be received, got nothing")
	}

	select {
	case entry := <-node1.committedValuesChan:
		if string(entry.Command) != string(cmd2) {
			t.Errorf("Expected command %s, got %s", cmd2, entry.Command)
		}
	case <-time.After(200 * time.Millisecond):
		t.Errorf("Expected entry 2 to be received, got nothing")
	}
}

func TestAppendEntriesLogInconsistency(t *testing.T) {
	// Create a follower node with existing log
	storage := &MockStorage{}
	logger := slog.Default()
	follower := NewRaftNode("follower1", "8080", []string{":8081"}, storage, logger)
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
		LeaderId:     ":8081",
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
