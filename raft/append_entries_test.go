package raft

import (
	"fmt"
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
		PrevLogIndex: 0, // 0 is valid for 1-indexed logs when there are no previous entries
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0, // 0 means no entries committed (1-indexed)
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

	// For 1-indexed logs, commitIndex 2 means the second entry is committed
	if node1.commitIndex != 2 {
		t.Errorf("Expected commitIndex to be 2, got %d", node1.commitIndex)
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
		{Term: 1, Index: 1, Command: []byte("command1")},          // 1-indexed
		{Term: 1, Index: 2, Command: []byte("command2")},          // 1-indexed
		{Term: 2, Index: 3, Command: []byte("command3-follower")}, // Conflicting entry, 1-indexed
	}

	// Create an AppendEntries request with conflicting entries
	entries := []LogEntry{
		{Term: 2, Index: 3, Command: []byte("command3-leader")}, // Different command at same index, 1-indexed
		{Term: 2, Index: 4, Command: []byte("command4")},        // 1-indexed
	}

	args := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     ":8081",
		PrevLogIndex: 2, // 1-indexed, refers to the second entry
		PrevLogTerm:  1,
		Entries:      entries,
		LeaderCommit: 4, // 1-indexed, commit up to the fourth entry
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
	// For 1-indexed logs, the third entry is at array index 2
	if string(follower.log[2].Command) != "command3-leader" {
		t.Errorf("Conflicting entry was not replaced correctly, got %s", string(follower.log[2].Command))
	}

	// For 1-indexed logs, commitIndex 4 means entries 1-4 are committed
	if follower.commitIndex != 4 {
		t.Errorf("Expected commitIndex to be 4, got %d", follower.commitIndex)
	}
}

func TestNextIndexMatchIndexUpdate(t *testing.T) {
	testCases := []struct {
		name         string
		prevLogIndex int
		entriesLen   int
	}{
		{"Empty entries", 5, 0},
		{"Single entry", 5, 1},
		{"Multiple entries", 5, 3},
		{"Zero prevLogIndex with entries", 0, 3},
		{"Zero prevLogIndex without entries", 0, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup for snippet 1
			node1 := &raftNode{
				nextIndex:  make(map[string]int),
				matchIndex: make(map[string]int),
			}

			// Setup for snippet 2
			node2 := &raftNode{
				nextIndex:  make(map[string]int),
				matchIndex: make(map[string]int),
			}

			peerId := "peer1"

			// Apply snippet 1
			node1.matchIndex[peerId] = tc.prevLogIndex + tc.entriesLen
			if node1.matchIndex[peerId] > 0 {
				node1.nextIndex[peerId] = node1.matchIndex[peerId] + 1
			}

			// Apply snippet 2
			node2.nextIndex[peerId] = tc.prevLogIndex + tc.entriesLen
			node2.matchIndex[peerId] = node2.nextIndex[peerId] - 1

			// Print detailed calculation
			fmt.Printf("\nTest case: %s\n", tc.name)
			fmt.Printf("prevLogIndex: %d, entriesLen: %d\n", tc.prevLogIndex, tc.entriesLen)
			fmt.Printf("Snippet 1:\n")
			fmt.Printf("  matchIndex = prevLogIndex + entriesLen = %d + %d = %d\n",
				tc.prevLogIndex, tc.entriesLen, node1.matchIndex[peerId])
			fmt.Printf("  nextIndex = matchIndex + 1 = %d + 1 = %d (if matchIndex > 0)\n",
				node1.matchIndex[peerId], node1.nextIndex[peerId])

			fmt.Printf("Snippet 2:\n")
			fmt.Printf("  nextIndex = prevLogIndex + entriesLen = %d + %d = %d\n",
				tc.prevLogIndex, tc.entriesLen, node2.nextIndex[peerId])
			fmt.Printf("  matchIndex = nextIndex - 1 = %d - 1 = %d\n",
				node2.nextIndex[peerId], node2.matchIndex[peerId])

			// Compare results
			if node1.nextIndex[peerId] != node2.nextIndex[peerId] {
				t.Errorf("nextIndex mismatch: snippet1=%d, snippet2=%d",
					node1.nextIndex[peerId], node2.nextIndex[peerId])
			}

			if node1.matchIndex[peerId] != node2.matchIndex[peerId] {
				t.Errorf("matchIndex mismatch: snippet1=%d, snippet2=%d",
					node1.matchIndex[peerId], node2.matchIndex[peerId])
			}
		})
	}
}
