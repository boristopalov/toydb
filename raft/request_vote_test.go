package raft

import (
	"log/slog"
	"testing"
)

func TestRequestVoteBasic(t *testing.T) {
	// Create a follower node
	storage := &MockStorage{}
	logger := slog.Default()
	peers := []string{"candidate1", "candidate2"}
	follower := NewRaftNode("follower1", "8080", peers, storage, logger)
	follower.currentTerm = 1

	// Create a RequestVote request
	args := &RequestVoteArgs{
		Term:         2, // Higher term
		CandidateId:  "candidate1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	reply := &RequestVoteReply{}

	// Process the request
	err := follower.RequestVote(args, reply)

	// Verify results
	if err != nil {
		t.Errorf("RequestVote returned error: %v", err)
	}

	if !reply.VoteGranted {
		t.Errorf("Vote should be granted for higher term with empty log")
	}

	if follower.currentTerm != 2 {
		t.Errorf("Follower should update term to 2, got %d", follower.currentTerm)
	}

	if follower.votedFor != "candidate1" {
		t.Errorf("Follower should vote for candidate1, voted for %s", follower.votedFor)
	}
}

func TestRequestVoteAlreadyVoted(t *testing.T) {
	// Create a follower node that already voted
	storage := &MockStorage{}
	logger := slog.Default()
	peers := []string{"candidate1", "candidate2"}
	follower := NewRaftNode("follower1", "8080", peers, storage, logger)
	follower.currentTerm = 2
	follower.votedFor = "candidate1"

	// Create a RequestVote request from a different candidate
	args := &RequestVoteArgs{
		Term:         2, // Same term
		CandidateId:  "candidate2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	reply := &RequestVoteReply{}

	// Process the request
	err := follower.RequestVote(args, reply)

	// Verify results
	if err != nil {
		t.Errorf("RequestVote returned error: %v", err)
	}

	if reply.VoteGranted {
		t.Errorf("Vote should not be granted when already voted for another candidate in same term")
	}

	if follower.votedFor != "candidate1" {
		t.Errorf("Follower should still have voted for candidate1, voted for %s", follower.votedFor)
	}
}

func TestRequestVoteLogUpToDate(t *testing.T) {
	// Create a follower node with some log entries
	storage := &MockStorage{}
	logger := slog.Default()
	peers := []string{"candidate1", "candidate2"}
	follower := NewRaftNode("follower1", "8080", peers, storage, logger)
	follower.currentTerm = 2
	follower.log = []LogEntry{
		{Term: 1, Index: 0, Command: []byte("command0")},
		{Term: 1, Index: 1, Command: []byte("command1")},
		{Term: 2, Index: 2, Command: []byte("command2")},
	}

	// Test cases for log comparison
	testCases := []struct {
		name           string
		candidateTerm  int
		candidateIndex int
		expectVote     bool
	}{
		{"Lower term", 1, 5, false},
		{"Same term, lower index", 2, 1, false},
		{"Same term, same index", 2, 2, true},
		{"Same term, higher index", 2, 3, true},
		{"Higher term", 3, 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset vote for each test case
			follower.votedFor = ""

			args := &RequestVoteArgs{
				Term:         follower.currentTerm,
				CandidateId:  "candidate1",
				LastLogTerm:  tc.candidateTerm,
				LastLogIndex: tc.candidateIndex,
			}

			reply := &RequestVoteReply{}

			// Process the request
			follower.RequestVote(args, reply)

			if reply.VoteGranted != tc.expectVote {
				t.Errorf("Expected vote granted: %v, got: %v", tc.expectVote, reply.VoteGranted)
			}
		})
	}
}
