package raft

import (
	"log/slog"
	"sync"
	"testing"
	"time"
)

// MockRaftNetwork simulates a network of Raft nodes for testing
type MockRaftNetwork struct {
	mu    sync.Mutex
	nodes map[string]RaftNode
}

// type TestableRaftNode struct {
// 	RaftNode
// }

// func (node *TestableRaftNode) SendRequestVote(peerId string) bool {
// 	return true
// }

// NewMockRaftNetwork creates a new mock network
func NewMockRaftNetwork() *MockRaftNetwork {
	return &MockRaftNetwork{
		nodes: make(map[string]RaftNode),
	}
}

// AddNode adds a node to the network
func (network *MockRaftNetwork) AddNode(node RaftNode) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.nodes[node.GetId()] = node
}

// DisconnectNode simulates a node disconnection
func (network *MockRaftNetwork) DisconnectNode(id string) {
	network.mu.Lock()
	defer network.mu.Unlock()
	delete(network.nodes, id)
}

// ReconnectNode simulates a node reconnection
func (network *MockRaftNetwork) ReconnectNode(node RaftNode) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.nodes[node.GetId()] = node
}

// TestElectionBasic tests a basic election with three nodes
func TestElectionBasic(t *testing.T) {
	// Create a mock storage implementation
	storage1 := &MockStorage{}
	storage2 := &MockStorage{}
	storage3 := &MockStorage{}

	// Create three nodes
	logger := slog.Default()
	node1 := NewRaftNode("node1", []string{"node2", "node3"}, storage1, logger)
	node2 := NewRaftNode("node2", []string{"node1", "node3"}, storage2, logger)
	node3 := NewRaftNode("node3", []string{"node1", "node2"}, storage3, logger)

	// Create a mock network
	network := NewMockRaftNetwork()
	network.AddNode(node1)
	network.AddNode(node2)
	network.AddNode(node3)

	// Start node1 and trigger an election
	// node1.Start()

	// Manually trigger an election on node1
	node1.StartElection()

	// Check if node1 became the leader
	node1.mu.Lock()
	role1 := node1.role
	term1 := node1.currentTerm
	node1.mu.Unlock()

	if role1 != Leader {
		t.Errorf("Expected node1 to have role Leader, but got role %v", role1)
	}

	if term1 != 1 {
		t.Errorf("Expected term to be 1, but got %d", term1)
	}

	// Check if other nodes recognized the leader
	node2.mu.Lock()
	term2 := node2.currentTerm
	votedFor2 := node2.votedFor
	node2.mu.Unlock()

	node3.mu.Lock()
	term3 := node3.currentTerm
	votedFor3 := node3.votedFor
	node3.mu.Unlock()

	if term2 != 1 || term3 != 1 {
		t.Errorf("Expected all nodes to be at term 1, but got term2=%d, term3=%d", term2, term3)
	}

	if votedFor2 != "node1" || votedFor3 != "node1" {
		t.Errorf("Expected all nodes to vote for node1, but got votedFor2=%s, votedFor3=%s", votedFor2, votedFor3)
	}

	// Clean up
	node1.Stop()
	node2.Stop()
	node3.Stop()
}

// TestElectionWithDisconnection tests election behavior when nodes disconnect
func TestElectionWithDisconnection(t *testing.T) {
	// Create a mock storage implementation
	storage1 := &MockStorage{}
	storage2 := &MockStorage{}
	storage3 := &MockStorage{}

	// Create three nodes
	logger := slog.Default()
	node1 := NewRaftNode("node1", []string{"node2", "node3"}, storage1, logger)
	node2 := NewRaftNode("node2", []string{"node1", "node3"}, storage2, logger)
	node3 := NewRaftNode("node3", []string{"node1", "node2"}, storage3, logger)

	// Create a mock network
	network := NewMockRaftNetwork()
	network.AddNode(node1)
	network.AddNode(node2)
	network.AddNode(node3)

	// Start all nodes
	// this starts the election timer
	node1.Start()
	node2.Start()
	node3.Start()

	// Give some time for the election to complete
	time.Sleep(100 * time.Millisecond)
	// Manually trigger an election on node1
	node1.StartElection()

	// Check if node1 became the leader
	node1.mu.Lock()
	isLeader := node1.role == Leader
	node1.mu.Unlock()

	if !isLeader {
		t.Errorf("Expected node1 to become leader")
	}

	// Disconnect the leader
	network.DisconnectNode("node1")

	// Manually trigger an election on node2
	node2.StartElection()

	// Give some time for the election to complete
	time.Sleep(100 * time.Millisecond)

	// Check if node2 became the new leader
	node2.mu.Lock()
	role2 := node2.role
	term2 := node2.currentTerm
	node2.mu.Unlock()

	if role2 != Leader {
		t.Errorf("Expected node2 to become leader after node1 disconnected, but got role %v", role2)
	}

	if term2 != 2 {
		t.Errorf("Expected term to be 2 after new election, but got %d", term2)
	}

	// Check if node3 recognized the new leader
	node3.mu.Lock()
	term3 := node3.currentTerm
	votedFor3 := node3.votedFor
	node3.mu.Unlock()

	if term3 != 2 {
		t.Errorf("Expected node3 to be at term 2, but got %d", term3)
	}

	if votedFor3 != "node2" {
		t.Errorf("Expected node3 to vote for node2, but voted for %s", votedFor3)
	}

	// Clean up
	node1.Stop()
	node2.Stop()
	node3.Stop()
}

// TestElectionTimeout tests that a follower starts an election when it doesn't hear from a leader
func TestElectionTimeout(t *testing.T) {
	// Create a mock storage implementation
	storage := &MockStorage{}
	logger := slog.Default()
	node := NewRaftNode("node1", []string{"node2", "node3"}, storage, logger)

	// Start the node
	node.Start()

	// Wait for the election timeout to trigger
	time.Sleep(100 * time.Millisecond)

	// Check if the node transitioned to candidate
	node.mu.Lock()
	role := node.role
	term := node.currentTerm
	node.mu.Unlock()

	if role != Candidate {
		t.Errorf("Expected node to become candidate after election timeout, but got role %v", role)
	}

	if term != 1 {
		t.Errorf("Expected term to be 1 after election timeout, but got %d", term)
	}

	// Clean up
	node.Stop()
}

// TestElectionResetTimeout tests that receiving an AppendEntries resets the election timeout
func TestElectionResetTimeout(t *testing.T) {
	// Create a mock storage implementation
	storage := &MockStorage{}
	logger := slog.Default()
	node := NewRaftNode("node1", []string{"node2"}, storage, logger)

	// Create a channel to track when startElection is called
	electionStarted := make(chan struct{}, 1)

	// Start the node
	node.Start()

	// Send an AppendEntries RPC to reset the election timeout
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}

	// Wait a bit, then send the AppendEntries
	time.Sleep(20 * time.Millisecond)
	node.AppendEntries(args, reply)

	// Wait for what would have been the original timeout
	time.Sleep(60 * time.Millisecond)

	// Check if an election was started (it shouldn't have been)
	select {
	case <-electionStarted:
		t.Errorf("Election was started despite receiving AppendEntries")
	default:
		// This is the expected path - no election was started
	}

	// Wait for the new timeout to elapse
	time.Sleep(60 * time.Millisecond)

	// Now an election should have started
	select {
	case <-electionStarted:
		// This is the expected path - an election was started
	default:
		t.Errorf("Election was not started after the reset timeout elapsed")
	}

	// Clean up
	node.Stop()
}
