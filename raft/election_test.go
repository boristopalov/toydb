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
	node1 := NewRaftNode("node1", "8080", []string{":8081", ":8082"}, storage1, logger)
	node2 := NewRaftNode("node2", "8081", []string{":8080", ":8082"}, storage2, logger)
	node3 := NewRaftNode("node3", "8082", []string{":8080", ":8081"}, storage3, logger)

	// Create a mock network
	network := NewMockRaftNetwork()
	network.AddNode(node1)
	network.AddNode(node2)
	network.AddNode(node3)

	node1.Start()
	node2.Start()
	node3.Start()

	node1.ConnectToPeers()
	node2.ConnectToPeers()
	node3.ConnectToPeers()

	// Clean up
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()

	// Manually trigger an election on node1
	node1.StartElection()

	time.Sleep(300 * time.Millisecond)

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

}

// TestElectionWithDisconnection tests election behavior when nodes disconnect
func TestElectionWithDisconnection(t *testing.T) {
	// Create a mock storage implementation
	storage1 := &MockStorage{}
	storage2 := &MockStorage{}
	storage3 := &MockStorage{}

	// Create three nodes
	logger := slog.Default()
	node1 := NewRaftNode("node1", "8080", []string{":8081", ":8082"}, storage1, logger)
	node2 := NewRaftNode("node2", "8081", []string{":8080", ":8082"}, storage2, logger)
	node3 := NewRaftNode("node3", "8082", []string{":8080", ":8081"}, storage3, logger)

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

	node1.ConnectToPeers()
	node2.ConnectToPeers()
	node3.ConnectToPeers()

	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()

	// Manually trigger an election on node1
	node1.StartElection()

	// Give some time for the election to complete
	time.Sleep(100 * time.Millisecond)

	// Check if node1 became the leader
	node1.mu.Lock()
	isLeader := node1.role == Leader
	node1.mu.Unlock()

	if !isLeader {
		t.Errorf("Expected node1 to become leader")
	}

	// Disconnect the leader
	node1.Stop()

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
		t.Errorf("Expected node2 to become leader after node1 disconnected, but got role2=%v", role2)
	}

	if term2 != 2 {
		t.Errorf("Expected term to be 2 after new election, but got term2=%d", term2)
	}

	// Check if node3 recognized the new leader
	node3.mu.Lock()
	role3 := node3.role
	term3 := node3.currentTerm
	node3.mu.Unlock()

	if role3 != Follower {
		t.Errorf("Expected node3 to be follower after node1 disconnected, but got role3=%v", role3)
	}

	if term3 != 2 {
		t.Errorf("Expected term to be 2 after new election, but got term3=%d", term3)
	}

}
