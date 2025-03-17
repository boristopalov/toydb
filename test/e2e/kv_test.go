package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"toydb/client"
	"toydb/db"
	"toydb/raft"
	"toydb/server"
)

// setupTestRaftCluster creates a test Raft cluster with three nodes
func setupTestRaftCluster(t *testing.T, nodeCount int) ([]raft.RaftNode, *db.KVStore, []*server.RaftKVServer, map[string]string) {
	t.Helper()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create KV store
	store := db.NewKVStore()

	// Base ports for Raft and HTTP
	baseRaftPort := 8090
	httpPort := 3090
	httpAddrs := make([]string, nodeCount)
	for i := range nodeCount {
		httpAddrs[i] = fmt.Sprintf(":%d", httpPort+i)
	}

	// Create peer addresses list
	peerAddrs := make([]string, nodeCount)
	for i := range nodeCount {
		peerAddrs[i] = fmt.Sprintf("localhost:%d", baseRaftPort+i)
	}

	// Create Raft nodes
	raftNodes := make([]raft.RaftNode, nodeCount)
	kvServers := make([]*server.RaftKVServer, nodeCount)

	// Create a map of node IDs to HTTP URLs
	serverURLs := make(map[string]string)

	for i := range nodeCount {
		// Filter out self from peer list
		var nodePeers []string
		for j, addr := range peerAddrs {
			if j != i {
				nodePeers = append(nodePeers, addr)
			}
		}

		nodeID := fmt.Sprintf("test-node-%d", i)
		port := fmt.Sprintf("%d", baseRaftPort+i)
		raftNodes[i] = raft.NewRaftNode(nodeID, port, nodePeers, raft.NewSimpleDiskStorage(), logger)
		raftNodes[i].Start()
		kvServers[i] = server.NewRaftKVServer(store, raftNodes[i], logger)
		httpAddr := httpAddrs[i]

		// Add to the map of server URLs
		serverURLs[nodeID] = fmt.Sprintf("http://localhost%s", httpAddr)

		go func() {
			if err := kvServers[i].Start(httpAddr); err != nil {
				t.Logf("Server stopped: %v", err)
			}
		}()
	}

	// Connect the nodes to each other
	for i := range nodeCount {
		raftNodes[i].ConnectToPeers()
	}

	// Start election on first node to make it the leader
	raftNodes[0].StartElection()

	// Wait longer for the node to become a leader
	time.Sleep(200 * time.Millisecond)

	return raftNodes, store, kvServers, serverURLs
}

// TestBasicGetPut tests basic Get and Put operations
func TestBasicGetPut(t *testing.T) {
	// Setup test cluster
	raftNodes, _, _, serverURLs := setupTestRaftCluster(t, 3)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client with the leader ID
	leaderID := raftNodes[0].GetId()
	apiClient := client.NewRaftKVClient(serverURLs, leaderID, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Test Put
	err := apiClient.Put(ctx, "test-key", "test-value")
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Test Get
	value, err := apiClient.Get(ctx, "test-key")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	// Verify value
	if value != "test-value" {
		t.Errorf("Expected value 'test-value', got '%s'", value)
	}
}

// TestKeyNotFound tests the behavior when a key is not found
func TestKeyNotFound(t *testing.T) {
	// Setup test cluster
	raftNodes, _, _, serverURLs := setupTestRaftCluster(t, 3)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client with the leader ID
	leaderID := raftNodes[0].GetId()
	apiClient := client.NewRaftKVClient(serverURLs, leaderID, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Test Get for non-existent key
	value, err := apiClient.Get(ctx, "non-existent-key")

	// Verify error
	if err != client.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// Verify value
	if value != "" {
		t.Errorf("Expected empty value, got '%s'", value)
	}
}

// TestMultipleOperations tests a sequence of operations
func TestMultipleOperations(t *testing.T) {
	// Setup test cluster
	raftNodes, _, _, serverURLs := setupTestRaftCluster(t, 3)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client with the leader ID
	leaderID := raftNodes[0].GetId()
	apiClient := client.NewRaftKVClient(serverURLs, leaderID, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Test multiple operations
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	// Get before putting any data
	for k := range testData {
		value, _ := apiClient.Get(ctx, k)
		if value != "" {
			t.Errorf("For key %s: expected empty value, got '%s'", k, value)
		}
	}

	// Put all test data
	for k, v := range testData {
		err := apiClient.Put(ctx, k, v)
		if err != nil {
			t.Fatalf("Failed to put value for key %s: %v", k, err)
		}
	}

	// Get and verify all test data
	for k, expectedValue := range testData {
		value, err := apiClient.Get(ctx, k)
		if err != nil {
			t.Fatalf("Failed to get value for key %s: %v", k, err)
		}
		if value != expectedValue {
			t.Errorf("For key %s: expected value '%s', got '%s'", k, expectedValue, value)
		}
	}

	// Update a key
	err := apiClient.Put(ctx, "key1", "updated-value1")
	if err != nil {
		t.Fatalf("Failed to update value: %v", err)
	}

	// Verify updated value
	value, err := apiClient.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Failed to get updated value: %v", err)
	}
	if value != "updated-value1" {
		t.Errorf("Expected updated value 'updated-value1', got '%s'", value)
	}

	// Get all logs
}

func TestLeaderRedirect(t *testing.T) {
	// Setup test cluster
	raftNodes, _, _, serverURLs := setupTestRaftCluster(t, 3)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client for a follower node (not setting the leader ID initially)
	apiClient := client.NewRaftKVClient(serverURLs, "", 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Test Put - this should cause a redirect and update the client's leader ID internally
	err := apiClient.Put(ctx, "test-key", "test-value")
	if err != nil {
		if _, ok := err.(*client.LeaderRedirectErr); ok {
			// Update the client to use the new leader
			apiClient.CurrentLeader = err.(*client.LeaderRedirectErr).LeaderID

			t.Fatalf("Expected to automatically handle LeaderRedirectErr, got %v", err)
		} else {
			t.Fatalf("Failed to put value: %v", err)
		}
	}

	// The client should now know about the leader internally
	// Test Get using the updated leader knowledge
	value, err := apiClient.Get(ctx, "test-key")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	// Verify value
	if value != "test-value" {
		t.Errorf("Expected value 'test-value', got '%s'", value)
	}
}
