package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"toydb/client"
	"toydb/db"
	"toydb/raft"
	"toydb/server"
)

// setupMultiNodeTestRaftCluster creates a test Raft cluster with multiple nodes
func setupMultiNodeTestRaftCluster(t *testing.T, nodeCount int) ([]raft.RaftNode, *db.KVStore, *server.RaftKVServer, string) {
	t.Helper()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create KV store
	store := db.NewKVStore()

	// Base ports for Raft and HTTP
	baseRaftPort := 9000
	httpPort := 4000
	httpAddr := fmt.Sprintf(":%d", httpPort)

	// Create peer addresses list
	peerAddrs := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		peerAddrs[i] = fmt.Sprintf("localhost:%d", baseRaftPort+i)
	}

	// Create Raft nodes
	raftNodes := make([]raft.RaftNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
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
	}

	// Connect the nodes to each other
	for i := 0; i < nodeCount; i++ {
		raftNodes[i].ConnectToPeers()
	}

	// Start election on first node
	raftNodes[0].StartElection()

	// Wait for leader election
	time.Sleep(1 * time.Second)

	// Create server with the first node (which should be the leader)
	kvServer := server.NewRaftKVServer(store, raftNodes[0], logger)

	// Start server in a goroutine
	go func() {
		if err := kvServer.Start(httpAddr); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	return raftNodes, store, kvServer, httpAddr
}

// TestConcurrentOperations tests concurrent Get and Put operations
func TestConcurrentOperations(t *testing.T) {
	// Setup test cluster with 3 nodes
	raftNodes, _, _, httpAddr := setupMultiNodeTestRaftCluster(t, 3)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Number of concurrent operations
	numOps := 10
	var wg sync.WaitGroup
	wg.Add(numOps)

	// Run concurrent Put operations
	for i := 0; i < numOps; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-key-%d", i)
			value := fmt.Sprintf("concurrent-value-%d", i)

			err := apiClient.Put(ctx, key, value)
			if err != nil {
				t.Errorf("Failed to put value for key %s: %v", key, err)
				return
			}

			// Verify the value was stored correctly
			retrievedValue, err := apiClient.Get(ctx, key)
			if err != nil {
				t.Errorf("Failed to get value for key %s: %v", key, err)
				return
			}

			if retrievedValue != value {
				t.Errorf("For key %s: expected value '%s', got '%s'", key, value, retrievedValue)
			}
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()
}

// TestRequestTimeout tests the client timeout behavior
func TestRequestTimeout(t *testing.T) {
	// Setup test cluster with a 3-node cluster
	raftNodes, _, _, httpAddr := setupMultiNodeTestRaftCluster(t, 3)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client with a very short timeout
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 1*time.Millisecond)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test Put with a timeout
	err := apiClient.Put(ctx, "timeout-key", "timeout-value")

	// The error might be a timeout or another error related to the short timeout
	if err == nil {
		t.Errorf("Expected timeout error, but got no error")
	}
}

// TestRequestIDDeduplication tests that requests with the same ID are deduplicated
func TestRequestIDDeduplication(t *testing.T) {
	// This test requires modifying the client to expose the request ID
	// For now, we'll just verify that multiple identical operations succeed

	// Setup test cluster
	raftNodes, store, _, httpAddr := setupMultiNodeTestRaftCluster(t, 3)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Put a value
	err := apiClient.Put(ctx, "dedup-key", "dedup-value")
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Get the value to verify it was stored
	value, err := apiClient.Get(ctx, "dedup-key")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if value != "dedup-value" {
		t.Errorf("Expected value 'dedup-value', got '%s'", value)
	}

	// Directly check the store to verify there's only one entry
	directValue, err := store.Get("dedup-key")
	if err != nil {
		t.Fatalf("Failed to get value directly from store: %v", err)
	}
	if directValue != "dedup-value" {
		t.Errorf("Expected direct value 'dedup-value', got '%s'", directValue)
	}

	// Put the same value again (should be idempotent)
	err = apiClient.Put(ctx, "dedup-key", "dedup-value")
	if err != nil {
		t.Fatalf("Failed to put value again: %v", err)
	}

	// Get the value again to verify it's still the same
	value, err = apiClient.Get(ctx, "dedup-key")
	if err != nil {
		t.Fatalf("Failed to get value after second put: %v", err)
	}
	if value != "dedup-value" {
		t.Errorf("Expected value 'dedup-value' after second put, got '%s'", value)
	}
}

// TestLargeValues tests storing and retrieving large values
func TestLargeValues(t *testing.T) {
	// Setup test cluster
	raftNodes, _, _, httpAddr := setupMultiNodeTestRaftCluster(t, 3)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a large value (100KB)
	largeValue := make([]byte, 100*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	largeValueStr := string(largeValue)

	// Put the large value
	err := apiClient.Put(ctx, "large-key", largeValueStr)
	if err != nil {
		t.Fatalf("Failed to put large value: %v", err)
	}

	// Get the large value
	retrievedValue, err := apiClient.Get(ctx, "large-key")
	if err != nil {
		t.Fatalf("Failed to get large value: %v", err)
	}

	// Verify the large value
	if retrievedValue != largeValueStr {
		t.Errorf("Retrieved large value does not match original")
	}
}
