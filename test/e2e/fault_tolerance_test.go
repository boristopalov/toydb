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

// TestBasicFaultTolerance tests that the system continues to function when a minority of nodes fail
func TestBasicFaultTolerance(t *testing.T) {
	// Skip this test in short mode as it's time-consuming
	if testing.Short() {
		t.Skip("Skipping fault tolerance test in short mode")
	}

	// Setup test cluster with 5 nodes (can tolerate 2 failures)
	raftNodes, _, _, httpAddr := setupMultiNodeTestRaftCluster(t, 5)

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Put some initial data
	initialData := map[string]string{
		"ft-key1": "ft-value1",
		"ft-key2": "ft-value2",
		"ft-key3": "ft-value3",
	}

	for k, v := range initialData {
		err := apiClient.Put(ctx, k, v)
		if err != nil {
			t.Fatalf("Failed to put initial value for key %s: %v", k, err)
		}
	}

	// Verify initial data
	for k, expectedValue := range initialData {
		value, err := apiClient.Get(ctx, k)
		if err != nil {
			t.Fatalf("Failed to get initial value for key %s: %v", k, err)
		}
		if value != expectedValue {
			t.Errorf("For key %s: expected initial value '%s', got '%s'", k, expectedValue, value)
		}
	}

	// Stop 2 follower nodes (nodes 3 and 4)
	// We're assuming node 0 is the leader, so we're stopping nodes that aren't the leader
	t.Log("Stopping 2 follower nodes")
	raftNodes[3].Stop()
	raftNodes[4].Stop()

	// Wait a bit for the cluster to stabilize
	time.Sleep(1 * time.Second)

	// The cluster should still function with 3 out of 5 nodes
	// Put some more data
	additionalData := map[string]string{
		"ft-key4": "ft-value4",
		"ft-key5": "ft-value5",
	}

	for k, v := range additionalData {
		err := apiClient.Put(ctx, k, v)
		if err != nil {
			t.Fatalf("Failed to put additional value for key %s after node failures: %v", k, err)
		}
	}

	// Verify all data (both initial and additional)
	allData := make(map[string]string)
	for k, v := range initialData {
		allData[k] = v
	}
	for k, v := range additionalData {
		allData[k] = v
	}

	for k, expectedValue := range allData {
		value, err := apiClient.Get(ctx, k)
		if err != nil {
			t.Fatalf("Failed to get value for key %s after node failures: %v", k, err)
		}
		if value != expectedValue {
			t.Errorf("For key %s: expected value '%s' after node failures, got '%s'", k, expectedValue, value)
		}
	}

	// Clean up remaining nodes
	for i := 0; i < 3; i++ {
		raftNodes[i].Stop()
	}
}

// TestLeaderFailover tests that the system can elect a new leader when the current leader fails
func TestLeaderFailover(t *testing.T) {
	// Skip this test in short mode as it's time-consuming
	if testing.Short() {
		t.Skip("Skipping leader failover test in short mode")
	}

	// This test requires a custom setup where we can control which node is the leader
	// and then create a new server with a different leader after failover

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create KV store
	store := db.NewKVStore()

	// Base ports for Raft and HTTP
	baseRaftPort := 9100
	httpPort := 4100
	httpAddr := fmt.Sprintf(":%d", httpPort)

	// Create 3 Raft nodes
	nodeCount := 3
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

		nodeID := fmt.Sprintf("failover-node-%d", i)
		port := fmt.Sprintf("%d", baseRaftPort+i)
		raftNodes[i] = raft.NewRaftNode(nodeID, port, nodePeers, raft.NewSimpleDiskStorage(), logger)
		raftNodes[i].Start()
	}

	// Start election on first node to make it the leader
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

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Put some initial data
	err := apiClient.Put(ctx, "leader-key", "leader-value")
	if err != nil {
		t.Fatalf("Failed to put initial value: %v", err)
	}

	// Verify initial data
	value, err := apiClient.Get(ctx, "leader-key")
	if err != nil {
		t.Fatalf("Failed to get initial value: %v", err)
	}
	if value != "leader-value" {
		t.Errorf("Expected initial value 'leader-value', got '%s'", value)
	}

	// Stop the leader (node 0)
	t.Log("Stopping the leader node")
	raftNodes[0].Stop()

	// Wait for a new leader to be elected
	time.Sleep(2 * time.Second)

	// Create a new server with node 1 (which should now be the leader)
	// We need to stop the old server first
	// In a real system, we would detect leader changes and update the server

	// Create a new HTTP port for the new server
	newHttpPort := 4101
	newHttpAddr := fmt.Sprintf(":%d", newHttpPort)

	// Create a new server with node 1
	newKvServer := server.NewRaftKVServer(store, raftNodes[1], logger)

	// Start the new server in a goroutine
	go func() {
		if err := newKvServer.Start(newHttpAddr); err != nil {
			t.Logf("New server stopped: %v", err)
		}
	}()

	// Wait for the new server to start
	time.Sleep(500 * time.Millisecond)

	// Create a new client for the new server
	newApiClient := client.NewRaftKVClient("http://localhost"+newHttpAddr, 5*time.Second)

	// Verify that the data is still accessible with the new leader
	newValue, err := newApiClient.Get(ctx, "leader-key")
	if err != nil {
		t.Fatalf("Failed to get value after leader failover: %v", err)
	}
	if newValue != "leader-value" {
		t.Errorf("Expected value 'leader-value' after leader failover, got '%s'", newValue)
	}

	// Put some new data with the new leader
	err = newApiClient.Put(ctx, "new-leader-key", "new-leader-value")
	if err != nil {
		t.Fatalf("Failed to put value with new leader: %v", err)
	}

	// Verify the new data
	newValue, err = newApiClient.Get(ctx, "new-leader-key")
	if err != nil {
		t.Fatalf("Failed to get new value with new leader: %v", err)
	}
	if newValue != "new-leader-value" {
		t.Errorf("Expected new value 'new-leader-value', got '%s'", newValue)
	}

	// Clean up
	raftNodes[1].Stop()
	raftNodes[2].Stop()
}

// TestNetworkPartition tests the system's behavior during a network partition
// Note: This is a simplified simulation of a network partition
func TestNetworkPartition(t *testing.T) {
	// Skip this test as it requires more complex setup to simulate network partitions
	// In a real test, we would need to control the network connections between nodes
	t.Skip("Skipping network partition test as it requires more complex setup")

	// In a real implementation, we would:
	// 1. Create a cluster with 5 nodes
	// 2. Put some data
	// 3. Simulate a network partition by preventing communication between two groups of nodes
	// 4. Verify that the majority partition continues to function
	// 5. Verify that the minority partition cannot make progress
	// 6. Heal the partition
	// 7. Verify that the system recovers and all nodes are consistent
}
