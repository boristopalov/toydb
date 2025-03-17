package e2e

import (
	"context"
	"testing"
	"time"

	"maps"
	"toydb/client"
)

// TestBasicFaultTolerance tests that the system continues to function when a minority of nodes fail
func TestBasicFaultTolerance(t *testing.T) {
	// Skip this test in short mode as it's time-consuming
	if testing.Short() {
		t.Skip("Skipping fault tolerance test in short mode")
	}

	// Setup test cluster with 5 nodes (can tolerate 2 failures)
	raftNodes, _, _, httpAddr := setupTestRaftCluster(t, 5)

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr[0], 5*time.Second)

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
	time.Sleep(300 * time.Millisecond)

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
	maps.Copy(allData, initialData)
	maps.Copy(allData, additionalData)

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

	// Setup test cluster with 3 nodes
	raftNodes, _, _, httpAddrs := setupTestRaftCluster(t, 3)

	// Start election on first node to make it the leader
	raftNodes[0].StartElection()

	// Wait for leader election
	time.Sleep(200 * time.Millisecond)

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddrs[0], 5*time.Second)

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
	time.Sleep(500 * time.Millisecond)

	// Create a new client for the new server
	newApiClient := client.NewRaftKVClient("http://localhost"+httpAddrs[1], 5*time.Second)

	// We don't know which node is the new leader,
	// but if we send a message to a follower, it will be redirected to the leader
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
