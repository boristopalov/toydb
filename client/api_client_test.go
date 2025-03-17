package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestKVClient(t *testing.T) {
	var followerServerURL string

	// Create a mock follower server that will redirect to a leader
	followerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check the request path and method
		if r.URL.Path == "/kv/testkey" {
			switch r.Method {
			case http.MethodGet:
				// Return a successful response for GET
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{"value": "testvalue"})
				return
			case http.MethodPut:
				// Return a successful response for PUT
				w.WriteHeader(http.StatusOK)
				return
			}
		} else if r.URL.Path == "/kv/nonexistent" && r.Method == http.MethodGet {
			// Return a 404 for a non-existent key
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		} else if r.URL.Path == "/kv/error" {
			// Return a server error
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		} else if r.URL.Path == "/kv/redirect" {
			// Simulate a redirect to the leader
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTemporaryRedirect)
			json.NewEncoder(w).Encode(map[string]string{
				"error":     "not_leader",
				"leader_id": "test-leader",
				"self_id":   "test-follower",
				"self_addr": followerServerURL,
			})
			return
		}

		// Default: Method not allowed
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}))
	defer followerServer.Close()

	// Store the URL after the server is created
	followerServerURL = followerServer.URL

	// Create a client with a map of server URLs
	serverURLs := map[string]string{
		"test-node": followerServer.URL,
	}
	client := NewRaftKVClient(serverURLs, "", 5*time.Second)

	// Test GET operation
	t.Run("Get existing key", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		value, err := client.Get(ctx, "testkey")
		if err != nil {
			t.Errorf("Failed to get testkey: %v", err)
		}
		if value != "testvalue" {
			t.Errorf("Expected value 'testvalue', got '%s'", value)
		}
	})

	// Test GET for non-existent key
	t.Run("Get non-existent key", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := client.Get(ctx, "nonexistent")
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
	})

	// Test PUT operation
	t.Run("Put key", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := client.Put(ctx, "testkey", "newvalue")
		if err != nil {
			t.Errorf("Failed to put testkey: %v", err)
		}
	})

	// Test server error
	t.Run("Server error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := client.Get(ctx, "error")
		if err == nil {
			t.Error("Expected error, got nil")
		}
	})

	// Test timeout
	t.Run("Timeout", func(t *testing.T) {
		// Create a server that sleeps
		slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"value": "testvalue"})
		}))
		defer slowServer.Close()

		// Create a client with a very short timeout
		slowServerURLs := map[string]string{
			"slow-node": slowServer.URL,
		}
		shortTimeoutClient := NewRaftKVClient(slowServerURLs, "", 50*time.Millisecond)

		ctx := context.Background()
		_, err := shortTimeoutClient.Get(ctx, "testkey")
		if err == nil {
			t.Error("Expected timeout error, got nil")
		}
	})

	// Test leader redirection
	t.Run("Leader redirection", func(t *testing.T) {
		// Create a mock leader server
		leaderServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/kv/redirect" && r.Method == http.MethodGet {
				// The leader should handle the same key that the follower redirected
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{"value": "leader-value"})
				return
			}
		}))
		defer leaderServer.Close()

		// Create a client that knows about both servers
		redirectClient := NewRaftKVClient(map[string]string{
			"test-follower": followerServer.URL,
			"test-leader":   leaderServer.URL,
		}, "test-leader", 5*time.Second)

		// Try to get a key from the "redirect" endpoint, which should redirect to the leader
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// This should work because the redirect is handled automatically
		value, err := redirectClient.Get(ctx, "redirect")
		if err != nil {
			t.Errorf("Failed to follow redirect: %v", err)
		}
		if value != "leader-value" {
			t.Errorf("Expected 'leader-value', got '%s'", value)
		}

		// Verify that the client learned about the leader
		if redirectClient.CurrentLeader != "test-leader" {
			t.Errorf("Expected current leader to be 'test-leader', got '%s'", redirectClient.CurrentLeader)
		}
	})

	// Test with test cluster configuration
	t.Run("Test cluster configuration", func(t *testing.T) {
		// This simulates your setupTestRaftCluster function but just creates the client config

		// Create node IDs in the same format as the test setup
		nodeCount := 3
		nodeIDs := make([]string, nodeCount)
		for i := 0; i < nodeCount; i++ {
			nodeIDs[i] = fmt.Sprintf("test-node-%d", i)
		}

		// Create HTTP addresses (for a real cluster, these would be actual server addresses)
		httpAddrs := []string{
			followerServer.URL,      // Use our mock server as the first node
			"http://localhost:3091", // These are placeholders
			"http://localhost:3092",
		}

		// Create the server URL mapping
		serverURLs := make(map[string]string)
		for i, nodeID := range nodeIDs {
			serverURLs[nodeID] = httpAddrs[i]
		}

		// Create a client with initial leader being the first node
		testClient := NewRaftKVClient(serverURLs, nodeIDs[0], 5*time.Second)

		// Verify the client has the expected configuration
		if testClient.CurrentLeader != "test-node-0" {
			t.Errorf("Expected current leader to be 'test-node-0', got '%s'", testClient.CurrentLeader)
		}

		if len(testClient.ServerURLs) != nodeCount {
			t.Errorf("Expected %d servers, got %d", nodeCount, len(testClient.ServerURLs))
		}
	})
}
