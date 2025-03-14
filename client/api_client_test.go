package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestKVClient(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		}

		// Default: Method not allowed
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}))
	defer server.Close()

	// Create a client
	client := NewRaftKVClient(server.URL, 5*time.Second)

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
		shortTimeoutClient := NewRaftKVClient(slowServer.URL, 50*time.Millisecond)

		ctx := context.Background()
		_, err := shortTimeoutClient.Get(ctx, "testkey")
		if err == nil {
			t.Error("Expected timeout error, got nil")
		}
	})
}
