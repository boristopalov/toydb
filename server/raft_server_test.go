package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"toydb/db"
	"toydb/raft"
)

// MockRaftNode is a mock implementation of the raft.RaftNode interface for testing
type MockRaftNode struct {
	commitChan chan raft.LogEntry
	commands   [][]byte
}

func NewMockRaftNode() *MockRaftNode {
	return &MockRaftNode{
		commitChan: make(chan raft.LogEntry, 100),
		commands:   make([][]byte, 0),
	}
}

func (m *MockRaftNode) StartElection()  {}
func (m *MockRaftNode) Start()          {}
func (m *MockRaftNode) Stop()           {}
func (m *MockRaftNode) GetId() string   { return "mock-node" }
func (m *MockRaftNode) ConnectToPeers() {}

func (m *MockRaftNode) SubmitCommand(command []byte) {
	m.commands = append(m.commands, command)

	// Simulate command being committed
	go func() {
		time.Sleep(10 * time.Millisecond)
		m.commitChan <- raft.LogEntry{
			Term:    1,
			Index:   len(m.commands),
			Command: command,
		}
	}()
}

func (m *MockRaftNode) SubmitCommandBatch(commands [][]byte) {
	for _, cmd := range commands {
		m.SubmitCommand(cmd)
	}
}

func (m *MockRaftNode) GetCommitChan() <-chan raft.LogEntry {
	return m.commitChan
}

func TestRaftKVServer(t *testing.T) {
	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Create a new KV store and mock Raft node
	store := db.NewKVStore()
	mockRaft := NewMockRaftNode()

	// Create the Raft-integrated server
	server := NewRaftKVServer(store, mockRaft, logger)

	// Test PUT operation
	putPayload := map[string]string{"value": "testvalue"}
	putBody, _ := json.Marshal(putPayload)

	putReq := httptest.NewRequest("PUT", "/kv/testkey", bytes.NewBuffer(putBody))
	putReq.Header.Set("Content-Type", "application/json")
	putRec := httptest.NewRecorder()

	server.ServeHTTP(putRec, putReq)

	if putRec.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, putRec.Code)
	}

	// Verify that a command was submitted to Raft
	if len(mockRaft.commands) != 1 {
		t.Errorf("Expected 1 command to be submitted to Raft, got %d", len(mockRaft.commands))
	}

	// Test GET operation
	getReq := httptest.NewRequest("GET", "/kv/testkey", nil)
	getRec := httptest.NewRecorder()

	server.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, getRec.Code)
	}

	var getResponse map[string]string
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Errorf("Failed to decode response: %v", err)
	}

	if getResponse["value"] != "testvalue" {
		t.Errorf("Expected value 'testvalue', got '%s'", getResponse["value"])
	}

	// Verify that another command was submitted to Raft (for the GET)
	if len(mockRaft.commands) != 2 {
		t.Errorf("Expected 2 commands to be submitted to Raft, got %d", len(mockRaft.commands))
	}
}

func TestDeduplication(t *testing.T) {
	// Setup
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	store := db.NewKVStore()
	mockRaft := NewMockRaftNode()
	server := NewRaftKVServer(store, mockRaft, logger)

	// Create a test server
	testServer := httptest.NewServer(server)
	defer testServer.Close()

	// Test PUT with the same request ID
	requestID := "test-request-1"

	// First PUT request
	req1, _ := http.NewRequest("PUT", testServer.URL+"/kv/test-key", strings.NewReader(`{"value":"test-value"}`))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("X-Request-ID", requestID)

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("Failed to send first request: %v", err)
	}
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp1.Status)
	}

	// Wait for the command to be processed
	time.Sleep(50 * time.Millisecond)

	// Verify the value was stored
	value, err := store.Get("test-key")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if value != "test-value" {
		t.Errorf("Expected value 'test-value', got '%s'", value)
	}

	// Count the number of commands submitted to Raft
	commandCount1 := len(mockRaft.commands)

	// Second PUT request with the same request ID
	req2, _ := http.NewRequest("PUT", testServer.URL+"/kv/test-key", strings.NewReader(`{"value":"test-value"}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Request-ID", requestID)

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("Failed to send second request: %v", err)
	}
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp2.Status)
	}

	// Wait for any potential processing
	time.Sleep(50 * time.Millisecond)

	// Count the number of commands submitted to Raft after the second request
	commandCount2 := len(mockRaft.commands)

	// Verify that no new command was submitted to Raft for the duplicate request
	if commandCount2 > commandCount1 {
		t.Errorf("Expected no new commands for duplicate request, but got %d new commands", commandCount2-commandCount1)
	}

	// Test GET with deduplication
	getRequestID := "test-get-request-1"

	// First GET request
	getReq1, _ := http.NewRequest("GET", testServer.URL+"/kv/test-key", nil)
	getReq1.Header.Set("X-Request-ID", getRequestID)

	getResp1, err := http.DefaultClient.Do(getReq1)
	if err != nil {
		t.Fatalf("Failed to send first GET request: %v", err)
	}
	defer getResp1.Body.Close()

	if getResp1.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK for GET, got %v", getResp1.Status)
	}

	var result1 map[string]string
	json.NewDecoder(getResp1.Body).Decode(&result1)

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	// Count commands after first GET
	getCommandCount1 := len(mockRaft.commands)

	// Second GET request with the same request ID
	getReq2, _ := http.NewRequest("GET", testServer.URL+"/kv/test-key", nil)
	getReq2.Header.Set("X-Request-ID", getRequestID)

	getResp2, err := http.DefaultClient.Do(getReq2)
	if err != nil {
		t.Fatalf("Failed to send second GET request: %v", err)
	}
	defer getResp2.Body.Close()

	var result2 map[string]string
	json.NewDecoder(getResp2.Body).Decode(&result2)

	// Count commands after second GET
	getCommandCount2 := len(mockRaft.commands)

	// Verify that no new command was submitted for the duplicate GET
	if getCommandCount2 > getCommandCount1 {
		t.Errorf("Expected no new commands for duplicate GET request, but got %d new commands", getCommandCount2-getCommandCount1)
	}

	// Verify both GET requests returned the same value
	if result1["value"] != result2["value"] {
		t.Errorf("Expected same value for both GET requests, got '%s' and '%s'", result1["value"], result2["value"])
	}
}

func TestErrorHandling(t *testing.T) {
	// Setup
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	store := db.NewKVStore()
	mockRaft := NewMockRaftNode()
	server := NewRaftKVServer(store, mockRaft, logger)

	// Create a test server
	testServer := httptest.NewServer(server)
	defer testServer.Close()

	// Test GET for a non-existent key
	req, _ := http.NewRequest("GET", testServer.URL+"/kv/non-existent-key", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// Verify that we get a 404 Not Found response
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404 Not Found for non-existent key, got %v", resp.Status)
	}

	// Test PUT with invalid data
	invalidReq, _ := http.NewRequest("PUT", testServer.URL+"/kv/test-key", strings.NewReader(`{"invalid":"json"`))
	invalidReq.Header.Set("Content-Type", "application/json")
	invalidResp, err := http.DefaultClient.Do(invalidReq)
	if err != nil {
		t.Fatalf("Failed to send invalid request: %v", err)
	}
	defer invalidResp.Body.Close()

	// Verify that we get a 400 Bad Request response
	if invalidResp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400 Bad Request for invalid JSON, got %v", invalidResp.Status)
	}

	// Test PUT without a value field
	missingValueReq, _ := http.NewRequest("PUT", testServer.URL+"/kv/test-key", strings.NewReader(`{"not_value":"test"}`))
	missingValueReq.Header.Set("Content-Type", "application/json")
	missingValueResp, err := http.DefaultClient.Do(missingValueReq)
	if err != nil {
		t.Fatalf("Failed to send request with missing value: %v", err)
	}
	defer missingValueResp.Body.Close()

	// Verify that we get a 400 Bad Request response
	if missingValueResp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400 Bad Request for missing value field, got %v", missingValueResp.Status)
	}
}
