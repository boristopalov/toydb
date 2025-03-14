package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"toydb/db"
	"toydb/raft"
)

// Command types
const (
	CmdGet = "GET"
	CmdPut = "PUT"
)

// Command represents a command to be processed by the Raft cluster
type Command struct {
	Type      string `json:"type"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	RequestID string `json:"request_id,omitempty"` // Client-provided request ID for deduplication
}

// RaftKVServer is an HTTP server that provides access to a KVStore via Raft consensus
type RaftKVServer struct {
	store      *db.KVStore
	raftNode   raft.RaftNode
	mux        *http.ServeMux
	logger     *slog.Logger
	clientID   string
	pendingOps map[string]chan OpResult
	mu         sync.Mutex

	processedRequestsLock sync.RWMutex
	// Deduplication cache
	processedRequests map[string]OpResult // Maps request IDs to their results
	// Maximum number of processed requests to keep in memory
	maxProcessedRequests int
	// List of request IDs in order of processing (oldest first)
	processedRequestsList []string
}

// OpResult represents the result of an operation
type OpResult struct {
	Value string
	Err   error
}

// NewRaftKVServer creates a new RaftKVServer with the given KVStore and Raft node
func NewRaftKVServer(store *db.KVStore, raftNode raft.RaftNode, logger *slog.Logger) *RaftKVServer {
	server := &RaftKVServer{
		store:                 store,
		raftNode:              raftNode,
		mux:                   http.NewServeMux(),
		logger:                logger,
		clientID:              fmt.Sprintf("kvserver-%d", time.Now().UnixNano()),
		pendingOps:            make(map[string]chan OpResult),
		processedRequests:     make(map[string]OpResult),
		maxProcessedRequests:  10000, // Keep last 10,000 processed requests
		processedRequestsList: make([]string, 0),
	}

	server.setupRoutes()

	// nodeCommittedValuesChan is the channel that receives committed log entries from raftNode
	nodeCommittedValuesChan := raftNode.GetCommitChan()
	go server.processLogEntries(nodeCommittedValuesChan)

	return server
}

// setupRoutes configures the HTTP routes for the server
func (s *RaftKVServer) setupRoutes() {
	s.mux.HandleFunc("/kv/", s.handleKV)
}

// ServeHTTP implements the http.Handler interface
func (s *RaftKVServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleKV handles GET and PUT requests for key-value operations
func (s *RaftKVServer) handleKV(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL path
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// Get request ID from header
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		// Fall back to a server-generated ID if client didn't provide one
		requestID = fmt.Sprintf("server-%d", time.Now().UnixNano())
	}

	// Check if this request has already been processed
	if result, found := s.checkProcessedRequest(requestID); found {
		s.logger.Info("Request already processed, returning cached result", "requestID", requestID)
		if result.Err == db.ErrKeyNotFound {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		} else if result.Err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Return the cached result
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"value": result.Value})
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key, requestID)
	case http.MethodPut:
		s.handlePut(w, r, key, requestID)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGet handles GET requests to retrieve values
func (s *RaftKVServer) handleGet(w http.ResponseWriter, r *http.Request, key, requestID string) {
	// Create a GET command
	cmd := Command{
		Type:      CmdGet,
		Key:       key,
		RequestID: requestID,
	}

	// Submit the command to Raft and wait for the result
	result, err := s.submitAndWait(r.Context(), cmd)
	if err != nil {
		if err == db.ErrKeyNotFound {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Return the result
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"value": result})
}

// handlePut handles PUT requests to store values
func (s *RaftKVServer) handlePut(w http.ResponseWriter, r *http.Request, key, requestID string) {
	var payload map[string]string
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	value, ok := payload["value"]
	if !ok {
		http.Error(w, "Missing 'value' field in request", http.StatusBadRequest)
		return
	}

	// Create a PUT command
	cmd := Command{
		Type:      CmdPut,
		Key:       key,
		Value:     value,
		RequestID: requestID,
	}

	// Submit the command to Raft and wait for the result
	_, err := s.submitAndWait(r.Context(), cmd)
	if err != nil {
		http.Error(w, "Failed to store value", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// checkProcessedRequest checks if a request has already been processed
func (s *RaftKVServer) checkProcessedRequest(requestID string) (OpResult, bool) {
	s.processedRequestsLock.RLock()
	defer s.processedRequestsLock.RUnlock()

	result, found := s.processedRequests[requestID]
	return result, found
}

// recordProcessedRequest records a processed request and its result
func (s *RaftKVServer) recordProcessedRequest(requestID string, result OpResult) {
	s.processedRequestsLock.Lock()
	defer s.processedRequestsLock.Unlock()

	// Check if we already have this request (shouldn't happen, but just in case)
	if _, exists := s.processedRequests[requestID]; exists {
		return
	}

	// Add to our maps
	s.processedRequests[requestID] = result
	s.processedRequestsList = append(s.processedRequestsList, requestID)

	// sliding window to delete old entries if we've exceeded our limit
	if len(s.processedRequestsList) > s.maxProcessedRequests {
		// Remove oldest entries
		for i := range len(s.processedRequestsList) - s.maxProcessedRequests {
			oldRequestID := s.processedRequestsList[i]
			delete(s.processedRequests, oldRequestID)
		}
		// Update the list to only keep the most recent entries
		s.processedRequestsList = s.processedRequestsList[len(s.processedRequestsList)-s.maxProcessedRequests:]
	}
}

// submitAndWait submits a command to Raft and waits for it to be applied
func (s *RaftKVServer) submitAndWait(ctx context.Context, cmd Command) (string, error) {
	// Check if this request has already been processed
	if result, found := s.checkProcessedRequest(cmd.RequestID); found {
		s.logger.Info("Request already processed, returning cached result", "requestID", cmd.RequestID)
		return result.Value, result.Err
	}

	// Generate a unique ID for this operation
	opID := fmt.Sprintf("%s-%s-%d", cmd.Type, cmd.Key, time.Now().UnixNano())

	// Create a channel to receive the result
	resultCh := make(chan OpResult, 1)

	// Register the pending operation
	s.mu.Lock()
	if _, found := s.pendingOps[opID]; found {
		s.logger.Error("Pending operation already exists", "opID", opID)
		s.mu.Unlock()
		return "", fmt.Errorf("pending operation already exists")
	}
	s.pendingOps[opID] = resultCh
	s.mu.Unlock()

	// Add the operation ID to the command
	cmdWithID := struct {
		Command
		OpID string `json:"op_id"`
	}{
		Command: cmd,
		OpID:    opID,
	}

	// Marshal the command to JSON
	cmdBytes, err := json.Marshal(cmdWithID)
	if err != nil {
		s.mu.Lock()
		delete(s.pendingOps, opID)
		s.mu.Unlock()
		return "", fmt.Errorf("failed to marshal command: %v", err)
	}

	// Submit the command to Raft
	s.raftNode.SubmitCommand(cmdBytes)

	// Wait for the result with a timeout
	// http request context can be used to cancel the operation if the client cancels the request
	select {
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.pendingOps, opID)
		s.mu.Unlock()
		return "", ctx.Err()
	case result := <-resultCh:
		// Record this request as processed
		if cmd.RequestID != "" {
			s.recordProcessedRequest(cmd.RequestID, result)
		}
		return result.Value, result.Err
	}
}

// processLogEntries processes committed log entries from Raft
func (s *RaftKVServer) processLogEntries(entryCh <-chan raft.LogEntry) {
	for entry := range entryCh {
		// Parse the command
		var cmdWithID struct {
			Type      string `json:"type"`
			Key       string `json:"key"`
			Value     string `json:"value,omitempty"`
			RequestID string `json:"request_id,omitempty"`
			OpID      string `json:"op_id"`
		}

		if err := json.Unmarshal(entry.Command, &cmdWithID); err != nil {
			s.logger.Error("Failed to unmarshal command", "error", err)
			continue
		}

		// Process the command
		var result OpResult

		switch cmdWithID.Type {
		case CmdGet:
			value, err := s.store.Get(cmdWithID.Key)
			result = OpResult{Value: value, Err: err}

		case CmdPut:
			err := s.store.Put(cmdWithID.Key, cmdWithID.Value)
			result = OpResult{Err: err}

		default:
			s.logger.Error("Unknown command type", "type", cmdWithID.Type)
			continue
		}

		// If this command has a request ID, record it as processed
		if cmdWithID.RequestID != "" {
			s.recordProcessedRequest(cmdWithID.RequestID, result)
		}

		// If this is a command we're waiting for, send the result
		if cmdWithID.OpID != "" {
			s.mu.Lock()
			if ch, ok := s.pendingOps[cmdWithID.OpID]; ok {
				ch <- result
				delete(s.pendingOps, cmdWithID.OpID)
				s.logger.Info("Sent result to pending operation", "opID", cmdWithID.OpID)
				close(ch)
			}
			s.mu.Unlock()
		}
	}
}

// Start starts the HTTP server on the given address
func (s *RaftKVServer) Start(addr string) error {
	return http.ListenAndServe(addr, s)
}
