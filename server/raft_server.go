package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"toydb/db"
	"toydb/raft"

	"github.com/google/uuid"
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
	Error     string `json:"error,omitempty"`      // Error message if operation failed
}

// RaftKVServer is an HTTP server that provides access to a KVStore via Raft consensus
type RaftKVServer struct {
	store      *db.KVStore
	raftNode   raft.RaftNode
	mux        *http.ServeMux
	logger     *slog.Logger
	clientID   string
	pendingOps map[string]chan Command
	mu         sync.Mutex
	httpServer *http.Server // Added to store HTTP server reference
	httpAddr   string       // The HTTP address this server is listening on

	processedRequestsLock sync.RWMutex
	// Deduplication cache
	processedRequests map[string]Command // Maps request IDs to their results
	// Maximum number of processed requests to keep in memory
	maxProcessedRequests int
	// List of request IDs in order of processing (oldest first)
	processedRequestsList []string
}

// NewRaftKVServer creates a new RaftKVServer with the given KVStore and Raft node
func NewRaftKVServer(store *db.KVStore, raftNode raft.RaftNode, logger *slog.Logger) *RaftKVServer {
	server := &RaftKVServer{
		store:                 store,
		raftNode:              raftNode,
		mux:                   http.NewServeMux(),
		logger:                logger,
		clientID:              fmt.Sprintf("kvserver-%s", uuid.New().String()),
		pendingOps:            make(map[string]chan Command),
		processedRequests:     make(map[string]Command),
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

	// Get request ID from header or generate a new one
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = fmt.Sprintf("server-%s", uuid.New().String())
	}

	// Check if this request has already been processed
	if result, found := s.checkProcessedRequest(requestID); found {
		s.logger.Info("[RaftKVServer] Request already processed, returning cached result", "requestID", requestID)
		if result.Error != "" {
			http.Error(w, result.Error, http.StatusInternalServerError)
			return
		}

		if result.Type == CmdGet && result.Value == "" {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}

		// Return the cached result
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"value": result.Value})
		return
	}

	// Check if this node is the leader
	leaderId := s.raftNode.GetLeaderId()
	nodeId := s.raftNode.GetId()

	// If this node is not the leader and we know who the leader is, redirect
	if nodeId != leaderId && leaderId != "" {
		// We're not the leader - respond with a redirect
		s.logger.Info("[RaftKVServer] Redirecting request to leader",
			"requestID", requestID,
			"selfId", nodeId,
			"leaderId", leaderId)

		// Return a JSON response with the leader information
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTemporaryRedirect)
		json.NewEncoder(w).Encode(map[string]string{
			"error":     "not_leader",
			"leader_id": leaderId,
			"self_id":   nodeId,
			"self_addr": s.httpAddr,
		})
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
		// Check for specific error types
		if err.Error() == db.ErrKeyNotFound.Error() {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		s.logger.Error("[RaftKVServer] GET operation failed", "error", err)
		http.Error(w, fmt.Sprintf("Internal server error: %v", err), http.StatusInternalServerError)
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
		s.logger.Error("[RaftKVServer] PUT operation failed", "error", err)
		http.Error(w, fmt.Sprintf("Failed to store value: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// checkProcessedRequest checks if a request has already been processed
func (s *RaftKVServer) checkProcessedRequest(requestID string) (Command, bool) {
	s.processedRequestsLock.RLock()
	defer s.processedRequestsLock.RUnlock()

	result, found := s.processedRequests[requestID]
	return result, found
}

// recordProcessedRequest records a processed request and its result
func (s *RaftKVServer) recordProcessedRequest(requestID string, result Command) {
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
		s.logger.Info("[RaftKVServer] Request already processed, returning cached result", "requestID", cmd.RequestID)
		if result.Error != "" {
			return "", errors.New(result.Error)
		}
		return result.Value, nil
	}

	// Generate a unique ID for this operation
	opID := fmt.Sprintf("%s-%s-%s", cmd.Type, cmd.Key, uuid.New().String())

	// Create a channel to receive the result
	resultCh := make(chan Command, 1)

	// Register the pending operation
	s.mu.Lock()
	if _, found := s.pendingOps[opID]; found {
		s.logger.Error("[RaftKVServer] Pending operation already exists", "opID", opID)
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
		if cmd.RequestID != result.RequestID {
			s.recordProcessedRequest(cmd.RequestID, result)
		}

		// Check if there was an error
		if result.Error != "" {
			return "", errors.New(result.Error)
		}

		return result.Value, nil
	}
}

// processLogEntries processes committed log entries from Raft
func (s *RaftKVServer) processLogEntries(entryCh <-chan raft.LogEntry) {
	for entry := range entryCh {
		// Parse the command
		var cmdWithId struct {
			Command
			OpID string `json:"op_id"`
		}

		if err := json.Unmarshal(entry.Command, &cmdWithId); err != nil {
			s.logger.Error("[RaftKVServer] Failed to unmarshal command", "error", err)
			continue
		}

		switch cmdWithId.Type {
		case CmdGet:
			value, err := s.store.Get(cmdWithId.Key)
			if err != nil {
				s.logger.Error("[RaftKVServer] Failed to GET value", "error", err)
				cmdWithId.Error = err.Error()
			} else {
				cmdWithId.Value = value
			}

		case CmdPut:
			err := s.store.Put(cmdWithId.Key, cmdWithId.Value)
			if err != nil {
				s.logger.Error("[RaftKVServer] Failed to PUT value", "error", err)
				cmdWithId.Error = err.Error()
			}

		default:
			s.logger.Error("[RaftKVServer] Unknown command type", "type", cmdWithId.Type)
			cmdWithId.Error = fmt.Sprintf("unknown command type: %s", cmdWithId.Type)
			continue
		}

		// If this command has a request ID, record it as processed
		if cmdWithId.RequestID != "" {
			s.recordProcessedRequest(cmdWithId.RequestID, cmdWithId.Command)
		}

		// If this is a command we're waiting for, send the result
		if cmdWithId.OpID != "" {
			s.mu.Lock()
			if ch, ok := s.pendingOps[cmdWithId.OpID]; ok {
				ch <- cmdWithId.Command
				delete(s.pendingOps, cmdWithId.OpID)
				s.logger.Info("[RaftKVServer] Sent result to pending operation", "opID", cmdWithId.OpID)
				close(ch)
			}
			s.mu.Unlock()
		}
	}
}

// Start starts the HTTP server on the given address
func (s *RaftKVServer) Start(addr string) error {
	s.httpAddr = addr
	server := &http.Server{
		Addr:    addr,
		Handler: s,
	}

	// Store the server instance so we can shut it down later
	s.httpServer = server

	return server.ListenAndServe()
}

// Stop gracefully shuts down the HTTP server and the Raft node
func (s *RaftKVServer) Stop(ctx context.Context) error {
	s.logger.Info("[RaftKVServer] Shutting down server")

	// First, shut down the HTTP server
	var httpErr error
	if s.httpServer != nil {
		s.logger.Info("[RaftKVServer] Shutting down HTTP server")
		httpErr = s.httpServer.Shutdown(ctx)
		if httpErr != nil {
			s.logger.Error("[RaftKVServer] Error shutting down HTTP server", "error", httpErr)
		}
	}

	// Then, stop the Raft node
	s.logger.Info("[RaftKVServer] Stopping Raft node")
	s.raftNode.Stop()

	// Return the HTTP error if there was one
	return httpErr
}
