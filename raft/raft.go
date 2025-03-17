package raft

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// TODO: command handler for leaders
// command == leader appends the command to the log with current term
// and then sends AppendEntries to all peers in parallel
// if any peer rejects, leader retries with updated nextIndex for each peer

// RaftNodeInterface combines all the interfaces that a RaftNode should implement
type RaftNode interface {
	StartElection()
	Start()
	Stop()
	GetId() string
	SubmitCommand(command []byte)
	SubmitCommandBatch(commands [][]byte)
	GetCommitChan() <-chan LogEntry
	ConnectToPeers()
	GetLeaderId() string
}

// NodeRole represents the state of a Raft node
type NodeRole int

func (r NodeRole) String() string {
	return []string{"Follower", "Candidate", "Leader"}[r]
}

const (
	Follower NodeRole = iota
	Candidate
	Leader

	// Command batching settings
	batchInterval = 6 * time.Millisecond // Maximum time to wait before submitting a batch
	maxBatchSize  = 100                  // Maximum number of commands in a batch
)

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int
	Index   int // 1-indexed position in the log
	Command []byte
}

// commandBatcher collects commands and submits them in batches
type commandBatcher struct {
	mu            sync.Mutex
	commands      [][]byte
	timer         *time.Timer
	node          *raftNode
	batchInterval time.Duration
	maxBatchSize  int
	closed        bool
	pendingBatch  bool
	stopChan      chan struct{}
}

// newCommandBatcher creates a new command batcher
func newCommandBatcher(node *raftNode, batchInterval time.Duration, maxBatchSize int) *commandBatcher {
	batcher := &commandBatcher{
		commands:      make([][]byte, 0, maxBatchSize),
		node:          node,
		batchInterval: batchInterval,
		maxBatchSize:  maxBatchSize,
		stopChan:      make(chan struct{}),
	}

	// Start the timer with a very long duration initially (it will be reset when commands are added)
	// basically the batcher remains idle until work arrives
	batcher.timer = time.NewTimer(24 * time.Hour)

	// Start the background goroutine that processes batches
	go batcher.processBatches()

	return batcher
}

// add adds a command to the batch
func (b *commandBatcher) add(command []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	// Add the command to the batch
	b.commands = append(b.commands, command)

	// If this is the first command in the batch, reset the timer
	if len(b.commands) == 1 {
		// Stop the timer if it's running
		if !b.timer.Stop() {
			// If the timer has already fired, drain the channel
			select {
			case <-b.timer.C:
			default:
			}
		}
		// Reset the timer
		b.timer.Reset(b.batchInterval)
	}

	// If we've reached the maximum batch size, submit immediately
	if len(b.commands) >= b.maxBatchSize {
		b.submitBatch()
	}
}

// submitBatch submits the current batch of commands
// Caller must hold the mutex
func (b *commandBatcher) submitBatch() {
	if len(b.commands) == 0 || b.pendingBatch {
		return
	}

	// Mark that we're processing a batch
	b.pendingBatch = true

	// Make a copy of the commands
	commands := make([][]byte, len(b.commands))
	copy(commands, b.commands)

	// Clear the batch
	b.commands = b.commands[:0]

	// Submit the batch in a separate goroutine to avoid blocking
	go func() {
		// Check if we're closed before submitting
		select {
		case <-b.stopChan:
			// We're closed, don't submit
			b.mu.Lock()
			b.pendingBatch = false
			b.mu.Unlock()
			return
		default:
			// Not closed, continue
		}

		b.node.SubmitCommandBatch(commands)

		// Mark that we're done processing this batch
		b.mu.Lock()
		b.pendingBatch = false

		// Check if more commands accumulated while we were processing
		if len(b.commands) > 0 && !b.closed {
			b.submitBatch()
		}
		b.mu.Unlock()
	}()
}

// processBatches processes batches when the timer expires
func (b *commandBatcher) processBatches() {
	for {
		select {
		case <-b.timer.C:
			// Submit the current batch
			b.mu.Lock()
			if b.closed {
				b.mu.Unlock()
				return
			}
			b.submitBatch()
			// Reset the timer with a very long duration (it will be reset when commands are added)
			b.timer.Reset(24 * time.Hour)
			b.mu.Unlock()
		case <-b.stopChan:
			return
		}
	}
}

// close closes the batcher and submits any pending commands
func (b *commandBatcher) close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	b.closed = true
	close(b.stopChan)

	// Stop the timer to prevent it from firing after we're closed
	if !b.timer.Stop() {
		// Drain the channel if the timer has already fired
		select {
		case <-b.timer.C:
		default:
		}
	}

	// Submit any pending commands
	if len(b.commands) > 0 && !b.pendingBatch {
		commands := make([][]byte, len(b.commands))
		copy(commands, b.commands)
		b.commands = nil

		// Submit synchronously since we're shutting down
		b.node.SubmitCommandBatch(commands)
	}
}

// RaftNode represents a node in the Raft cluster
type raftNode struct {
	mu sync.Mutex

	// Persistent state
	currentTerm int
	votedFor    string
	log         []LogEntry // 0-indexed array, but represents 1-indexed log entries

	// Volatile state
	role            NodeRole
	commitIndex     int // Index of the highest committed log entry (1-indexed)
	lastApplied     int // Index of the highest log entry in the state machine (1-indexed)
	currentLeaderId string

	// Channel to notify external clients of committed entries
	newCommitChan          chan struct{}
	processingNewCommitsWg sync.WaitGroup
	committedValuesChan    chan LogEntry

	// Client subscriptions
	clientChannels map[string]chan LogEntry
	clientMu       sync.RWMutex

	// Leader state -- only used if role is Leader
	nextIndex  map[string]int // Next index to send to each follower
	matchIndex map[string]int // Highest index known to be replicated (not just sent), i.e. follower has accepted the entry to the leader

	// Node information
	id        string
	peerAddrs []string
	storage   Storage

	peerClients map[string]RaftClient // RPC clients for each peer ID
	port        string
	rpcServer   *RaftRPCServer

	// Election timer channels
	resetChan chan struct{} // Signal to reset election timer
	stopChan  chan struct{} // Signal to stop election timer

	// Command channel to tell the leader to send AppendEntries
	commandChan chan struct{}

	// Command batcher
	batcher *commandBatcher

	// Running state
	running bool

	logger *slog.Logger
}

// NewRaftNode creates a new Raft node
func NewRaftNode(id string, port string, peerAddrs []string, storage Storage, logger *slog.Logger) *raftNode {
	node := &raftNode{
		id:                     id,
		peerAddrs:              peerAddrs,
		storage:                storage,
		role:                   Follower,
		currentTerm:            0,
		votedFor:               "",
		log:                    make([]LogEntry, 0),
		commitIndex:            0, // 0 means no entries committed (1-indexed)
		lastApplied:            0, // 0 means no entries applied (1-indexed)
		nextIndex:              make(map[string]int),
		matchIndex:             make(map[string]int),
		resetChan:              make(chan struct{}, 1), // Buffer of 1 to avoid blocking
		stopChan:               make(chan struct{}),
		commandChan:            make(chan struct{}, 1), // Buffer of 1 to avoid blocking
		peerClients:            make(map[string]RaftClient),
		newCommitChan:          make(chan struct{}, 10), // Buffer of 100 to avoid blocking
		processingNewCommitsWg: sync.WaitGroup{},
		committedValuesChan:    make(chan LogEntry),
		clientChannels:         make(map[string]chan LogEntry),
		clientMu:               sync.RWMutex{},
		running:                false,
		port:                   port,
		logger:                 logger,
	}

	// Initialize the command batcher
	node.batcher = newCommandBatcher(node, batchInterval, maxBatchSize)

	term, votedFor, err := node.storage.LoadState()
	if err != nil {
		node.logger.Error("[RaftNode] Failed to load state", "error", err)
	} else {
		node.currentTerm = term
		node.votedFor = votedFor
	}
	// Load log entries from disk
	logEntries, err := node.storage.GetLogEntries(0, -1)
	if err != nil {
		node.logger.Error("[RaftNode] Failed to load log entries", "error", err)
	} else {
		node.log = logEntries
	}

	return node
}

// Start starts the Raft node server and sets the node's state to running
func (node *raftNode) Start() {
	rpcServer := NewRaftRPCServer(node, node.port, node.logger)
	node.rpcServer = rpcServer

	node.logger.Info("Starting Raft node", "id", node.id)
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.running {
		return
	}

	go node.rpcServer.Start()

	node.processingNewCommitsWg.Add(1)
	go node.listenForNewCommits()
	node.running = true
}

func (node *raftNode) ConnectToPeers() {
	// Try 5 times to connect to each peer
	for _, peerAddr := range node.peerAddrs {
		for range 5 {
			rpcClient, err := NewRaftRPCClient(peerAddr, node.logger)
			if err != nil {
				node.logger.Error("[RaftNode] Failed to create RPC client", "node", node.id, "peerAddr", peerAddr, "error", err)
				time.Sleep(200 * time.Millisecond)
				continue
			}
			node.logger.Info("[RaftNode] Connected to peer", "node", node.id, "peerAddr", peerAddr)
			node.peerClients[peerAddr] = rpcClient

			// Load persistent state if available
			term, votedFor, err := node.storage.LoadState()
			if err == nil {
				node.currentTerm = term
				node.votedFor = votedFor
			}
			break
		}
	}
}

func (node *raftNode) StartElectionTimer() {
	go node.startElectionTimer()
}

// Stop stops the Raft node with a graceful shutdown sequence
func (node *raftNode) Stop() {
	node.logger.Info("Stopping Raft node", "id", node.id)

	// Create a context with timeout for the entire shutdown process
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	node.mu.Lock()
	// First check if we're already stopped
	if !node.running {
		node.logger.Info("Raft node already stopped", "id", node.id)
		node.mu.Unlock()
		return
	}

	// Mark as not running first to prevent new operations
	node.running = false
	node.mu.Unlock()

	// Signal all goroutines to stop
	close(node.stopChan)

	// Phase 1: Stop accepting new commands
	node.logger.Info("[Shutdown] Phase 1: Stopping command processing", "id", node.id)

	// Close the command batcher
	if node.batcher != nil {
		node.batcher.close()
	}

	// Phase 2: Wait for in-flight operations to complete with timeout
	node.logger.Info("[Shutdown] Phase 2: Waiting for in-flight operations", "id", node.id)

	// Create a done channel for timeout handling
	done := make(chan struct{})

	// Wait for in-flight operations in a separate goroutine
	go func() {
		// Wait a moment for goroutines to notice the stop signal
		time.Sleep(100 * time.Millisecond)
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		// In-flight operations completed
	case <-ctx.Done():
		node.logger.Warn("[Shutdown] Timeout waiting for in-flight operations", "id", node.id)
	}

	// Phase 3: Close network connections
	node.logger.Info("[Shutdown] Phase 3: Closing network connections", "id", node.id)

	node.mu.Lock()
	// Close all RPC client connections
	for peerAddr, client := range node.peerClients {
		node.logger.Info("Closing RPC client connection", "id", node.id, "peer", peerAddr)
		if err := client.Close(); err != nil {
			node.logger.Error("Error closing RPC client connection", "id", node.id, "peer", peerAddr, "error", err)
		}
	}
	node.mu.Unlock()

	// Stop the RPC server
	if node.rpcServer != nil {
		node.logger.Info("Stopping Raft RPC server", "id", node.id)
		node.rpcServer.Stop()
	}

	// Phase 4: Close internal channels and wait for goroutines
	node.logger.Info("[Shutdown] Phase 4: Closing internal channels", "id", node.id)

	// Close the newCommitChan to signal listenForNewCommits to exit
	close(node.newCommitChan)

	// Wait for the commit processing goroutine with timeout
	waitDone := make(chan struct{})
	go func() {
		node.processingNewCommitsWg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		node.logger.Info("ProcessingNewCommitsWg finished", "id", node.id)
	case <-ctx.Done():
		node.logger.Warn("[Shutdown] Timeout waiting for commit processing", "id", node.id)
	}

	// Phase 5: Final cleanup
	node.logger.Info("[Shutdown] Phase 5: Final cleanup", "id", node.id)

	// Close the committedValuesChan last, after all producers have stopped
	close(node.committedValuesChan)

	node.logger.Info("Raft node stopped", "id", node.id)
}

func (node *raftNode) GetId() string {
	return node.id
}

func (node *raftNode) SubmitCommand(command []byte) {
	node.mu.Lock()
	// Check if node is still running
	if !node.running {
		node.logger.Warn("[RaftNode] SubmitCommand called on stopped node", "node", node.id)
		node.mu.Unlock()
		return
	}

	isLeader := node.role == Leader

	// Fast path for single commands when the system is not under high load
	// This bypasses the batcher for better latency in low-concurrency scenarios
	if isLeader && node.batcher != nil && len(node.commandChan) == 0 {
		// Create a new log entry directly
		newEntry := LogEntry{
			Term:    node.currentTerm,
			Index:   len(node.log) + 1, // 1-indexed position
			Command: command,
		}
		node.log = append(node.log, newEntry)
		node.mu.Unlock()

		// Notify about the new command
		select {
		case node.commandChan <- struct{}{}:
			// Successfully sent command notification
		case <-node.stopChan:
			// Node is stopping, don't send on commandChan
			return
		default:
			// Channel is full, fall back to batching
			if node.running {
				node.batcher.add(command)
			}
			return
		}

		// Persist the entry
		if err := node.storage.AppendLogEntries([]LogEntry{newEntry}); err != nil {
			node.logger.Error("[RaftNode] Failed to persist log entry", "node", node.id, "error", err)
		}
		return
	}

	node.mu.Unlock()

	if !isLeader {
		node.logger.Error("[RaftNode] SubmitCommand failed", "node", node.id, "error", "not a leader")
		return
	}

	// Add the command to the batcher (fallback path for high concurrency)
	if node.running {
		node.batcher.add(command)
	}
}

// SubmitCommandBatch submits a batch of commands
func (node *raftNode) SubmitCommandBatch(commands [][]byte) {
	if len(commands) == 0 {
		return // Nothing to do
	}

	node.mu.Lock()
	// Check if node is still running
	if !node.running {
		node.logger.Warn("[RaftNode] SubmitCommandBatch called on stopped node", "node", node.id)
		node.mu.Unlock()
		return
	}

	if node.role != Leader {
		node.logger.Error("[RaftNode] SubmitCommandBatch failed", "node", node.id, "error", "not a leader", "commandCount", len(commands))
		node.mu.Unlock()
		return
	}

	node.logger.Info("[Leader] Submitting command batch", "node", node.id, "commandCount", len(commands))

	// Create log entries for all commands
	newEntries := make([]LogEntry, len(commands))
	for i, cmd := range commands {
		// Set the Index field to the 1-indexed position in the log
		newEntries[i] = LogEntry{
			Term:    node.currentTerm,
			Index:   len(node.log) + i + 1, // 1-indexed position
			Command: cmd,
		}
	}

	// Append all entries to the log
	node.log = append(node.log, newEntries...)

	// Store the current log length to check if we need to trigger AppendEntries
	logLength := len(node.log)
	node.mu.Unlock()

	// Persist all entries at once
	if err := node.storage.AppendLogEntries(newEntries); err != nil {
		node.logger.Error("[RaftNode] Failed to persist log entries", "node", node.id, "error", err, "entryCount", len(newEntries))
	}

	// Notify about new commands (only once for the whole batch)
	select {
	case node.commandChan <- struct{}{}:
		// Successfully sent command notification
	case <-node.stopChan:
		// Node is stopping, don't send on commandChan
		return
	default:
		// Channel is full, but we still need to ensure AppendEntries is triggered
		// Check if we need to force an AppendEntries RPC
		node.mu.Lock()
		currentLogLength := len(node.log)
		isLeader := node.role == Leader
		node.mu.Unlock()

		if isLeader && currentLogLength == logLength {
			// The log hasn't changed since we added our entries, and we're still the leader
			// This means our notification might have been lost, so force an AppendEntries
			go func() {
				// Small delay to allow potential in-flight AppendEntries to complete
				time.Sleep(3 * time.Millisecond)

				// Check again if we're still the leader and if AppendEntries is needed
				node.mu.Lock()
				isStillLeader := node.role == Leader && node.running
				node.mu.Unlock()

				if isStillLeader {
					select {
					case node.commandChan <- struct{}{}:
						// Successfully sent command notification
					case <-node.stopChan:
						// Node is stopping
					default:
						// Channel still full, log a warning
						node.logger.Warn("[Leader] Command channel still full, AppendEntries might be delayed",
							"node", node.id, "commandCount", len(commands))
					}
				}
			}()
		}
	}
}

func (node *raftNode) listenForNewCommits() {
	defer node.processingNewCommitsWg.Done()
	node.logger.Info("[RaftNode] listenForNewCommits starting", "node", node.id)
	for range node.newCommitChan {
		node.logger.Info("[RaftNode] listenForNewCommits Acquiring lock", "node", node.id)
		node.mu.Lock()
		node.logger.Info("[RaftNode] listenForNewCommits Acquired lock", "node", node.id)

		// lastApplied is the index of the last log entry that was applied to the state machine
		// Before this batch of commits
		var entriesToApply []LogEntry
		if node.commitIndex > node.lastApplied {
			// For 1-indexed logs, we need to adjust array access
			// lastApplied and commitIndex are 1-indexed, so we subtract 1 for array access
			if node.lastApplied == 0 {
				// No entries applied yet, start from the beginning
				entriesToApply = node.log[:node.commitIndex]
			} else {
				entriesToApply = node.log[node.lastApplied:node.commitIndex]
			}
			node.lastApplied = node.commitIndex
		}
		node.mu.Unlock()
		// node.logger.Info("[RaftNode] listenForNewCommits Released lock", "node", node.id)

		// Send committed entries to commit channel
		for _, entry := range entriesToApply {
			node.logger.Info("[RaftNode] listenForNewCommits sending committed entry to commit channel", "node", node.id)
			// The Index field should already be set correctly when the entry was created
			node.committedValuesChan <- LogEntry{
				Term:    entry.Term,
				Index:   entry.Index, // Already 1-indexed
				Command: entry.Command,
			}
		}
		node.logger.Info("[RaftNode] listenForNewCommits sent committed entries to commit channel", "node", node.id)
	}
	node.logger.Info("[RaftNode] listenForNewCommits finished", "node", node.id)
}

func (node *raftNode) GetCommitChan() <-chan LogEntry {
	return node.committedValuesChan
}

// GetLeaderId returns the ID of the current leader, or an empty string if there is no known leader
func (node *raftNode) GetLeaderId() string {
	node.mu.Lock()
	defer node.mu.Unlock()

	// If this node is the leader, return its own ID
	if node.role == Leader {
		return node.id
	}

	// Otherwise, return the stored leader ID
	return node.currentLeaderId
}
