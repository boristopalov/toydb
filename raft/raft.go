package raft

import (
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
)

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int
	Index   int // 1-indexed position in the log
	Command []byte
}

// RaftNode represents a node in the Raft cluster
type raftNode struct {
	mu sync.Mutex

	// Persistent state
	currentTerm int
	votedFor    string
	log         []LogEntry // 0-indexed array, but represents 1-indexed log entries

	// Volatile state
	role        NodeRole
	commitIndex int // Index of the highest committed log entry (1-indexed)
	lastApplied int // Index of the highest log entry in the state machine (1-indexed)

	// Channel to notify external clients of committed entries
	newCommitChan       chan struct{}
	committedValuesChan chan LogEntry

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

	// Running state
	running bool

	logger *slog.Logger
}

// NewRaftNode creates a new Raft node
func NewRaftNode(id string, port string, peerAddrs []string, storage Storage, logger *slog.Logger) *raftNode {
	node := &raftNode{
		id:                  id,
		peerAddrs:           peerAddrs,
		storage:             storage,
		role:                Follower,
		currentTerm:         0,
		votedFor:            "",
		log:                 make([]LogEntry, 0),
		commitIndex:         0, // 0 means no entries committed (1-indexed)
		lastApplied:         0, // 0 means no entries applied (1-indexed)
		nextIndex:           make(map[string]int),
		matchIndex:          make(map[string]int),
		resetChan:           make(chan struct{}, 1), // Buffer of 1 to avoid blocking
		stopChan:            make(chan struct{}),
		commandChan:         make(chan struct{}, 1), // Buffer of 1 to avoid blocking
		peerClients:         make(map[string]RaftClient),
		newCommitChan:       make(chan struct{}, 1), // Buffer of 1 to avoid blocking
		committedValuesChan: make(chan LogEntry),
		clientChannels:      make(map[string]chan LogEntry),
		clientMu:            sync.RWMutex{},
		running:             false,
		port:                port,
		logger:              logger,
	}

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

// Stop stops the Raft node
func (node *raftNode) Stop() {
	node.logger.Info("Stopping Raft node", "id", node.id)

	// First signal all goroutines to stop without holding the lock
	// This allows any goroutines that might be waiting on the lock to proceed
	close(node.stopChan)

	// Now acquire the lock to update state
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.running {
		node.logger.Info("Raft node already stopped", "id", node.id)
		return
	}

	// Stop the RPC server
	if node.rpcServer != nil {
		node.rpcServer.Stop()
	}

	// Close the newCommitChan to signal listenForNewCommits to exit
	// Only close if not already closed
	select {
	case <-node.newCommitChan:
		// Channel already closed or drained
	default:
		close(node.newCommitChan)
	}

	// Mark as not running
	node.running = false

	node.logger.Info("Raft node stopped", "id", node.id)
}

func (node *raftNode) GetId() string {
	return node.id
}

func (node *raftNode) SubmitCommand(command []byte) {
	node.mu.Lock()

	if node.role != Leader {
		node.logger.Error("[RaftNode] SubmitCommand failed", "node", node.id, "error", "not a leader")
		node.mu.Unlock()
		return
	}

	node.logger.Info("[Leader] Submitting command", "node", node.id, "command", command)
	// Set the Index field to the 1-indexed position in the log
	newEntry := LogEntry{
		Term:    node.currentTerm,
		Index:   len(node.log) + 1, // 1-indexed position
		Command: command,
	}
	node.log = append(node.log, newEntry)
	node.mu.Unlock()

	select {
	case node.commandChan <- struct{}{}:
		// Successfully sent command notification
	default:
		// Channel is full, log a warning but don't block
		node.logger.Warn("[Leader] Command channel full, AppendEntries might be delayed", "node", node.id)
	}
	node.storage.AppendLogEntries([]LogEntry{newEntry})
}

func (node *raftNode) SubmitCommandBatch(commands [][]byte) {
	if len(commands) == 0 {
		return // Nothing to do
	}

	node.mu.Lock()

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
	node.mu.Unlock()

	// Notify about new commands (only once for the whole batch)
	select {
	case node.commandChan <- struct{}{}:
		// Successfully sent command notification
	default:
		// Channel is full, log a warning but don't block
		node.logger.Warn("[Leader] Command channel full, AppendEntries might be delayed", "node", node.id, "commandCount", len(commands))
	}

	// Persist all entries at once
	node.storage.AppendLogEntries(newEntries)
}

func (node *raftNode) listenForNewCommits() {
	for {
		select {
		case _, ok := <-node.newCommitChan:
			if !ok {
				// Channel closed, exit the goroutine
				node.logger.Info("[RaftNode] newCommitChan closed, exiting listenForNewCommits", "node", node.id)
				return
			}

			node.mu.Lock()
			node.logger.Info("[RaftNode] New commits detected", "node", node.id, "commitIndex", node.commitIndex)

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

			// Send committed entries to commit channel
			for _, entry := range entriesToApply {
				node.logger.Info("[RaftNode] Sending committed entry to commit channel", "node", node.id, "index", entry.Index)
				// The Index field should already be set correctly when the entry was created
				select {
				case node.committedValuesChan <- LogEntry{
					Term:    entry.Term,
					Index:   entry.Index, // Already 1-indexed
					Command: entry.Command,
				}:
					// Successfully sent
				case <-node.stopChan:
					// Node is shutting down, exit
					node.logger.Info("[RaftNode] Shutdown detected during commit, exiting listenForNewCommits", "node", node.id)
					return
				}
			}

		case <-node.stopChan:
			// Node is shutting down
			node.logger.Info("[RaftNode] Shutdown detected, exiting listenForNewCommits", "node", node.id)
			return
		}
	}
}

func (node *raftNode) GetCommitChan() <-chan LogEntry {
	return node.committedValuesChan
}
