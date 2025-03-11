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
	RequestVoter
	AppendEntriesSender
	ElectionHandler
	Start()
	Stop()
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
	Index   int
	Command []byte
}

// RaftNode represents a node in the Raft cluster
type raftNode struct {
	mu sync.Mutex

	// Persistent state
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state
	role        NodeRole
	commitIndex int // Index of the highest committed log entry
	lastApplied int // Index of the highest log entry in the state machine

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
		commitIndex:         -1,
		lastApplied:         -1,
		nextIndex:           make(map[string]int),
		matchIndex:          make(map[string]int),
		resetChan:           make(chan struct{}, 1), // Buffer of 1 to avoid blocking
		stopChan:            make(chan struct{}),
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
		node.logger.Error("Failed to load state", "error", err)
	} else {
		node.currentTerm = term
		node.votedFor = votedFor
	}
	// Load log entries from disk
	logEntries, err := node.storage.GetLogEntries(0, -1)
	if err != nil {
		node.logger.Error("Failed to load log entries", "error", err)
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
	go node.broadcastCommittedEntries()
	node.running = true
}

func (node *raftNode) ConnectToPeers() {
	// Try 5 times to connect to each peer
	for _, peerAddr := range node.peerAddrs {
		for range 5 {
			rpcClient, err := NewRaftRPCClient(peerAddr, node.logger)
			if err != nil {
				node.logger.Error("Failed to create RPC client", "node", node.id, "peerAddr", peerAddr, "error", err)
				time.Sleep(200 * time.Millisecond)
				continue
			}
			node.logger.Info("Connected to peer", "node", node.id, "peerAddr", peerAddr)
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
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.running {
		node.logger.Info("Raft node already stopped", "id", node.id)
		return
	}

	node.logger.Info("Stopping Raft node", "id", node.id)

	// Signal all goroutines to stop
	close(node.stopChan)
	// Stop the RPC server
	node.rpcServer.Stop()
	node.running = false
}

func (node *raftNode) GetId() string {
	return node.id
}

func (node *raftNode) SubmitCommand(command []byte) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.role != Leader {
		node.logger.Error("SubmitCommand failed", "node", node.id, "error", "not a leader")
		return
	}

	node.logger.Info("Submitting command", "node", node.id, "command", command)
	node.log = append(node.log, LogEntry{Term: node.currentTerm, Command: command})
}

// Subscribe registers a client and returns a channel for committed entries
func (node *raftNode) Subscribe(clientID string) <-chan LogEntry {
	clientChan := make(chan LogEntry, 100)

	node.clientMu.Lock()
	node.clientChannels[clientID] = clientChan
	node.clientMu.Unlock()

	return clientChan
}

// Unsubscribe removes a client subscription
func (node *raftNode) Unsubscribe(clientID string) {
	node.clientMu.Lock()
	defer node.clientMu.Unlock()

	if ch, ok := node.clientChannels[clientID]; ok {
		close(ch)
		delete(node.clientChannels, clientID)
	}
}

func (node *raftNode) broadcastCommittedEntries() {
	for entry := range node.committedValuesChan {
		node.clientMu.RLock()
		for clientID, clientChan := range node.clientChannels {
			select {
			case clientChan <- entry:
				// Entry sent successfully
			default:
				node.logger.Warn("Client channel full, dropping entry", "clientID", clientID)
				// Could implement a policy to disconnect slow clients
			}
		}
		node.clientMu.RUnlock()
	}
}

func (node *raftNode) listenForNewCommits() {
	for range node.newCommitChan {
		node.logger.Info("[Leader] New commits detected", "node", node.id, "commitIndex", node.commitIndex)
		node.mu.Lock()
		currentTerm := node.currentTerm

		// lastApplied is the index of the last log entry that was applied to the state machine
		// Before this batch of commits
		lastApplied := node.lastApplied
		var entriesToApply []LogEntry
		if node.commitIndex > node.lastApplied {
			entriesToApply = node.log[node.lastApplied+1 : node.commitIndex+1]
			node.lastApplied = node.commitIndex
		}
		node.mu.Unlock()

		// Send committed entries to commmit channel
		for i, entry := range entriesToApply {
			node.committedValuesChan <- LogEntry{
				Term:    currentTerm,
				Index:   lastApplied + i + 1,
				Command: entry.Command,
			}
		}
	}
	node.logger.Info("listenForNewCommits finished", "node", node.id)
}
