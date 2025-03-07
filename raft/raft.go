package raft

import (
	"log/slog"
	"sync"
)

// RaftNodeInterface combines all the interfaces that a RaftNode should implement
// TODO: Test this
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

	// Leader state -- only used if role is Leader
	nextIndex  map[string]int // Next index to send to each follower
	matchIndex map[string]int // Highest index known to be replicated (not just sent), i.e. follower has accepted the entry to the leader

	// Node information
	id      string
	peers   []string
	storage Storage

	rpcClient *RaftRPCClient
	rpcServer *RaftRPCServer

	// Election timer channels
	resetChan chan struct{} // Signal to reset election timer
	stopChan  chan struct{} // Signal to stop election timer

	// Running state
	running bool

	logger *slog.Logger
}

// NewRaftNode creates a new Raft node
func NewRaftNode(id string, peers []string, storage Storage, logger *slog.Logger) *raftNode {
	node := &raftNode{
		id:          id,
		peers:       peers,
		storage:     storage,
		role:        Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
		resetChan:   make(chan struct{}, 1), // Buffer of 1 to avoid blocking
		stopChan:    make(chan struct{}),
		running:     false,
		logger:      logger,
	}

	// Load persistent state if available
	term, votedFor, err := storage.LoadState()
	if err == nil {
		node.currentTerm = term
		node.votedFor = votedFor
	}

	return node
}

// Start starts the Raft node
func (node *raftNode) Start() {
	node.logger.Info("Starting Raft node", "id", node.id)
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.running {
		return
	}

	node.running = true

	// Start election timer
	go node.startElectionTimer()
}

// Stop stops the Raft node
func (node *raftNode) Stop() {
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.running {
		return
	}

	node.running = false

	// Signal all goroutines to stop
	close(node.stopChan)

	// Create new channels for next start
	node.resetChan = make(chan struct{}, 1)
	node.stopChan = make(chan struct{})
}

func (node *raftNode) GetId() string {
	return node.id
}
