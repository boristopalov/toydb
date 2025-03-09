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
		id:          id,
		peerAddrs:   peerAddrs,
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
		peerClients: make(map[string]RaftClient),
		running:     false,
		port:        port,
		logger:      logger,
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
