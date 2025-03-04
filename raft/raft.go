package raft

import (
	"sync"
)

// NodeRole represents the state of a Raft node
type NodeRole int

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
type RaftNode struct {
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
}

// Storage interface defines how Raft interacts with persistent storage
type Storage interface {
	SaveState(term int, votedFor string) error
	LoadState() (term int, votedFor string, err error)

	AppendLogEntries(entries []LogEntry) error
	GetLogEntries(startIndex, endIndex int) ([]LogEntry, error)
}

// NewRaftNode creates a new Raft node
func NewRaftNode(id string, peers []string, storage Storage) *RaftNode {
	node := &RaftNode{
		id:          id,
		peers:       peers,
		storage:     storage,
		role:        Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         []LogEntry{},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
	}

	// Load persistent state if available
	term, votedFor, err := storage.LoadState()
	if err == nil {
		node.currentTerm = term
		node.votedFor = votedFor
	}

	return node
}
