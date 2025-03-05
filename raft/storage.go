package raft

// Storage interface defines how Raft interacts with persistent storage
type Storage interface {
	SaveState(term int, votedFor string) error
	LoadState() (term int, votedFor string, err error)

	AppendLogEntries(entries []LogEntry) error
	GetLogEntries(startIndex, endIndex int) ([]LogEntry, error)
}
