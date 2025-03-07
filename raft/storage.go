package raft

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// Storage interface provides raft nodes with persistence
type Storage interface {
	SaveState(term int, votedFor string) error
	LoadState() (term int, votedFor string, err error)

	AppendLogEntries(entries []LogEntry) error
	GetLogEntries(startIndex, endIndex int) ([]LogEntry, error)
}

// simpleDiskStorage is a simple disk storage implementation for Raft
type simpleDiskStorage struct {
	mu sync.Mutex

	term     int
	votedFor string
	log      []LogEntry
	logFile  string
}

func NewSimpleDiskStorage() *simpleDiskStorage {
	// create a random log file name
	logFile := fmt.Sprintf("raft-%s.log", uuid.New().String())
	s := &simpleDiskStorage{
		logFile: logFile,
	}

	// go s.flushLogsToDisk(1 * time.Second)

	return s
}

func (s *simpleDiskStorage) SaveState(term int, votedFor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.term = term
	s.votedFor = votedFor
	return nil
}

func (s *simpleDiskStorage) LoadState() (term int, votedFor string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.votedFor, nil
}

func (s *simpleDiskStorage) AppendLogEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.log = append(s.log, entries...)
	return nil
}

func (s *simpleDiskStorage) GetLogEntries(startIndex, endIndex int) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log[startIndex:endIndex], nil
}

// periodically flush logs to disk
// func (s *simpleDiskStorage) flushLogsToDisk(interval time.Duration) {
// 	for {
// 		time.Sleep(interval)
// 		data, err := json.Marshal(s.log)
// 		if err != nil {
// 			slog.Error("failed to marshal log", "error", err)
// 			continue
// 		}
// 		os.WriteFile(s.logFile, data, 0644)
// 		slog.Info("flushed logs to disk")
// 		s.mu.Lock()
// 		s.log = nil
// 		s.mu.Unlock()
// 	}
// }
