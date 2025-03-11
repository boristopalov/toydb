package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

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
	mu       sync.Mutex
	term     int
	votedFor string
	log      []LogEntry

	basePath string
}

func NewSimpleDiskStorage() *simpleDiskStorage {
	// timestamp the directory name and add UID to avoid collisions
	dir := fmt.Sprintf("raft-%s-%s", time.Now().Format("20060102150405"), uuid.New().String())
	os.MkdirAll(dir, 0755)
	s := &simpleDiskStorage{
		basePath: dir,
	}

	s.LoadState()

	// TODO: maybe flush logs to disk periodically
	// TODO: log compaction
	// go s.flushLogsToDisk(1 * time.Second)

	return s
}

func (s *simpleDiskStorage) stateFilePath() string {
	return s.basePath + ".state"
}

func (s *simpleDiskStorage) logFilePath() string {
	return s.basePath + ".log"
}

func (s *simpleDiskStorage) SaveState(term int, votedFor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.term = term
	s.votedFor = votedFor

	data := fmt.Sprintf("%d\n%s", term, votedFor)
	return os.WriteFile(s.stateFilePath(), []byte(data), 0644)
}

func (s *simpleDiskStorage) LoadState() (term int, votedFor string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.stateFilePath())
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", nil
		}
		return 0, "", err
	}

	_, err = fmt.Sscanf(string(data), "%d\n%s", &s.term, &s.votedFor)
	if err != nil {
		return 0, "", err
	}

	return s.term, s.votedFor, nil
}

func (s *simpleDiskStorage) AppendLogEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.log = append(s.log, entries...)

	// Write all logs to disk
	file, err := os.OpenFile(s.logFilePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if _, err := file.Write(append(data, '\n')); err != nil {
			return err
		}
	}

	return nil
}

func (s *simpleDiskStorage) GetLogEntries(startIndex, endIndex int) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Load logs if they haven't been loaded yet
	if len(s.log) == 0 {
		file, err := os.OpenFile(s.logFilePath(), os.O_RDONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var entry LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				return nil, err
			}
			s.log = append(s.log, entry)
		}
	}

	if startIndex >= len(s.log) {
		return nil, fmt.Errorf("startIndex %d out of bounds (len=%d)", startIndex, len(s.log))
	}

	// Handle special case for retrieving all entries
	if endIndex == -1 {
		return s.log[startIndex:], nil
	}

	if endIndex > len(s.log) {
		return nil, fmt.Errorf("endIndex %d out of bounds (len=%d)", endIndex, len(s.log))
	}

	return s.log[startIndex:endIndex], nil
}
