package raft

import (
	"encoding/gob"
	"fmt"
	"io"
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
	basePath string

	// For log entries
	logFile *os.File
	encoder *gob.Encoder
	closed  bool

	// In-memory cache of log entries
	logEntries []LogEntry

	// Sync control
	lastSyncTime     time.Time
	syncInterval     time.Duration
	pendingWrites    int
	maxPendingWrites int
}

func NewSimpleDiskStorage() *simpleDiskStorage {
	// timestamp the directory name and add UID to avoid collisions
	dir := fmt.Sprintf("raft-%s-%s", time.Now().Format("20060102150405"), uuid.New().String())
	os.MkdirAll(dir, 0755)
	s := &simpleDiskStorage{
		basePath:         dir,
		logEntries:       make([]LogEntry, 0, 1000), // Pre-allocate some capacity
		syncInterval:     250 * time.Millisecond,    // Sync at most every 100ms
		lastSyncTime:     time.Now(),
		maxPendingWrites: 500, // Sync after 500 writes
	}

	s.LoadState()

	// Load existing log entries into memory
	entries, err := s.readLogEntriesFromDisk()
	if err == nil {
		s.logEntries = entries
	}

	// Ensure log file is open for writing
	s.ensureLogFileOpen()

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

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	s.term = term
	s.votedFor = votedFor

	data := fmt.Sprintf("%d\n%s", term, votedFor)
	return os.WriteFile(s.stateFilePath(), []byte(data), 0644)
}

func (s *simpleDiskStorage) LoadState() (term int, votedFor string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, "", fmt.Errorf("storage is closed")
	}

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

// ensureLogFileOpen ensures the log file is open and the encoder is initialized
// Caller must hold the mutex
func (s *simpleDiskStorage) ensureLogFileOpen() error {
	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	if s.logFile != nil {
		return nil
	}

	// Open the log file in append mode
	logFile, err := os.OpenFile(s.logFilePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	s.logFile = logFile
	s.encoder = gob.NewEncoder(logFile)
	return nil
}

func (s *simpleDiskStorage) AppendLogEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Make sure the log file is open
	if err := s.ensureLogFileOpen(); err != nil {
		return err
	}

	// Write each entry using the existing encoder
	for _, entry := range entries {
		if err := s.encoder.Encode(entry); err != nil {
			return err
		}

		// Add to in-memory cache
		s.logEntries = append(s.logEntries, entry)
	}

	// Increment pending writes counter
	s.pendingWrites += len(entries)

	// Determine if we should sync to disk
	shouldSync := s.pendingWrites >= s.maxPendingWrites ||
		time.Since(s.lastSyncTime) >= s.syncInterval

	// Only sync to disk periodically to improve performance
	if shouldSync {
		err := s.logFile.Sync()
		if err != nil {
			return err
		}
		s.lastSyncTime = time.Now()
		s.pendingWrites = 0
	}

	return nil
}

func (s *simpleDiskStorage) GetLogEntries(startIndex, endIndex int) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	// Use in-memory cache instead of reading from disk
	entries := s.logEntries

	// Check bounds
	if len(entries) == 0 || startIndex >= len(entries) {
		return nil, fmt.Errorf("startIndex %d out of bounds (len=%d)", startIndex, len(entries))
	}

	// Handle special case for retrieving all entries
	if endIndex == -1 {
		return entries[startIndex:], nil
	}

	if endIndex > len(entries) {
		return nil, fmt.Errorf("endIndex %d out of bounds (len=%d)", endIndex, len(entries))
	}

	return entries[startIndex:endIndex], nil
}

// readLogEntriesFromDisk reads all log entries from disk
// Caller must hold the mutex
func (s *simpleDiskStorage) readLogEntriesFromDisk() ([]LogEntry, error) {
	// Check if the log file exists
	_, err := os.Stat(s.logFilePath())
	if os.IsNotExist(err) {
		// No log file yet, return empty log
		return make([]LogEntry, 0), nil
	}

	// Open a separate file handle for reading
	file, err := os.OpenFile(s.logFilePath(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create a decoder to read the entries
	decoder := gob.NewDecoder(file)

	// Read all entries
	var entries []LogEntry
	for {
		var entry LogEntry
		err := decoder.Decode(&entry)
		if err == io.EOF {
			break // End of file
		}
		if err != nil {
			return nil, fmt.Errorf("error decoding log entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// close closes the storage and releases any resources
func (s *simpleDiskStorage) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil // Already closed
	}

	s.closed = true

	if s.logFile != nil {
		// Make sure to sync any pending writes before closing
		if s.pendingWrites > 0 {
			s.logFile.Sync()
		}

		err := s.logFile.Close()
		s.logFile = nil
		s.encoder = nil
		return err
	}

	return nil
}
