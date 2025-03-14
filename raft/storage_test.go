package raft

import (
	"os"
	"testing"
)

func TestSimpleDiskStorage_Persistence(t *testing.T) {
	// Create temp directory for test files
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Test SaveState and LoadState
	t.Run("state persistence", func(t *testing.T) {
		// Create storage with known path
		storage := &simpleDiskStorage{
			basePath: tmpDir + "/state",
		}
		defer storage.close()

		err := storage.SaveState(5, "node1")
		if err != nil {
			t.Fatal(err)
		}

		// Create new storage instance to verify persistence
		storage2 := &simpleDiskStorage{
			basePath: tmpDir + "/state",
		}
		defer storage2.close()

		term, votedFor, err := storage2.LoadState()
		if err != nil {
			t.Fatal(err)
		}
		if term != 5 || votedFor != "node1" {
			t.Errorf("got term=%d, votedFor=%s; want term=5, votedFor=node1", term, votedFor)
		}
	})

	// Test log persistence
	t.Run("log persistence", func(t *testing.T) {
		// Create storage with known path
		storage := &simpleDiskStorage{
			basePath: tmpDir + "/log1",
		}
		defer storage.close()

		entries := []LogEntry{{Term: 1, Command: []byte("test")}}
		err := storage.AppendLogEntries(entries)
		if err != nil {
			t.Fatal(err)
		}

		// Create new storage instance to verify persistence
		storage2 := &simpleDiskStorage{
			basePath: tmpDir + "/log1",
		}
		defer storage2.close()

		retrieved, err := storage2.GetLogEntries(0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(retrieved) != 1 || retrieved[0].Term != 1 {
			t.Errorf("got entries=%v; want entries with term=1", retrieved)
		}
	})

	t.Run("get all log entries", func(t *testing.T) {
		// Create storage with known path and a clean log file
		storage := &simpleDiskStorage{
			basePath: tmpDir + "/log2",
		}
		defer storage.close()

		// Test getting all entries
		entries := []LogEntry{{Term: 1, Command: []byte("test")}, {Term: 2, Command: []byte("test2")}}
		err := storage.AppendLogEntries(entries)
		if err != nil {
			t.Fatal(err)
		}
		retrieved, err := storage.GetLogEntries(0, -1)
		if err != nil {
			t.Fatal(err)
		}
		if len(retrieved) != 2 {
			t.Errorf("got %d entries, want 2 entries", len(retrieved))
		}

		// Test getting partial entries
		retrieved, err = storage.GetLogEntries(1, -1)
		if err != nil {
			t.Fatal(err)
		}
		if len(retrieved) != 1 {
			t.Errorf("got %d entries, want 1 entry", len(retrieved))
		}
	})
}

// TestMultipleAppendLogEntries specifically tests the issue with appending multiple log entries
func TestMultipleAppendLogEntries(t *testing.T) {
	// Create temp directory for test files
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage with known path
	storage := &simpleDiskStorage{
		basePath: tmpDir,
	}
	defer storage.close()

	// Append multiple entries in a single call
	entries := []LogEntry{
		{Term: 1, Index: 0, Command: []byte("command1")},
		{Term: 1, Index: 1, Command: []byte("command2")},
	}

	err = storage.AppendLogEntries(entries)
	if err != nil {
		t.Fatal(err)
	}

	// Retrieve all entries
	retrieved, err := storage.GetLogEntries(0, -1)
	if err != nil {
		t.Fatal(err)
	}

	// Verify we got both entries
	if len(retrieved) != 2 {
		t.Errorf("got %d entries, want 2 entries", len(retrieved))
	}

	// Verify the content of each entry
	if string(retrieved[0].Command) != "command1" || retrieved[0].Term != 1 {
		t.Errorf("first entry mismatch: got %v, want Term=1, Command=command1", retrieved[0])
	}

	if string(retrieved[1].Command) != "command2" || retrieved[1].Term != 1 {
		t.Errorf("second entry mismatch: got %v, want Term=1, Command=command2", retrieved[1])
	}
}
