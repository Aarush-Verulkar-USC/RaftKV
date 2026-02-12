package storage

import (
	"os"
	"testing"

	"github.com/aarush/raft-kv/pkg/raft"
)

func TestBoltStorage_StateRoundtrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBoltStorage(dir, 1)
	if err != nil {
		t.Fatalf("NewBoltStorage failed: %v", err)
	}
	defer s.Close()

	// Save and load state
	if err := s.SaveState(5, 3); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	term, votedFor, err := s.LoadState()
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}
	if term != 5 {
		t.Errorf("expected term 5, got %d", term)
	}
	if votedFor != 3 {
		t.Errorf("expected votedFor 3, got %d", votedFor)
	}
}

func TestBoltStorage_LogRoundtrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBoltStorage(dir, 1)
	if err != nil {
		t.Fatalf("NewBoltStorage failed: %v", err)
	}
	defer s.Close()

	entries := []*raft.LogEntry{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 1, Command: []byte("cmd2")},
		{Index: 3, Term: 2, Command: []byte("cmd3")},
	}

	if err := s.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	// LastIndex
	lastIdx, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex failed: %v", err)
	}
	if lastIdx != 3 {
		t.Errorf("expected last index 3, got %d", lastIdx)
	}

	// LastTerm
	lastTerm, err := s.LastTerm()
	if err != nil {
		t.Fatalf("LastTerm failed: %v", err)
	}
	if lastTerm != 2 {
		t.Errorf("expected last term 2, got %d", lastTerm)
	}

	// GetEntry
	e, err := s.GetEntry(2)
	if err != nil {
		t.Fatalf("GetEntry failed: %v", err)
	}
	if e == nil || string(e.Command) != "cmd2" {
		t.Error("expected entry with command 'cmd2'")
	}

	// GetEntries range
	rangeEntries, err := s.GetEntries(1, 3)
	if err != nil {
		t.Fatalf("GetEntries failed: %v", err)
	}
	if len(rangeEntries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(rangeEntries))
	}

	// TruncateAfter
	if err := s.TruncateAfter(2); err != nil {
		t.Fatalf("TruncateAfter failed: %v", err)
	}
	lastIdx, _ = s.LastIndex()
	if lastIdx != 2 {
		t.Errorf("expected last index 2, got %d", lastIdx)
	}
}

func TestBoltStorage_SnapshotRoundtrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBoltStorage(dir, 1)
	if err != nil {
		t.Fatalf("NewBoltStorage failed: %v", err)
	}
	defer s.Close()

	snapData := []byte("snapshot-data")
	if err := s.SaveSnapshot(snapData, 10, 3); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	data, idx, term, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}
	if string(data) != "snapshot-data" {
		t.Errorf("expected 'snapshot-data', got '%s'", string(data))
	}
	if idx != 10 {
		t.Errorf("expected index 10, got %d", idx)
	}
	if term != 3 {
		t.Errorf("expected term 3, got %d", term)
	}
}

func TestInMemoryStorage(t *testing.T) {
	s := NewInMemoryStorage()

	// State
	s.SaveState(2, 1)
	term, votedFor, _ := s.LoadState()
	if term != 2 || votedFor != 1 {
		t.Errorf("state mismatch: term=%d votedFor=%d", term, votedFor)
	}

	// Entries
	s.AppendEntries([]*raft.LogEntry{
		{Index: 1, Term: 1, Command: []byte("a")},
		{Index: 2, Term: 1, Command: []byte("b")},
	})

	lastIdx, _ := s.LastIndex()
	if lastIdx != 2 {
		t.Errorf("expected last index 2, got %d", lastIdx)
	}

	e, _ := s.GetEntry(1)
	if e == nil || string(e.Command) != "a" {
		t.Error("expected entry 'a'")
	}

	// Truncate
	s.TruncateAfter(1)
	lastIdx, _ = s.LastIndex()
	if lastIdx != 1 {
		t.Errorf("expected last index 1, got %d", lastIdx)
	}
}
