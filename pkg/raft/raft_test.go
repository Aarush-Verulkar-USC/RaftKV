package raft

import (
	"context"
	"sync"
	"testing"
	"time"
)

// mockStorage implements Storage for testing
type mockStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor uint64
	entries  []*LogEntry
	snap     []byte
	snapIdx  uint64
	snapTerm uint64
}

func newMockStorage() *mockStorage {
	return &mockStorage{entries: make([]*LogEntry, 0)}
}

func (s *mockStorage) SaveState(term uint64, votedFor uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	s.votedFor = votedFor
	return nil
}
func (s *mockStorage) LoadState() (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.votedFor, nil
}
func (s *mockStorage) AppendEntries(entries []*LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entries...)
	return nil
}
func (s *mockStorage) GetEntries(start, end uint64) ([]*LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*LogEntry
	for _, e := range s.entries {
		if e.Index >= start && e.Index < end {
			result = append(result, e)
		}
	}
	return result, nil
}
func (s *mockStorage) GetEntry(index uint64) (*LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.Index == index {
			return e, nil
		}
	}
	return nil, nil
}
func (s *mockStorage) TruncateAfter(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var newLog []*LogEntry
	for _, e := range s.entries {
		if e.Index <= index {
			newLog = append(newLog, e)
		}
	}
	s.entries = newLog
	return nil
}
func (s *mockStorage) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return s.snapIdx, nil
	}
	return s.entries[len(s.entries)-1].Index, nil
}
func (s *mockStorage) LastTerm() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return s.snapTerm, nil
	}
	return s.entries[len(s.entries)-1].Term, nil
}
func (s *mockStorage) SaveSnapshot(snap []byte, idx, term uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = snap
	s.snapIdx = idx
	s.snapTerm = term
	return nil
}
func (s *mockStorage) LoadSnapshot() ([]byte, uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap, s.snapIdx, s.snapTerm, nil
}

// mockTransport records RPCs for verification
type mockTransport struct {
	mu            sync.Mutex
	voteResponses map[uint64]*RequestVoteResponse
	aeResponses   map[uint64]*AppendEntriesResponse
	voteCalls     int
	aeCalls       int
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		voteResponses: make(map[uint64]*RequestVoteResponse),
		aeResponses:   make(map[uint64]*AppendEntriesResponse),
	}
}

func (t *mockTransport) RequestVote(ctx context.Context, peerID uint64, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.voteCalls++
	if resp, ok := t.voteResponses[peerID]; ok {
		return resp, nil
	}
	return &RequestVoteResponse{Term: req.Term, VoteGranted: false}, nil
}

func (t *mockTransport) AppendEntries(ctx context.Context, peerID uint64, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.aeCalls++
	if resp, ok := t.aeResponses[peerID]; ok {
		return resp, nil
	}
	return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (t *mockTransport) InstallSnapshot(ctx context.Context, peerID uint64, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	return &InstallSnapshotResponse{Term: req.Term}, nil
}

func TestNewRaft(t *testing.T) {
	storage := newMockStorage()
	transport := newMockTransport()
	applyCh := make(chan ApplyMsg, 10)

	config := DefaultConfig(1, []uint64{2, 3})
	r, err := NewRaft(config, transport, storage, applyCh)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer r.Shutdown()

	term, isLeader := r.GetState()
	if term != 0 {
		t.Errorf("expected term 0, got %d", term)
	}
	if isLeader {
		t.Error("new node should not be leader")
	}
}

func TestHandleRequestVote_GrantsVote(t *testing.T) {
	storage := newMockStorage()
	transport := newMockTransport()
	applyCh := make(chan ApplyMsg, 10)

	config := DefaultConfig(1, []uint64{2, 3})
	r, err := NewRaft(config, transport, storage, applyCh)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer r.Shutdown()

	resp := r.HandleRequestVote(&RequestVoteRequest{
		Term:         1,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Error("expected vote to be granted")
	}
	if resp.Term != 1 {
		t.Errorf("expected term 1, got %d", resp.Term)
	}
}

func TestHandleRequestVote_RejectsOldTerm(t *testing.T) {
	storage := newMockStorage()
	storage.term = 5
	transport := newMockTransport()
	applyCh := make(chan ApplyMsg, 10)

	config := DefaultConfig(1, []uint64{2, 3})
	r, err := NewRaft(config, transport, storage, applyCh)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer r.Shutdown()

	resp := r.HandleRequestVote(&RequestVoteRequest{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Error("should not grant vote for old term")
	}
}

func TestHandleRequestVote_RejectsDuplicateVote(t *testing.T) {
	storage := newMockStorage()
	transport := newMockTransport()
	applyCh := make(chan ApplyMsg, 10)

	config := DefaultConfig(1, []uint64{2, 3})
	r, err := NewRaft(config, transport, storage, applyCh)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer r.Shutdown()

	// First vote should succeed
	resp := r.HandleRequestVote(&RequestVoteRequest{
		Term: 1, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if !resp.VoteGranted {
		t.Error("first vote should be granted")
	}

	// Second vote to different candidate in same term should fail
	resp = r.HandleRequestVote(&RequestVoteRequest{
		Term: 1, CandidateId: 3, LastLogIndex: 0, LastLogTerm: 0,
	})
	if resp.VoteGranted {
		t.Error("second vote in same term should be rejected")
	}
}

func TestHandleAppendEntries_AcceptsFromLeader(t *testing.T) {
	storage := newMockStorage()
	transport := newMockTransport()
	applyCh := make(chan ApplyMsg, 10)

	config := DefaultConfig(1, []uint64{2, 3})
	r, err := NewRaft(config, transport, storage, applyCh)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer r.Shutdown()

	// Heartbeat from leader in term 1
	resp := r.HandleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	})

	if !resp.Success {
		t.Error("should accept heartbeat from leader")
	}
	if resp.Term != 1 {
		t.Errorf("expected term 1, got %d", resp.Term)
	}
}

func TestHandleAppendEntries_RejectsOldTerm(t *testing.T) {
	storage := newMockStorage()
	storage.term = 5
	transport := newMockTransport()
	applyCh := make(chan ApplyMsg, 10)

	config := DefaultConfig(1, []uint64{2, 3})
	r, err := NewRaft(config, transport, storage, applyCh)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer r.Shutdown()

	resp := r.HandleAppendEntries(&AppendEntriesRequest{
		Term:     3,
		LeaderId: 2,
	})

	if resp.Success {
		t.Error("should reject AppendEntries with old term")
	}
}

func TestHandleAppendEntries_AppendsEntries(t *testing.T) {
	storage := newMockStorage()
	transport := newMockTransport()
	applyCh := make(chan ApplyMsg, 10)

	config := DefaultConfig(1, []uint64{2, 3})
	r, err := NewRaft(config, transport, storage, applyCh)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer r.Shutdown()

	entries := []*LogEntry{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 1, Command: []byte("cmd2")},
	}

	resp := r.HandleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 0,
	})

	if !resp.Success {
		t.Error("should accept entries")
	}

	// Verify entries were stored
	r.mu.RLock()
	lastIdx := r.log.LastIndex()
	r.mu.RUnlock()

	if lastIdx != 2 {
		t.Errorf("expected last index 2, got %d", lastIdx)
	}
}

func TestSubmit_NonLeader(t *testing.T) {
	storage := newMockStorage()
	transport := newMockTransport()
	applyCh := make(chan ApplyMsg, 10)

	config := DefaultConfig(1, []uint64{2, 3})
	r, err := NewRaft(config, transport, storage, applyCh)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer r.Shutdown()

	_, _, isLeader := r.Submit([]byte("test"))
	if isLeader {
		t.Error("non-leader should not accept submissions")
	}
}

func TestLogOperations(t *testing.T) {
	storage := newMockStorage()
	log := NewLog(storage)

	// Empty log checks
	if log.LastIndex() != 0 {
		t.Errorf("expected last index 0, got %d", log.LastIndex())
	}
	if log.LastTerm() != 0 {
		t.Errorf("expected last term 0, got %d", log.LastTerm())
	}

	// Append entries
	err := log.Append(
		&LogEntry{Index: 1, Term: 1, Command: []byte("a")},
		&LogEntry{Index: 2, Term: 1, Command: []byte("b")},
		&LogEntry{Index: 3, Term: 2, Command: []byte("c")},
	)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if log.LastIndex() != 3 {
		t.Errorf("expected last index 3, got %d", log.LastIndex())
	}
	if log.LastTerm() != 2 {
		t.Errorf("expected last term 2, got %d", log.LastTerm())
	}

	// Get single entry
	entry, err := log.Get(2)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if entry == nil || string(entry.Command) != "b" {
		t.Error("expected entry with command 'b'")
	}

	// Range query
	entries, err := log.Range(1, 3)
	if err != nil {
		t.Fatalf("Range failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}

	// Truncate
	err = log.TruncateAfter(2)
	if err != nil {
		t.Fatalf("TruncateAfter failed: %v", err)
	}
	if log.LastIndex() != 2 {
		t.Errorf("expected last index 2 after truncate, got %d", log.LastIndex())
	}

	// Term lookup
	if log.Term(1) != 1 {
		t.Errorf("expected term 1 for index 1, got %d", log.Term(1))
	}
	if log.Term(2) != 1 {
		t.Errorf("expected term 1 for index 2, got %d", log.Term(2))
	}
}

func TestLeaderElection_WithMockTransport(t *testing.T) {
	// Create 3 nodes with mock transport that grants votes
	storage1 := newMockStorage()
	transport1 := newMockTransport()
	transport1.voteResponses[2] = &RequestVoteResponse{Term: 1, VoteGranted: true}
	transport1.voteResponses[3] = &RequestVoteResponse{Term: 1, VoteGranted: true}
	applyCh1 := make(chan ApplyMsg, 10)

	config1 := &Config{
		ID:                      1,
		Peers:                   []uint64{2, 3},
		ElectionTimeoutMin:      50 * time.Millisecond,
		ElectionTimeoutMax:      100 * time.Millisecond,
		HeartbeatInterval:       25 * time.Millisecond,
		MaxLogEntriesPerRequest: 100,
	}

	r, err := NewRaft(config1, transport1, storage1, applyCh1)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer r.Shutdown()

	// Wait for election
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, isLeader := r.GetState()
		if isLeader {
			t.Log("Node became leader")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Error("node did not become leader within timeout")
}
