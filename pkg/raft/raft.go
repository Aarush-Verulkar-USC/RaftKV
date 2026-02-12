// Package raft implements the Raft consensus protocol.
//
// Raft is a consensus algorithm that ensures a replicated log is consistent
// across a cluster of servers. This implementation follows the Raft paper:
// "In Search of an Understandable Consensus Algorithm" by Ongaro and Ousterhout.
package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// State represents the current state of a Raft node
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Config holds the configuration for a Raft node
type Config struct {
	// ID is the unique identifier for this node
	ID uint64

	// Peers is the list of peer node IDs (excluding self)
	Peers []uint64

	// ElectionTimeoutMin is the minimum election timeout
	ElectionTimeoutMin time.Duration

	// ElectionTimeoutMax is the maximum election timeout
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is how often the leader sends heartbeats
	HeartbeatInterval time.Duration

	// MaxLogEntriesPerRequest limits entries sent in a single AppendEntries
	MaxLogEntriesPerRequest int
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(id uint64, peers []uint64) *Config {
	return &Config{
		ID:                      id,
		Peers:                   peers,
		ElectionTimeoutMin:      150 * time.Millisecond,
		ElectionTimeoutMax:      300 * time.Millisecond,
		HeartbeatInterval:       50 * time.Millisecond,
		MaxLogEntriesPerRequest: 100,
	}
}

// ApplyMsg is sent on the apply channel when a log entry is committed
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	// For snapshots
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  uint64
	SnapshotIndex uint64
}

// Transport defines the interface for Raft RPC communication
type Transport interface {
	// RequestVote sends a RequestVote RPC to a peer
	RequestVote(ctx context.Context, peerID uint64, req *RequestVoteRequest) (*RequestVoteResponse, error)

	// AppendEntries sends an AppendEntries RPC to a peer
	AppendEntries(ctx context.Context, peerID uint64, req *AppendEntriesRequest) (*AppendEntriesResponse, error)

	// InstallSnapshot sends an InstallSnapshot RPC to a peer
	InstallSnapshot(ctx context.Context, peerID uint64, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

// Storage defines the interface for persistent storage
type Storage interface {
	// SaveState persists the current term and voted for
	SaveState(term uint64, votedFor uint64) error

	// LoadState loads the persisted term and voted for
	LoadState() (term uint64, votedFor uint64, err error)

	// AppendEntries appends entries to the log
	AppendEntries(entries []*LogEntry) error

	// GetEntries returns log entries in the range [startIndex, endIndex)
	GetEntries(startIndex, endIndex uint64) ([]*LogEntry, error)

	// GetEntry returns a single log entry
	GetEntry(index uint64) (*LogEntry, error)

	// TruncateAfter removes all entries after the given index
	TruncateAfter(index uint64) error

	// LastIndex returns the index of the last log entry
	LastIndex() (uint64, error)

	// LastTerm returns the term of the last log entry
	LastTerm() (uint64, error)

	// SaveSnapshot saves a snapshot
	SaveSnapshot(snapshot []byte, lastIndex, lastTerm uint64) error

	// LoadSnapshot loads the latest snapshot
	LoadSnapshot() (snapshot []byte, lastIndex, lastTerm uint64, err error)
}

// Raft implements a single Raft node
type Raft struct {
	mu sync.RWMutex

	// Node identity
	id    uint64
	peers []uint64

	// Persistent state (must be saved to storage before responding to RPCs)
	currentTerm uint64
	votedFor    uint64 // 0 means no vote
	log         *Log

	// Volatile state
	commitIndex uint64
	lastApplied uint64
	state       State
	leaderId    uint64

	// Leader-only volatile state (reinitialized after election)
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Components
	config    *Config
	transport Transport
	storage   Storage

	// Channels
	applyCh chan ApplyMsg

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Shutdown
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

// NewRaft creates a new Raft node
func NewRaft(config *Config, transport Transport, storage Storage, applyCh chan ApplyMsg) (*Raft, error) {
	r := &Raft{
		id:          config.ID,
		peers:       config.Peers,
		config:      config,
		transport:   transport,
		storage:     storage,
		applyCh:     applyCh,
		state:       Follower,
		nextIndex:   make(map[uint64]uint64),
		matchIndex:  make(map[uint64]uint64),
		shutdownCh:  make(chan struct{}),
	}

	// Initialize log
	r.log = NewLog(storage)

	// Restore persistent state
	if err := r.restoreState(); err != nil {
		return nil, err
	}

	// Start election timer
	r.resetElectionTimer()

	// Start the main loop
	r.wg.Add(1)
	go r.run()

	return r, nil
}

// restoreState restores persisted state from storage
func (r *Raft) restoreState() error {
	term, votedFor, err := r.storage.LoadState()
	if err != nil {
		return err
	}
	r.currentTerm = term
	r.votedFor = votedFor

	// Restore log state
	if err := r.log.Restore(); err != nil {
		return err
	}

	return nil
}

// run is the main event loop
func (r *Raft) run() {
	defer r.wg.Done()

	for {
		select {
		case <-r.shutdownCh:
			return
		default:
		}

		r.mu.RLock()
		state := r.state
		r.mu.RUnlock()

		switch state {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// runFollower runs the follower state machine
func (r *Raft) runFollower() {
	r.mu.Lock()
	if r.electionTimer == nil {
		r.resetElectionTimer()
	}
	timer := r.electionTimer
	r.mu.Unlock()

	select {
	case <-r.shutdownCh:
		return
	case <-timer.C:
		r.mu.Lock()
		r.state = Candidate
		r.mu.Unlock()
	}
}

// runCandidate runs the candidate state machine
func (r *Raft) runCandidate() {
	r.mu.Lock()
	r.currentTerm++
	r.votedFor = r.id
	currentTerm := r.currentTerm
	r.storage.SaveState(r.currentTerm, r.votedFor)
	r.resetElectionTimer()
	timer := r.electionTimer
	r.mu.Unlock()

	// Start election
	voteCh := r.startElection(currentTerm)

	votes := 1 // Vote for self
	majority := (len(r.peers)+1)/2 + 1

	for {
		select {
		case <-r.shutdownCh:
			return
		case <-timer.C:
			// Election timeout, start new election
			return
		case vote := <-voteCh:
			if vote.Term > currentTerm {
				r.mu.Lock()
				r.currentTerm = vote.Term
				r.votedFor = 0
				r.state = Follower
				r.storage.SaveState(r.currentTerm, r.votedFor)
				r.mu.Unlock()
				return
			}
			if vote.VoteGranted {
				votes++
				if votes >= majority {
					r.mu.Lock()
					r.state = Leader
					r.becomeLeader()
					r.mu.Unlock()
					return
				}
			}
		}
	}
}

// startElection sends RequestVote RPCs to all peers
func (r *Raft) startElection(term uint64) <-chan *RequestVoteResponse {
	voteCh := make(chan *RequestVoteResponse, len(r.peers))

	r.mu.RLock()
	lastLogIndex := r.log.LastIndex()
	lastLogTerm := r.log.LastTerm()
	r.mu.RUnlock()

	req := &RequestVoteRequest{
		Term:         term,
		CandidateId:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peer := range r.peers {
		go func(peerID uint64) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := r.transport.RequestVote(ctx, peerID, req)
			if err != nil {
				return
			}
			voteCh <- resp
		}(peer)
	}

	return voteCh
}

// becomeLeader initializes leader state
func (r *Raft) becomeLeader() {
	lastIndex := r.log.LastIndex()

	for _, peer := range r.peers {
		r.nextIndex[peer] = lastIndex + 1
		r.matchIndex[peer] = 0
	}

	r.leaderId = r.id

	// Start heartbeat timer
	r.heartbeatTimer = time.NewTimer(0) // Send immediately
}

// runLeader runs the leader state machine
func (r *Raft) runLeader() {
	r.mu.Lock()
	if r.heartbeatTimer == nil {
		r.heartbeatTimer = time.NewTimer(0)
	}
	timer := r.heartbeatTimer
	r.mu.Unlock()

	select {
	case <-r.shutdownCh:
		return
	case <-timer.C:
		r.sendHeartbeats()
		r.mu.Lock()
		r.heartbeatTimer.Reset(r.config.HeartbeatInterval)
		r.mu.Unlock()
	}
}

// sendHeartbeats sends AppendEntries RPCs to all peers
func (r *Raft) sendHeartbeats() {
	r.mu.RLock()
	currentTerm := r.currentTerm
	commitIndex := r.commitIndex
	r.mu.RUnlock()

	for _, peer := range r.peers {
		go func(peerID uint64) {
			r.replicateTo(peerID, currentTerm, commitIndex)
		}(peer)
	}
}

// replicateTo sends log entries to a single peer
func (r *Raft) replicateTo(peerID uint64, term uint64, commitIndex uint64) {
	r.mu.RLock()
	nextIdx := r.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 {
		if entry, err := r.log.Get(prevLogIndex); err == nil && entry != nil {
			prevLogTerm = entry.Term
		}
	}

	entries, _ := r.log.Range(nextIdx, r.log.LastIndex()+1)
	r.mu.RUnlock()

	req := &AppendEntriesRequest{
		Term:         term,
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := r.transport.AppendEntries(ctx, peerID, req)
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.votedFor = 0
		r.state = Follower
		r.storage.SaveState(r.currentTerm, r.votedFor)
		return
	}

	if resp.Success {
		r.nextIndex[peerID] = nextIdx + uint64(len(entries))
		r.matchIndex[peerID] = r.nextIndex[peerID] - 1
		r.updateCommitIndex()
	} else {
		// Decrement nextIndex and retry
		if resp.ConflictIndex > 0 {
			r.nextIndex[peerID] = resp.ConflictIndex
		} else if r.nextIndex[peerID] > 1 {
			r.nextIndex[peerID]--
		}
	}
}

// updateCommitIndex advances commitIndex if possible
func (r *Raft) updateCommitIndex() {
	// Find the highest index replicated on a majority
	for n := r.log.LastIndex(); n > r.commitIndex; n-- {
		entry, err := r.log.Get(n)
		if err != nil || entry == nil {
			continue
		}

		// Only commit entries from current term
		if entry.Term != r.currentTerm {
			continue
		}

		count := 1 // Self
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= n {
				count++
			}
		}

		if count > (len(r.peers)+1)/2 {
			r.commitIndex = n
			go r.applyCommitted()
			break
		}
	}
}

// applyCommitted sends committed entries to the apply channel
func (r *Raft) applyCommitted() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		entry, err := r.log.Get(r.lastApplied)
		if err != nil || entry == nil {
			continue
		}

		r.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}
	}
}

// resetElectionTimer resets the election timer with a random timeout
func (r *Raft) resetElectionTimer() {
	timeout := r.config.ElectionTimeoutMin +
		time.Duration(rand.Int63n(int64(r.config.ElectionTimeoutMax-r.config.ElectionTimeoutMin)))

	if r.electionTimer == nil {
		r.electionTimer = time.NewTimer(timeout)
	} else {
		r.electionTimer.Reset(timeout)
	}
}

// Submit submits a command to the Raft log (leader only)
func (r *Raft) Submit(command []byte) (index uint64, term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, 0, false
	}

	entry := &LogEntry{
		Index:   r.log.LastIndex() + 1,
		Term:    r.currentTerm,
		Command: command,
	}

	if err := r.log.Append(entry); err != nil {
		return 0, 0, false
	}

	return entry.Index, entry.Term, true
}

// GetState returns the current term and whether this node is leader
func (r *Raft) GetState() (term uint64, isLeader bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm, r.state == Leader
}

// GetLeaderID returns the current leader's ID (0 if unknown)
func (r *Raft) GetLeaderID() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderId
}

// Shutdown gracefully stops the Raft node
func (r *Raft) Shutdown() {
	close(r.shutdownCh)
	r.wg.Wait()
}

// RPC request/response types (matching proto definitions)

type RequestVoteRequest struct {
	Term         uint64
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderId          uint64
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Offset            uint64
	Data              []byte
	Done              bool
}

type InstallSnapshotResponse struct {
	Term uint64
}

// HandleRequestVote processes an incoming RequestVote RPC
func (r *Raft) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}

	// Reply false if term < currentTerm
	if req.Term < r.currentTerm {
		return resp
	}

	// Update term if we see a higher term
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = 0
		r.state = Follower
		r.storage.SaveState(r.currentTerm, r.votedFor)
	}

	resp.Term = r.currentTerm

	// Check if we can grant vote
	if r.votedFor == 0 || r.votedFor == req.CandidateId {
		// Check if candidate's log is at least as up-to-date as ours
		lastLogTerm := r.log.LastTerm()
		lastLogIndex := r.log.LastIndex()

		logOk := req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

		if logOk {
			r.votedFor = req.CandidateId
			r.storage.SaveState(r.currentTerm, r.votedFor)
			r.resetElectionTimer()
			resp.VoteGranted = true
		}
	}

	return resp
}

// HandleAppendEntries processes an incoming AppendEntries RPC
func (r *Raft) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}

	// Reply false if term < currentTerm
	if req.Term < r.currentTerm {
		return resp
	}

	// Update term if we see a higher term
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = 0
		r.storage.SaveState(r.currentTerm, r.votedFor)
	}

	r.state = Follower
	r.leaderId = req.LeaderId
	r.resetElectionTimer()

	resp.Term = r.currentTerm

	// Check if log contains entry at prevLogIndex with prevLogTerm
	if req.PrevLogIndex > 0 {
		entry, err := r.log.Get(req.PrevLogIndex)
		if err != nil || entry == nil {
			resp.ConflictIndex = r.log.LastIndex() + 1
			return resp
		}
		if entry.Term != req.PrevLogTerm {
			resp.ConflictTerm = entry.Term
			// Find first index of conflicting term
			for i := req.PrevLogIndex; i > 0; i-- {
				e, _ := r.log.Get(i)
				if e == nil || e.Term != resp.ConflictTerm {
					resp.ConflictIndex = i + 1
					break
				}
			}
			return resp
		}
	}

	// Append new entries
	for i, entry := range req.Entries {
		existing, _ := r.log.Get(entry.Index)
		if existing != nil && existing.Term != entry.Term {
			// Conflict: delete this and all following entries
			r.log.TruncateAfter(entry.Index - 1)
		}
		if existing == nil || existing.Term != entry.Term {
			// Append remaining entries
			r.log.Append(req.Entries[i:]...)
			break
		}
	}

	// Update commit index
	if req.LeaderCommit > r.commitIndex {
		lastNewIndex := uint64(0)
		if len(req.Entries) > 0 {
			lastNewIndex = req.Entries[len(req.Entries)-1].Index
		}
		if req.LeaderCommit < lastNewIndex {
			r.commitIndex = req.LeaderCommit
		} else {
			r.commitIndex = lastNewIndex
		}
		go r.applyCommitted()
	}

	resp.Success = true
	return resp
}
