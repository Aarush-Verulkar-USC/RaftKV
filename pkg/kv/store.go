// Package kv implements the key-value store state machine on top of Raft.
package kv

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/aarush/raft-kv/pkg/raft"
)

// CommandType represents the type of KV operation
type CommandType uint8

const (
	CmdPut CommandType = iota
	CmdDelete
)

// Command represents a KV operation to be replicated
type Command struct {
	Type  CommandType
	Key   string
	Value string
}

// Serialize encodes a command for the Raft log
func (c *Command) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DeserializeCommand decodes a command from bytes
func DeserializeCommand(data []byte) (*Command, error) {
	var cmd Command
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

// Store is the key-value state machine
type Store struct {
	mu   sync.RWMutex
	data map[string]string

	// Raft instance
	raft *raft.Raft

	// Pending requests waiting for commit
	pending   map[uint64]chan Result
	pendingMu sync.Mutex
}

// Result represents the result of a KV operation
type Result struct {
	Value string
	Found bool
	Err   error
}

// NewStore creates a new KV store
func NewStore(raft *raft.Raft) *Store {
	s := &Store{
		data:    make(map[string]string),
		raft:    raft,
		pending: make(map[uint64]chan Result),
	}
	return s
}

// ApplyLoop reads from the apply channel and applies committed commands
func (s *Store) ApplyLoop(applyCh <-chan raft.ApplyMsg) {
	for msg := range applyCh {
		if msg.CommandValid {
			s.applyCommand(msg.CommandIndex, msg.Command)
		} else if msg.SnapshotValid {
			s.applySnapshot(msg.Snapshot)
		}
	}
}

// applyCommand applies a committed command to the state machine
func (s *Store) applyCommand(index uint64, data []byte) {
	cmd, err := DeserializeCommand(data)
	if err != nil {
		s.notifyPending(index, Result{Err: err})
		return
	}

	s.mu.Lock()
	var result Result

	switch cmd.Type {
	case CmdPut:
		s.data[cmd.Key] = cmd.Value
		result = Result{Value: cmd.Value, Found: true}
	case CmdDelete:
		_, existed := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		result = Result{Found: existed}
	}
	s.mu.Unlock()

	s.notifyPending(index, result)
}

// applySnapshot restores state from a snapshot
func (s *Store) applySnapshot(data []byte) {
	if len(data) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var snapshot map[string]string
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&snapshot); err != nil {
		return
	}

	s.data = snapshot
}

// notifyPending notifies a waiting request that its command has been applied
func (s *Store) notifyPending(index uint64, result Result) {
	s.pendingMu.Lock()
	ch, ok := s.pending[index]
	if ok {
		delete(s.pending, index)
	}
	s.pendingMu.Unlock()

	if ok {
		ch <- result
		close(ch)
	}
}

// registerPending registers a pending request
func (s *Store) registerPending(index uint64) <-chan Result {
	ch := make(chan Result, 1)
	s.pendingMu.Lock()
	s.pending[index] = ch
	s.pendingMu.Unlock()
	return ch
}

// Get retrieves the value for a key
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

// Put stores a key-value pair (must be replicated through Raft)
func (s *Store) Put(key, value string) (uint64, <-chan Result, bool) {
	cmd := &Command{
		Type:  CmdPut,
		Key:   key,
		Value: value,
	}

	data, err := cmd.Serialize()
	if err != nil {
		ch := make(chan Result, 1)
		ch <- Result{Err: err}
		close(ch)
		return 0, ch, false
	}

	index, _, isLeader := s.raft.Submit(data)
	if !isLeader {
		return 0, nil, false
	}

	return index, s.registerPending(index), true
}

// Delete removes a key (must be replicated through Raft)
func (s *Store) Delete(key string) (uint64, <-chan Result, bool) {
	cmd := &Command{
		Type: CmdDelete,
		Key:  key,
	}

	data, err := cmd.Serialize()
	if err != nil {
		ch := make(chan Result, 1)
		ch <- Result{Err: err}
		close(ch)
		return 0, ch, false
	}

	index, _, isLeader := s.raft.Submit(data)
	if !isLeader {
		return 0, nil, false
	}

	return index, s.registerPending(index), true
}

// Snapshot creates a snapshot of the current state
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Size returns the number of keys in the store
func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Keys returns all keys in the store
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}
