package raft

import (
	"sync"
)

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Log manages the Raft log entries
type Log struct {
	mu      sync.RWMutex
	entries []*LogEntry // In-memory log (index 0 is unused, real entries start at index 1)
	storage Storage

	// Snapshot state
	snapshotIndex uint64 // Last index included in snapshot
	snapshotTerm  uint64 // Term of last index in snapshot
}

// NewLog creates a new Log
func NewLog(storage Storage) *Log {
	return &Log{
		entries: make([]*LogEntry, 1), // Index 0 is a dummy entry
		storage: storage,
	}
}

// Restore loads log entries from storage
func (l *Log) Restore() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Load snapshot metadata first
	_, lastIndex, lastTerm, err := l.storage.LoadSnapshot()
	if err == nil && lastIndex > 0 {
		l.snapshotIndex = lastIndex
		l.snapshotTerm = lastTerm
	}

	// Load log entries after snapshot
	startIndex := l.snapshotIndex + 1
	lastIdx, err := l.storage.LastIndex()
	if err != nil {
		return err
	}

	if lastIdx >= startIndex {
		entries, err := l.storage.GetEntries(startIndex, lastIdx+1)
		if err != nil {
			return err
		}
		l.entries = make([]*LogEntry, 1) // Reset with dummy entry
		l.entries = append(l.entries, entries...)
	}

	return nil
}

// Append adds entries to the log
func (l *Log) Append(entries ...*LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	// Persist to storage first
	if err := l.storage.AppendEntries(entries); err != nil {
		return err
	}

	// Add to in-memory log
	for _, entry := range entries {
		// Ensure entries are added at the correct position
		idx := entry.Index - l.snapshotIndex
		for uint64(len(l.entries)) <= idx {
			l.entries = append(l.entries, nil)
		}
		l.entries[idx] = entry
	}

	return nil
}

// Get returns the log entry at the given index
func (l *Log) Get(index uint64) (*LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index <= l.snapshotIndex {
		// Entry is in snapshot, not available individually
		return nil, nil
	}

	adjustedIdx := index - l.snapshotIndex
	if adjustedIdx >= uint64(len(l.entries)) {
		return nil, nil
	}

	return l.entries[adjustedIdx], nil
}

// Range returns log entries in the range [start, end)
func (l *Log) Range(start, end uint64) ([]*LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if start <= l.snapshotIndex {
		start = l.snapshotIndex + 1
	}

	if end <= start {
		return nil, nil
	}

	startIdx := start - l.snapshotIndex
	endIdx := end - l.snapshotIndex

	if startIdx >= uint64(len(l.entries)) {
		return nil, nil
	}

	if endIdx > uint64(len(l.entries)) {
		endIdx = uint64(len(l.entries))
	}

	result := make([]*LogEntry, 0, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		if l.entries[i] != nil {
			result = append(result, l.entries[i])
		}
	}

	return result, nil
}

// LastIndex returns the index of the last log entry
func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.entries) <= 1 {
		return l.snapshotIndex
	}

	// Find the last non-nil entry
	for i := len(l.entries) - 1; i >= 1; i-- {
		if l.entries[i] != nil {
			return l.entries[i].Index
		}
	}

	return l.snapshotIndex
}

// LastTerm returns the term of the last log entry
func (l *Log) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.entries) <= 1 {
		return l.snapshotTerm
	}

	// Find the last non-nil entry
	for i := len(l.entries) - 1; i >= 1; i-- {
		if l.entries[i] != nil {
			return l.entries[i].Term
		}
	}

	return l.snapshotTerm
}

// TruncateAfter removes all entries after the given index
func (l *Log) TruncateAfter(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < l.snapshotIndex {
		// Can't truncate into snapshot
		return nil
	}

	// Truncate storage first
	if err := l.storage.TruncateAfter(index); err != nil {
		return err
	}

	// Truncate in-memory log
	adjustedIdx := index - l.snapshotIndex + 1
	if adjustedIdx < uint64(len(l.entries)) {
		l.entries = l.entries[:adjustedIdx]
	}

	return nil
}

// Length returns the number of entries in the log (excluding snapshot)
func (l *Log) Length() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.LastIndex() - l.snapshotIndex
}

// Term returns the term of the entry at the given index
func (l *Log) Term(index uint64) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 {
		return 0
	}

	if index == l.snapshotIndex {
		return l.snapshotTerm
	}

	if index < l.snapshotIndex {
		return 0 // Entry is compacted
	}

	adjustedIdx := index - l.snapshotIndex
	if adjustedIdx >= uint64(len(l.entries)) || l.entries[adjustedIdx] == nil {
		return 0
	}

	return l.entries[adjustedIdx].Term
}

// Compact compacts the log up to the given index using a snapshot
func (l *Log) Compact(index uint64, term uint64, snapshot []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= l.snapshotIndex {
		return nil // Already compacted
	}

	// Save snapshot
	if err := l.storage.SaveSnapshot(snapshot, index, term); err != nil {
		return err
	}

	// Remove compacted entries from memory
	if index >= l.LastIndex() {
		l.entries = make([]*LogEntry, 1)
	} else {
		newStart := index - l.snapshotIndex
		if newStart < uint64(len(l.entries)) {
			remaining := l.entries[newStart+1:]
			l.entries = make([]*LogEntry, 1)
			l.entries = append(l.entries, remaining...)
		}
	}

	l.snapshotIndex = index
	l.snapshotTerm = term

	return nil
}
