// Package storage provides persistent storage for Raft state and logs.
package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/aarush/raft-kv/pkg/raft"
	bolt "go.etcd.io/bbolt"
)

var (
	// Bucket names
	stateBucket    = []byte("state")
	logBucket      = []byte("log")
	snapshotBucket = []byte("snapshot")

	// State keys
	termKey     = []byte("term")
	votedForKey = []byte("votedFor")

	// Snapshot keys
	snapshotDataKey  = []byte("data")
	snapshotIndexKey = []byte("index")
	snapshotTermKey  = []byte("term")
)

// BoltStorage implements raft.Storage using BoltDB
type BoltStorage struct {
	mu   sync.RWMutex
	db   *bolt.DB
	path string
}

// NewBoltStorage creates a new BoltDB-backed storage
func NewBoltStorage(dataDir string, nodeID uint64) (*BoltStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	dbPath := filepath.Join(dataDir, fmt.Sprintf("raft-%d.db", nodeID))
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Initialize buckets
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{stateBucket, logBucket, snapshotBucket} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize buckets: %w", err)
	}

	return &BoltStorage{
		db:   db,
		path: dbPath,
	}, nil
}

// Close closes the database
func (s *BoltStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// SaveState persists the current term and votedFor
func (s *BoltStorage) SaveState(term uint64, votedFor uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(stateBucket)

		termBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(termBuf, term)
		if err := b.Put(termKey, termBuf); err != nil {
			return err
		}

		votedForBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(votedForBuf, votedFor)
		return b.Put(votedForKey, votedForBuf)
	})
}

// LoadState loads the persisted term and votedFor
func (s *BoltStorage) LoadState() (term uint64, votedFor uint64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(stateBucket)

		termBuf := b.Get(termKey)
		if termBuf != nil {
			term = binary.BigEndian.Uint64(termBuf)
		}

		votedForBuf := b.Get(votedForKey)
		if votedForBuf != nil {
			votedFor = binary.BigEndian.Uint64(votedForBuf)
		}

		return nil
	})

	return term, votedFor, err
}

// AppendEntries appends entries to the log
func (s *BoltStorage) AppendEntries(entries []*raft.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)

		for _, entry := range entries {
			key := indexToKey(entry.Index)
			value, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if err := b.Put(key, value); err != nil {
				return err
			}
		}

		return nil
	})
}

// GetEntries returns log entries in the range [startIndex, endIndex)
func (s *BoltStorage) GetEntries(startIndex, endIndex uint64) ([]*raft.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entries []*raft.LogEntry

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		c := b.Cursor()

		startKey := indexToKey(startIndex)
		endKey := indexToKey(endIndex)

		for k, v := c.Seek(startKey); k != nil && compareKeys(k, endKey) < 0; k, v = c.Next() {
			var entry raft.LogEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				return err
			}
			entries = append(entries, &entry)
		}

		return nil
	})

	return entries, err
}

// GetEntry returns a single log entry
func (s *BoltStorage) GetEntry(index uint64) (*raft.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entry *raft.LogEntry

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		v := b.Get(indexToKey(index))
		if v == nil {
			return nil
		}

		entry = &raft.LogEntry{}
		return json.Unmarshal(v, entry)
	})

	return entry, err
}

// TruncateAfter removes all entries after the given index
func (s *BoltStorage) TruncateAfter(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		c := b.Cursor()

		startKey := indexToKey(index + 1)
		var keysToDelete [][]byte

		for k, _ := c.Seek(startKey); k != nil; k, _ = c.Next() {
			keysToDelete = append(keysToDelete, append([]byte{}, k...))
		}

		for _, k := range keysToDelete {
			if err := b.Delete(k); err != nil {
				return err
			}
		}

		return nil
	})
}

// LastIndex returns the index of the last log entry
func (s *BoltStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var lastIndex uint64

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		c := b.Cursor()
		k, _ := c.Last()
		if k != nil {
			lastIndex = keyToIndex(k)
		}
		return nil
	})

	return lastIndex, err
}

// LastTerm returns the term of the last log entry
func (s *BoltStorage) LastTerm() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var lastTerm uint64

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		c := b.Cursor()
		_, v := c.Last()
		if v != nil {
			var entry raft.LogEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				return err
			}
			lastTerm = entry.Term
		}
		return nil
	})

	return lastTerm, err
}

// SaveSnapshot saves a snapshot
func (s *BoltStorage) SaveSnapshot(snapshot []byte, lastIndex, lastTerm uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(snapshotBucket)

		if err := b.Put(snapshotDataKey, snapshot); err != nil {
			return err
		}

		indexBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(indexBuf, lastIndex)
		if err := b.Put(snapshotIndexKey, indexBuf); err != nil {
			return err
		}

		termBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(termBuf, lastTerm)
		return b.Put(snapshotTermKey, termBuf)
	})
}

// LoadSnapshot loads the latest snapshot
func (s *BoltStorage) LoadSnapshot() (snapshot []byte, lastIndex, lastTerm uint64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(snapshotBucket)

		snapshot = b.Get(snapshotDataKey)

		indexBuf := b.Get(snapshotIndexKey)
		if indexBuf != nil {
			lastIndex = binary.BigEndian.Uint64(indexBuf)
		}

		termBuf := b.Get(snapshotTermKey)
		if termBuf != nil {
			lastTerm = binary.BigEndian.Uint64(termBuf)
		}

		return nil
	})

	return snapshot, lastIndex, lastTerm, err
}

// Helper functions for key encoding

func indexToKey(index uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, index)
	return key
}

func keyToIndex(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}

func compareKeys(a, b []byte) int {
	if len(a) != 8 || len(b) != 8 {
		return 0
	}
	ia := binary.BigEndian.Uint64(a)
	ib := binary.BigEndian.Uint64(b)
	if ia < ib {
		return -1
	}
	if ia > ib {
		return 1
	}
	return 0
}

// InMemoryStorage implements raft.Storage for testing
type InMemoryStorage struct {
	mu        sync.RWMutex
	term      uint64
	votedFor  uint64
	log       []*raft.LogEntry
	snapshot  []byte
	snapIndex uint64
	snapTerm  uint64
}

// NewInMemoryStorage creates a new in-memory storage
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		log: make([]*raft.LogEntry, 0),
	}
}

func (s *InMemoryStorage) SaveState(term uint64, votedFor uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	s.votedFor = votedFor
	return nil
}

func (s *InMemoryStorage) LoadState() (uint64, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.term, s.votedFor, nil
}

func (s *InMemoryStorage) AppendEntries(entries []*raft.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	return nil
}

func (s *InMemoryStorage) GetEntries(startIndex, endIndex uint64) ([]*raft.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*raft.LogEntry
	for _, e := range s.log {
		if e.Index >= startIndex && e.Index < endIndex {
			result = append(result, e)
		}
	}
	return result, nil
}

func (s *InMemoryStorage) GetEntry(index uint64) (*raft.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, e := range s.log {
		if e.Index == index {
			return e, nil
		}
	}
	return nil, nil
}

func (s *InMemoryStorage) TruncateAfter(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var newLog []*raft.LogEntry
	for _, e := range s.log {
		if e.Index <= index {
			newLog = append(newLog, e)
		}
	}
	s.log = newLog
	return nil
}

func (s *InMemoryStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.log) == 0 {
		return s.snapIndex, nil
	}
	return s.log[len(s.log)-1].Index, nil
}

func (s *InMemoryStorage) LastTerm() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.log) == 0 {
		return s.snapTerm, nil
	}
	return s.log[len(s.log)-1].Term, nil
}

func (s *InMemoryStorage) SaveSnapshot(snapshot []byte, lastIndex, lastTerm uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot = snapshot
	s.snapIndex = lastIndex
	s.snapTerm = lastTerm
	return nil
}

func (s *InMemoryStorage) LoadSnapshot() ([]byte, uint64, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot, s.snapIndex, s.snapTerm, nil
}
