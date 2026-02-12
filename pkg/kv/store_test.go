package kv

import (
	"testing"
)

func TestCommandSerialization(t *testing.T) {
	cmd := &Command{
		Type:  CmdPut,
		Key:   "hello",
		Value: "world",
	}

	data, err := cmd.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	decoded, err := DeserializeCommand(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if decoded.Type != CmdPut {
		t.Errorf("expected CmdPut, got %d", decoded.Type)
	}
	if decoded.Key != "hello" {
		t.Errorf("expected key 'hello', got '%s'", decoded.Key)
	}
	if decoded.Value != "world" {
		t.Errorf("expected value 'world', got '%s'", decoded.Value)
	}
}

func TestCommandSerialization_Delete(t *testing.T) {
	cmd := &Command{
		Type: CmdDelete,
		Key:  "mykey",
	}

	data, err := cmd.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	decoded, err := DeserializeCommand(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if decoded.Type != CmdDelete {
		t.Errorf("expected CmdDelete, got %d", decoded.Type)
	}
	if decoded.Key != "mykey" {
		t.Errorf("expected key 'mykey', got '%s'", decoded.Key)
	}
}

func TestStore_DirectGetAfterApply(t *testing.T) {
	// Test the in-memory state machine directly (without Raft)
	s := &Store{
		data:    make(map[string]string),
		pending: make(map[uint64]chan Result),
	}

	// Apply a Put command
	cmd := &Command{Type: CmdPut, Key: "k1", Value: "v1"}
	data, _ := cmd.Serialize()
	s.applyCommand(1, data)

	// Verify Get works
	val, found := s.Get("k1")
	if !found {
		t.Error("expected key to be found")
	}
	if val != "v1" {
		t.Errorf("expected 'v1', got '%s'", val)
	}

	// Apply a Delete
	delCmd := &Command{Type: CmdDelete, Key: "k1"}
	delData, _ := delCmd.Serialize()
	s.applyCommand(2, delData)

	_, found = s.Get("k1")
	if found {
		t.Error("expected key to be deleted")
	}
}

func TestStore_PendingNotification(t *testing.T) {
	s := &Store{
		data:    make(map[string]string),
		pending: make(map[uint64]chan Result),
	}

	// Register a pending request
	ch := s.registerPending(1)

	// Apply the command (this should notify the pending channel)
	cmd := &Command{Type: CmdPut, Key: "k", Value: "v"}
	data, _ := cmd.Serialize()
	s.applyCommand(1, data)

	result := <-ch
	if result.Err != nil {
		t.Errorf("unexpected error: %v", result.Err)
	}
	if result.Value != "v" {
		t.Errorf("expected value 'v', got '%s'", result.Value)
	}
}

func TestStore_MultipleKeys(t *testing.T) {
	s := &Store{
		data:    make(map[string]string),
		pending: make(map[uint64]chan Result),
	}

	// Insert multiple keys
	for i, kv := range []struct{ k, v string }{
		{"a", "1"}, {"b", "2"}, {"c", "3"},
	} {
		cmd := &Command{Type: CmdPut, Key: kv.k, Value: kv.v}
		data, _ := cmd.Serialize()
		s.applyCommand(uint64(i+1), data)
	}

	if s.Size() != 3 {
		t.Errorf("expected size 3, got %d", s.Size())
	}

	keys := s.Keys()
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestStore_Snapshot(t *testing.T) {
	s := &Store{
		data:    make(map[string]string),
		pending: make(map[uint64]chan Result),
	}

	// Add data
	s.data["key1"] = "val1"
	s.data["key2"] = "val2"

	// Create snapshot
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Clear and restore
	s.data = make(map[string]string)
	s.applySnapshot(snap)

	val, found := s.Get("key1")
	if !found || val != "val1" {
		t.Errorf("expected 'val1', got '%s' (found=%v)", val, found)
	}
	val, found = s.Get("key2")
	if !found || val != "val2" {
		t.Errorf("expected 'val2', got '%s' (found=%v)", val, found)
	}
}
