package testharness

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/aarush/raft-kv/pkg/raft"
)

// Network simulates network conditions between nodes
type Network struct {
	mu sync.RWMutex

	transports   map[uint64]*SimulatedTransport
	raftNodes    map[uint64]*raft.Raft // Registered Raft instances for routing RPCs
	disconnected map[uint64]bool
	partitions   map[uint64]map[uint64]bool
	delays       map[uint64]time.Duration
	dropRates    map[uint64]float64
}

// NewNetwork creates a new simulated network
func NewNetwork() *Network {
	return &Network{
		transports:   make(map[uint64]*SimulatedTransport),
		raftNodes:    make(map[uint64]*raft.Raft),
		disconnected: make(map[uint64]bool),
		partitions:   make(map[uint64]map[uint64]bool),
		delays:       make(map[uint64]time.Duration),
		dropRates:    make(map[uint64]float64),
	}
}

// CreateTransport creates a transport for a node
func (n *Network) CreateTransport(nodeID uint64) *SimulatedTransport {
	n.mu.Lock()
	defer n.mu.Unlock()

	t := &SimulatedTransport{
		nodeID:  nodeID,
		network: n,
	}
	n.transports[nodeID] = t
	return t
}

// RegisterRaft registers a Raft instance so RPCs can be routed to it
func (n *Network) RegisterRaft(nodeID uint64, r *raft.Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.raftNodes[nodeID] = r
}

func (n *Network) getRaft(nodeID uint64) *raft.Raft {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raftNodes[nodeID]
}

// Disconnect disconnects a node from the network
func (n *Network) Disconnect(nodeID uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disconnected[nodeID] = true
}

// Reconnect reconnects a node to the network
func (n *Network) Reconnect(nodeID uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.disconnected, nodeID)
}

// Partition creates a network partition between two groups
func (n *Network) Partition(group1, group2 []uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, id1 := range group1 {
		if n.partitions[id1] == nil {
			n.partitions[id1] = make(map[uint64]bool)
		}
		for _, id2 := range group2 {
			n.partitions[id1][id2] = true
		}
	}
	for _, id2 := range group2 {
		if n.partitions[id2] == nil {
			n.partitions[id2] = make(map[uint64]bool)
		}
		for _, id1 := range group1 {
			n.partitions[id2][id1] = true
		}
	}
}

// HealPartition removes all partitions
func (n *Network) HealPartition() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions = make(map[uint64]map[uint64]bool)
}

// CanReach checks if src can reach dst
func (n *Network) CanReach(src, dst uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.disconnected[src] || n.disconnected[dst] {
		return false
	}
	if unreachable, ok := n.partitions[src]; ok {
		if unreachable[dst] {
			return false
		}
	}
	return true
}

// SetDelay sets network delay for a node
func (n *Network) SetDelay(nodeID uint64, delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delays[nodeID] = delay
}

func (n *Network) getDelay(nodeID uint64) time.Duration {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.delays[nodeID]
}

// SetDropRate sets packet drop rate for a node
func (n *Network) SetDropRate(nodeID uint64, rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRates[nodeID] = rate
}

func (n *Network) shouldDrop(nodeID uint64) bool {
	n.mu.RLock()
	rate := n.dropRates[nodeID]
	n.mu.RUnlock()
	if rate <= 0 {
		return false
	}
	return rand.Float64() < rate
}

// SimulatedTransport implements raft.Transport with network simulation
type SimulatedTransport struct {
	nodeID  uint64
	network *Network
}

func (t *SimulatedTransport) checkReachable(peerID uint64) error {
	if !t.network.CanReach(t.nodeID, peerID) {
		return fmt.Errorf("network: %d cannot reach %d", t.nodeID, peerID)
	}
	if t.network.shouldDrop(t.nodeID) {
		return fmt.Errorf("network: packet dropped from %d", t.nodeID)
	}
	return nil
}

func (t *SimulatedTransport) applyDelay(ctx context.Context) error {
	delay := t.network.getDelay(t.nodeID)
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (t *SimulatedTransport) RequestVote(ctx context.Context, peerID uint64, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	if err := t.checkReachable(peerID); err != nil {
		return nil, err
	}
	if err := t.applyDelay(ctx); err != nil {
		return nil, err
	}

	peer := t.network.getRaft(peerID)
	if peer == nil {
		return nil, fmt.Errorf("peer %d not registered", peerID)
	}

	return peer.HandleRequestVote(req), nil
}

func (t *SimulatedTransport) AppendEntries(ctx context.Context, peerID uint64, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	if err := t.checkReachable(peerID); err != nil {
		return nil, err
	}
	if err := t.applyDelay(ctx); err != nil {
		return nil, err
	}

	peer := t.network.getRaft(peerID)
	if peer == nil {
		return nil, fmt.Errorf("peer %d not registered", peerID)
	}

	return peer.HandleAppendEntries(req), nil
}

func (t *SimulatedTransport) InstallSnapshot(ctx context.Context, peerID uint64, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	if err := t.checkReachable(peerID); err != nil {
		return nil, err
	}
	if err := t.applyDelay(ctx); err != nil {
		return nil, err
	}

	// Snapshot installation not yet implemented
	return &raft.InstallSnapshotResponse{Term: 0}, nil
}
