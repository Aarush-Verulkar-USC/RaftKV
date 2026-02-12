// Package testharness provides a test cluster for simulating failures and network conditions.
package testharness

import (
	"fmt"
	"sync"
	"time"

	"github.com/aarush/raft-kv/pkg/kv"
	"github.com/aarush/raft-kv/pkg/raft"
	"github.com/aarush/raft-kv/pkg/storage"
)

// Cluster represents a test cluster of Raft nodes
type Cluster struct {
	mu sync.Mutex

	// Nodes in the cluster
	nodes map[uint64]*Node

	// Network simulation
	network *Network

	// Configuration
	nodeCount int
}

// Node represents a single node in the test cluster
type Node struct {
	ID      uint64
	Raft    *raft.Raft
	Store   *kv.Store
	Storage *storage.InMemoryStorage
	ApplyCh chan raft.ApplyMsg

	// For tracking applied commands
	applied     []raft.ApplyMsg
	appliedLock sync.Mutex
}

// ClusterConfig holds cluster configuration
type ClusterConfig struct {
	NodeCount            int
	ElectionTimeoutMin   time.Duration
	ElectionTimeoutMax   time.Duration
	HeartbeatInterval    time.Duration
}

// DefaultClusterConfig returns sensible defaults for testing
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		NodeCount:          3,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
}

// NewCluster creates a new test cluster
func NewCluster(config *ClusterConfig) (*Cluster, error) {
	if config == nil {
		config = DefaultClusterConfig()
	}

	c := &Cluster{
		nodes:     make(map[uint64]*Node),
		network:   NewNetwork(),
		nodeCount: config.NodeCount,
	}

	// Create all nodes
	for i := 1; i <= config.NodeCount; i++ {
		nodeID := uint64(i)
		peers := make([]uint64, 0, config.NodeCount-1)
		for j := 1; j <= config.NodeCount; j++ {
			if uint64(j) != nodeID {
				peers = append(peers, uint64(j))
			}
		}

		node, err := c.createNode(nodeID, peers, config)
		if err != nil {
			c.Shutdown()
			return nil, err
		}
		c.nodes[nodeID] = node
	}

	return c, nil
}

// createNode creates a single node
func (c *Cluster) createNode(id uint64, peers []uint64, config *ClusterConfig) (*Node, error) {
	storage := storage.NewInMemoryStorage()
	applyCh := make(chan raft.ApplyMsg, 100)

	raftConfig := &raft.Config{
		ID:                      id,
		Peers:                   peers,
		ElectionTimeoutMin:      config.ElectionTimeoutMin,
		ElectionTimeoutMax:      config.ElectionTimeoutMax,
		HeartbeatInterval:       config.HeartbeatInterval,
		MaxLogEntriesPerRequest: 100,
	}

	transport := c.network.CreateTransport(id)

	raftNode, err := raft.NewRaft(raftConfig, transport, storage, applyCh)
	if err != nil {
		return nil, err
	}

	// Register so other nodes' transports can route RPCs to this node
	c.network.RegisterRaft(id, raftNode)

	node := &Node{
		ID:      id,
		Raft:    raftNode,
		Storage: storage,
		ApplyCh: applyCh,
		applied: make([]raft.ApplyMsg, 0),
	}

	// Start apply loop
	go node.applyLoop()

	return node, nil
}

// applyLoop processes applied commands
func (n *Node) applyLoop() {
	for msg := range n.ApplyCh {
		n.appliedLock.Lock()
		n.applied = append(n.applied, msg)
		n.appliedLock.Unlock()
	}
}

// GetApplied returns all applied commands
func (n *Node) GetApplied() []raft.ApplyMsg {
	n.appliedLock.Lock()
	defer n.appliedLock.Unlock()
	result := make([]raft.ApplyMsg, len(n.applied))
	copy(result, n.applied)
	return result
}

// Shutdown stops all nodes
func (c *Cluster) Shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, node := range c.nodes {
		node.Raft.Shutdown()
		close(node.ApplyCh)
	}
}

// GetNode returns a node by ID
func (c *Cluster) GetNode(id uint64) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// GetLeader returns the current leader (or nil if no leader)
func (c *Cluster) GetLeader() *Node {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, node := range c.nodes {
		_, isLeader := node.Raft.GetState()
		if isLeader {
			return node
		}
	}
	return nil
}

// WaitForLeader waits until a leader is elected
func (c *Cluster) WaitForLeader(timeout time.Duration) (*Node, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if leader := c.GetLeader(); leader != nil {
			return leader, nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil, fmt.Errorf("no leader elected within %v", timeout)
}

// Submit submits a command to the cluster (through the leader)
func (c *Cluster) Submit(command []byte) (uint64, error) {
	leader := c.GetLeader()
	if leader == nil {
		return 0, fmt.Errorf("no leader available")
	}

	index, _, isLeader := leader.Raft.Submit(command)
	if !isLeader {
		return 0, fmt.Errorf("node is no longer leader")
	}

	return index, nil
}

// WaitForCommit waits for a command at the given index to be committed on majority
func (c *Cluster) WaitForCommit(index uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	majority := c.nodeCount/2 + 1

	for time.Now().Before(deadline) {
		count := 0
		c.mu.Lock()
		for _, node := range c.nodes {
			applied := node.GetApplied()
			for _, msg := range applied {
				if msg.CommandIndex >= index {
					count++
					break
				}
			}
		}
		c.mu.Unlock()

		if count >= majority {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	return fmt.Errorf("command at index %d not committed within %v", index, timeout)
}

// DisconnectNode disconnects a node from the network
func (c *Cluster) DisconnectNode(id uint64) {
	c.network.Disconnect(id)
}

// ReconnectNode reconnects a node to the network
func (c *Cluster) ReconnectNode(id uint64) {
	c.network.Reconnect(id)
}

// PartitionNetwork creates a network partition
// group1 and group2 cannot communicate with each other
func (c *Cluster) PartitionNetwork(group1, group2 []uint64) {
	c.network.Partition(group1, group2)
}

// HealPartition removes all network partitions
func (c *Cluster) HealPartition() {
	c.network.HealPartition()
}

// SetNetworkDelay sets network delay for a node
func (c *Cluster) SetNetworkDelay(id uint64, delay time.Duration) {
	c.network.SetDelay(id, delay)
}

// SetNetworkDropRate sets packet drop rate for a node (0.0 to 1.0)
func (c *Cluster) SetNetworkDropRate(id uint64, rate float64) {
	c.network.SetDropRate(id, rate)
}
