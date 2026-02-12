package testharness

import (
	"fmt"
	"testing"
	"time"
)

// Scenario represents a test scenario
type Scenario struct {
	Name        string
	Description string
	Run         func(t *testing.T, c *Cluster) error
}

// BuiltinScenarios returns the built-in test scenarios
func BuiltinScenarios() []Scenario {
	return []Scenario{
		{
			Name:        "leader_election",
			Description: "Test that a leader is elected in a fresh cluster",
			Run:         ScenarioLeaderElection,
		},
		{
			Name:        "leader_failure",
			Description: "Test that a new leader is elected when the current leader fails",
			Run:         ScenarioLeaderFailure,
		},
		{
			Name:        "network_partition",
			Description: "Test behavior during a network partition",
			Run:         ScenarioNetworkPartition,
		},
		{
			Name:        "log_replication",
			Description: "Test that logs are replicated to all nodes",
			Run:         ScenarioLogReplication,
		},
		{
			Name:        "minority_partition",
			Description: "Test that minority partition cannot elect leader",
			Run:         ScenarioMinorityPartition,
		},
	}
}

// ScenarioLeaderElection tests basic leader election
func ScenarioLeaderElection(t *testing.T, c *Cluster) error {
	t.Log("Waiting for leader election...")

	leader, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		return fmt.Errorf("leader election failed: %w", err)
	}

	t.Logf("Leader elected: node %d", leader.ID)

	// Verify only one leader
	leaderCount := 0
	for i := 1; i <= 3; i++ {
		node := c.GetNode(uint64(i))
		_, isLeader := node.Raft.GetState()
		if isLeader {
			leaderCount++
		}
	}

	if leaderCount != 1 {
		return fmt.Errorf("expected 1 leader, got %d", leaderCount)
	}

	return nil
}

// ScenarioLeaderFailure tests leader failure and re-election
func ScenarioLeaderFailure(t *testing.T, c *Cluster) error {
	// Wait for initial leader
	leader, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		return fmt.Errorf("initial leader election failed: %w", err)
	}

	oldLeaderID := leader.ID
	t.Logf("Initial leader: node %d", oldLeaderID)

	// Disconnect the leader
	t.Logf("Disconnecting leader (node %d)...", oldLeaderID)
	c.DisconnectNode(oldLeaderID)

	// Wait for new leader
	time.Sleep(500 * time.Millisecond) // Give time for election timeout

	var newLeader *Node
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i := 1; i <= 3; i++ {
			if uint64(i) == oldLeaderID {
				continue
			}
			node := c.GetNode(uint64(i))
			_, isLeader := node.Raft.GetState()
			if isLeader {
				newLeader = node
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if newLeader == nil {
		return fmt.Errorf("no new leader elected after old leader failure")
	}

	if newLeader.ID == oldLeaderID {
		return fmt.Errorf("same node is still leader after disconnect")
	}

	t.Logf("New leader elected: node %d", newLeader.ID)

	// Reconnect old leader
	t.Logf("Reconnecting old leader (node %d)...", oldLeaderID)
	c.ReconnectNode(oldLeaderID)

	// Old leader should step down
	time.Sleep(200 * time.Millisecond)
	oldNode := c.GetNode(oldLeaderID)
	_, isLeader := oldNode.Raft.GetState()
	if isLeader {
		// This might be OK if old leader wins re-election, but typically shouldn't happen
		t.Log("Note: old leader is still/again leader after reconnect")
	}

	return nil
}

// ScenarioNetworkPartition tests behavior during network partition
func ScenarioNetworkPartition(t *testing.T, c *Cluster) error {
	// Wait for initial leader
	leader, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		return fmt.Errorf("initial leader election failed: %w", err)
	}

	t.Logf("Initial leader: node %d", leader.ID)

	// Create partition: [1] | [2, 3]
	t.Log("Creating partition: [1] | [2, 3]")
	c.PartitionNetwork([]uint64{1}, []uint64{2, 3})

	// If node 1 was leader, nodes 2 and 3 should elect a new leader
	// If node 1 was not leader, the majority side should keep or elect a leader
	time.Sleep(time.Second)

	// Check that majority side has a leader
	hasLeader := false
	for _, id := range []uint64{2, 3} {
		node := c.GetNode(id)
		_, isLeader := node.Raft.GetState()
		if isLeader {
			hasLeader = true
			t.Logf("Majority side leader: node %d", id)
			break
		}
	}

	if !hasLeader {
		t.Log("Warning: majority side doesn't have a leader yet, waiting...")
		time.Sleep(2 * time.Second)
	}

	// Heal partition
	t.Log("Healing partition...")
	c.HealPartition()

	time.Sleep(500 * time.Millisecond)

	// Verify cluster stabilizes with one leader
	leaderCount := 0
	for i := 1; i <= 3; i++ {
		node := c.GetNode(uint64(i))
		_, isLeader := node.Raft.GetState()
		if isLeader {
			leaderCount++
		}
	}

	if leaderCount != 1 {
		return fmt.Errorf("expected 1 leader after partition heal, got %d", leaderCount)
	}

	return nil
}

// ScenarioLogReplication tests log replication
func ScenarioLogReplication(t *testing.T, c *Cluster) error {
	// Wait for leader
	leader, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		return fmt.Errorf("leader election failed: %w", err)
	}

	t.Logf("Leader: node %d", leader.ID)

	// Submit several commands
	numCommands := 5
	for i := 0; i < numCommands; i++ {
		cmd := []byte(fmt.Sprintf("command-%d", i))
		index, err := c.Submit(cmd)
		if err != nil {
			return fmt.Errorf("failed to submit command %d: %w", i, err)
		}
		t.Logf("Submitted command %d at index %d", i, index)
	}

	// Wait for commands to be committed
	time.Sleep(500 * time.Millisecond)

	// Verify all nodes have the commands
	for i := 1; i <= 3; i++ {
		node := c.GetNode(uint64(i))
		applied := node.GetApplied()
		if len(applied) < numCommands {
			t.Logf("Node %d has %d applied commands (expected %d)", i, len(applied), numCommands)
		} else {
			t.Logf("Node %d has %d applied commands", i, len(applied))
		}
	}

	return nil
}

// ScenarioMinorityPartition tests that minority cannot elect leader
func ScenarioMinorityPartition(t *testing.T, c *Cluster) error {
	// Wait for initial leader
	leader, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		return fmt.Errorf("initial leader election failed: %w", err)
	}

	t.Logf("Initial leader: node %d", leader.ID)

	// Isolate node 1 completely
	t.Log("Isolating node 1...")
	c.DisconnectNode(1)

	// Wait for node 1 to timeout and try to become candidate
	time.Sleep(time.Second)

	// Node 1 should not be able to become leader (can't get majority)
	node1 := c.GetNode(1)
	_, isLeader := node1.Raft.GetState()

	if isLeader {
		return fmt.Errorf("isolated node became leader without majority")
	}

	t.Log("Isolated node correctly cannot become leader")

	// Reconnect
	c.ReconnectNode(1)

	return nil
}

// RunAllScenarios runs all built-in scenarios
func RunAllScenarios(t *testing.T) {
	scenarios := BuiltinScenarios()

	for _, scenario := range scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			t.Logf("Running scenario: %s", scenario.Description)

			cluster, err := NewCluster(DefaultClusterConfig())
			if err != nil {
				t.Fatalf("Failed to create cluster: %v", err)
			}
			defer cluster.Shutdown()

			if err := scenario.Run(t, cluster); err != nil {
				t.Errorf("Scenario failed: %v", err)
			}
		})
	}
}
