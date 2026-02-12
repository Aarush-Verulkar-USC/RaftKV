// Package transport provides gRPC-based networking for Raft and KV RPCs.
package transport

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aarush/raft-kv/pkg/raft"
	pb "github.com/aarush/raft-kv/pkg/transport/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport implements raft.Transport using gRPC
type GRPCTransport struct {
	mu      sync.RWMutex
	nodeID  uint64
	peers   map[uint64]string             // peerID -> address
	clients map[uint64]pb.RaftServiceClient // peerID -> gRPC client
	conns   map[uint64]*grpc.ClientConn
}

// NewGRPCTransport creates a new gRPC transport
func NewGRPCTransport(nodeID uint64, peers map[uint64]string) *GRPCTransport {
	return &GRPCTransport{
		nodeID:  nodeID,
		peers:   peers,
		clients: make(map[uint64]pb.RaftServiceClient),
		conns:   make(map[uint64]*grpc.ClientConn),
	}
}

// ConnectPeers establishes connections to all peers
func (t *GRPCTransport) ConnectPeers() {
	for id, addr := range t.peers {
		if id == t.nodeID {
			continue
		}
		go t.connectToPeer(id, addr)
	}
}

func (t *GRPCTransport) connectToPeer(peerID uint64, addr string) {
	for {
		conn, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(5*time.Second),
		)
		if err != nil {
			log.Printf("Failed to connect to peer %d at %s: %v (retrying...)", peerID, addr, err)
			time.Sleep(time.Second)
			continue
		}

		t.mu.Lock()
		t.conns[peerID] = conn
		t.clients[peerID] = pb.NewRaftServiceClient(conn)
		t.mu.Unlock()

		log.Printf("Connected to peer %d at %s", peerID, addr)
		return
	}
}

func (t *GRPCTransport) getClient(peerID uint64) (pb.RaftServiceClient, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	client, ok := t.clients[peerID]
	if !ok {
		return nil, fmt.Errorf("no connection to peer %d", peerID)
	}
	return client, nil
}

// RequestVote sends a RequestVote RPC to a peer
func (t *GRPCTransport) RequestVote(ctx context.Context, peerID uint64, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	client, err := t.getClient(peerID)
	if err != nil {
		return nil, err
	}

	pbReq := &pb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	pbResp, err := client.RequestVote(ctx, pbReq)
	if err != nil {
		return nil, err
	}

	return &raft.RequestVoteResponse{
		Term:        pbResp.Term,
		VoteGranted: pbResp.VoteGranted,
	}, nil
}

// AppendEntries sends an AppendEntries RPC to a peer
func (t *GRPCTransport) AppendEntries(ctx context.Context, peerID uint64, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	client, err := t.getClient(peerID)
	if err != nil {
		return nil, err
	}

	pbEntries := make([]*pb.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		pbEntries[i] = &pb.LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		}
	}

	pbReq := &pb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: req.LeaderCommit,
	}

	pbResp, err := client.AppendEntries(ctx, pbReq)
	if err != nil {
		return nil, err
	}

	return &raft.AppendEntriesResponse{
		Term:          pbResp.Term,
		Success:       pbResp.Success,
		ConflictIndex: pbResp.ConflictIndex,
		ConflictTerm:  pbResp.ConflictTerm,
	}, nil
}

// InstallSnapshot sends an InstallSnapshot RPC to a peer
func (t *GRPCTransport) InstallSnapshot(ctx context.Context, peerID uint64, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	client, err := t.getClient(peerID)
	if err != nil {
		return nil, err
	}

	pbReq := &pb.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	}

	pbResp, err := client.InstallSnapshot(ctx, pbReq)
	if err != nil {
		return nil, err
	}

	return &raft.InstallSnapshotResponse{
		Term: pbResp.Term,
	}, nil
}

// Close closes all peer connections
func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for id, conn := range t.conns {
		conn.Close()
		delete(t.conns, id)
		delete(t.clients, id)
	}
}

// RaftGRPCServer implements the gRPC RaftService server
type RaftGRPCServer struct {
	pb.UnimplementedRaftServiceServer
	raft *raft.Raft
}

// NewRaftGRPCServer creates a new Raft gRPC server handler
func NewRaftGRPCServer(r *raft.Raft) *RaftGRPCServer {
	return &RaftGRPCServer{raft: r}
}

// RegisterRaftGRPCServer registers the Raft gRPC service
func RegisterRaftGRPCServer(s *grpc.Server, srv *RaftGRPCServer) {
	pb.RegisterRaftServiceServer(s, srv)
}

func (s *RaftGRPCServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	raftReq := &raft.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	resp := s.raft.HandleRequestVote(raftReq)

	return &pb.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (s *RaftGRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	entries := make([]*raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = &raft.LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Command: e.Command,
		}
	}

	raftReq := &raft.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}

	resp := s.raft.HandleAppendEntries(raftReq)

	return &pb.AppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

func (s *RaftGRPCServer) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	// TODO: Implement snapshot installation
	return &pb.InstallSnapshotResponse{
		Term: 0,
	}, nil
}

