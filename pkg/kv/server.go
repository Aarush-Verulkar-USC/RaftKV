package kv

import (
	"context"
	"time"

	"github.com/aarush/raft-kv/pkg/raft"
	pb "github.com/aarush/raft-kv/pkg/transport/proto"
	"google.golang.org/grpc"
)

// Server implements the gRPC KVService server
type Server struct {
	pb.UnimplementedKVServiceServer
	store       *Store
	raft        *raft.Raft
	peerAddrs   map[uint64]string // nodeID -> client address
	waitTimeout time.Duration
}

// NewServer creates a new KV gRPC server
func NewServer(store *Store, raft *raft.Raft, peerAddrs map[uint64]string) *Server {
	return &Server{
		store:       store,
		raft:        raft,
		peerAddrs:   peerAddrs,
		waitTimeout: 5 * time.Second,
	}
}

// RegisterServer registers the KV gRPC service
func RegisterServer(s *grpc.Server, srv *Server) {
	pb.RegisterKVServiceServer(s, srv)
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	_, isLeader := s.raft.GetState()

	// For linearizable reads, redirect to leader
	if !isLeader && !req.AllowStale {
		leaderID := s.raft.GetLeaderID()
		leaderAddr := s.peerAddrs[leaderID]
		return &pb.GetResponse{
			IsLeader:      false,
			LeaderAddress: leaderAddr,
			Error: &pb.Error{
				Code:    pb.ErrorCode_ERROR_CODE_NOT_LEADER,
				Message: "not the leader",
			},
		}, nil
	}

	value, found := s.store.Get(req.Key)
	return &pb.GetResponse{
		Value:    value,
		Found:    found,
		IsLeader: isLeader,
	}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	_, resultCh, isLeader := s.store.Put(req.Key, req.Value)
	if !isLeader {
		leaderID := s.raft.GetLeaderID()
		leaderAddr := s.peerAddrs[leaderID]
		return &pb.PutResponse{
			Success:       false,
			IsLeader:      false,
			LeaderAddress: leaderAddr,
			Error: &pb.Error{
				Code:    pb.ErrorCode_ERROR_CODE_NOT_LEADER,
				Message: "not the leader",
			},
		}, nil
	}

	// Wait for commit
	select {
	case result := <-resultCh:
		if result.Err != nil {
			return &pb.PutResponse{
				Success:  false,
				IsLeader: true,
				Error: &pb.Error{
					Code:    pb.ErrorCode_ERROR_CODE_INTERNAL,
					Message: result.Err.Error(),
				},
			}, nil
		}
		return &pb.PutResponse{
			Success:  true,
			IsLeader: true,
		}, nil
	case <-time.After(s.waitTimeout):
		return &pb.PutResponse{
			Success:  false,
			IsLeader: true,
			Error: &pb.Error{
				Code:    pb.ErrorCode_ERROR_CODE_TIMEOUT,
				Message: "request timed out waiting for commit",
			},
		}, nil
	case <-ctx.Done():
		return &pb.PutResponse{
			Success:  false,
			IsLeader: true,
			Error: &pb.Error{
				Code:    pb.ErrorCode_ERROR_CODE_TIMEOUT,
				Message: ctx.Err().Error(),
			},
		}, nil
	}
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	_, resultCh, isLeader := s.store.Delete(req.Key)
	if !isLeader {
		leaderID := s.raft.GetLeaderID()
		leaderAddr := s.peerAddrs[leaderID]
		return &pb.DeleteResponse{
			Success:       false,
			IsLeader:      false,
			LeaderAddress: leaderAddr,
			Error: &pb.Error{
				Code:    pb.ErrorCode_ERROR_CODE_NOT_LEADER,
				Message: "not the leader",
			},
		}, nil
	}

	// Wait for commit
	select {
	case result := <-resultCh:
		if result.Err != nil {
			return &pb.DeleteResponse{
				Success:  false,
				IsLeader: true,
				Error: &pb.Error{
					Code:    pb.ErrorCode_ERROR_CODE_INTERNAL,
					Message: result.Err.Error(),
				},
			}, nil
		}
		return &pb.DeleteResponse{
			Success:  true,
			Existed:  result.Found,
			IsLeader: true,
		}, nil
	case <-time.After(s.waitTimeout):
		return &pb.DeleteResponse{
			Success:  false,
			IsLeader: true,
			Error: &pb.Error{
				Code:    pb.ErrorCode_ERROR_CODE_TIMEOUT,
				Message: "request timed out waiting for commit",
			},
		}, nil
	case <-ctx.Done():
		return &pb.DeleteResponse{
			Success:  false,
			IsLeader: true,
			Error: &pb.Error{
				Code:    pb.ErrorCode_ERROR_CODE_TIMEOUT,
				Message: ctx.Err().Error(),
			},
		}, nil
	}
}
