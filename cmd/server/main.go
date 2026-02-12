package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aarush/raft-kv/pkg/kv"
	"github.com/aarush/raft-kv/pkg/raft"
	"github.com/aarush/raft-kv/pkg/storage"
	"github.com/aarush/raft-kv/pkg/transport"
	"google.golang.org/grpc"
)

func main() {
	nodeID := flag.Uint64("id", 1, "Node ID (must be unique in cluster)")
	addr := flag.String("addr", "localhost:9001", "Address to listen on for client requests")
	raftAddr := flag.String("raft-addr", "localhost:8001", "Address to listen on for Raft RPCs")
	peers := flag.String("peers", "", "Comma-separated peer list: id:raft-addr:client-addr,...")
	dataDir := flag.String("data-dir", "./data", "Directory for persistent storage")
	flag.Parse()

	raftPeers, clientPeers := parsePeers(*peers)
	peerIDs := make([]uint64, 0)
	for id := range raftPeers {
		if id != *nodeID {
			peerIDs = append(peerIDs, id)
		}
	}

	log.Printf("Starting raft-kv node %d (client=%s, raft=%s)", *nodeID, *addr, *raftAddr)

	// Storage
	store, err := storage.NewBoltStorage(*dataDir, *nodeID)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Transport
	grpcTransport := transport.NewGRPCTransport(*nodeID, raftPeers)

	// Raft
	config := raft.DefaultConfig(*nodeID, peerIDs)
	applyCh := make(chan raft.ApplyMsg, 100)
	raftNode, err := raft.NewRaft(config, grpcTransport, store, applyCh)
	if err != nil {
		log.Fatalf("Failed to initialize Raft: %v", err)
	}

	// KV store
	kvStore := kv.NewStore(raftNode)
	go kvStore.ApplyLoop(applyCh)

	// Raft gRPC server
	raftGRPCServer := transport.NewRaftGRPCServer(raftNode)
	go func() {
		lis, err := net.Listen("tcp", *raftAddr)
		if err != nil {
			log.Fatalf("Failed to listen on %s: %v", *raftAddr, err)
		}
		s := grpc.NewServer()
		transport.RegisterRaftGRPCServer(s, raftGRPCServer)
		log.Printf("Raft RPC listening on %s", *raftAddr)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Raft server failed: %v", err)
		}
	}()

	// KV gRPC server
	kvServer := kv.NewServer(kvStore, raftNode, clientPeers)
	go func() {
		lis, err := net.Listen("tcp", *addr)
		if err != nil {
			log.Fatalf("Failed to listen on %s: %v", *addr, err)
		}
		s := grpc.NewServer()
		kv.RegisterServer(s, kvServer)
		log.Printf("KV API listening on %s", *addr)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("KV server failed: %v", err)
		}
	}()

	grpcTransport.ConnectPeers()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	raftNode.Shutdown()
	grpcTransport.Close()
}

// parsePeers parses "1:localhost:8001:localhost:9001,2:..."
// Returns raftPeers (id->raftAddr) and clientPeers (id->clientAddr)
func parsePeers(peersStr string) (map[uint64]string, map[uint64]string) {
	raftPeers := make(map[uint64]string)
	clientPeers := make(map[uint64]string)
	if peersStr == "" {
		return raftPeers, clientPeers
	}

	for _, p := range strings.Split(peersStr, ",") {
		p = strings.TrimSpace(p)
		parts := strings.SplitN(p, ":", 2)
		if len(parts) < 2 {
			continue
		}
		var id uint64
		fmt.Sscanf(parts[0], "%d", &id)

		// Rest could be "host:raftPort:host:clientPort" or just "host:raftPort"
		addrs := parts[1]
		addrParts := strings.Split(addrs, ":")
		if len(addrParts) >= 2 {
			raftPeers[id] = addrParts[0] + ":" + addrParts[1]
		}
		if len(addrParts) >= 4 {
			clientPeers[id] = addrParts[2] + ":" + addrParts[3]
		}
	}
	return raftPeers, clientPeers
}
