package main

import (
	"context"
	"fmt"
	"os"
	"time"

	pb "github.com/aarush/raft-kv/pkg/transport/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	serverAddr := "localhost:9001"
	if envAddr := os.Getenv("RAFTKV_SERVER"); envAddr != "" {
		serverAddr = envAddr
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to %s: %v\n", serverAddr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewKVServiceClient(conn)

	switch cmd {
	case "get":
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "Usage: client get <key>")
			os.Exit(1)
		}
		doGet(client, args[0])

	case "put":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: client put <key> <value>")
			os.Exit(1)
		}
		doPut(client, args[0], args[1])

	case "delete":
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "Usage: client delete <key>")
			os.Exit(1)
		}
		doDelete(client, args[0])

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func doGet(client pb.KVServiceClient, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Error != nil && resp.Error.Code == pb.ErrorCode_ERROR_CODE_NOT_LEADER {
		fmt.Fprintf(os.Stderr, "Not leader. Try: RAFTKV_SERVER=%s client get %s\n", resp.LeaderAddress, key)
		os.Exit(1)
	}

	if !resp.Found {
		fmt.Printf("(not found)\n")
		os.Exit(0)
	}

	fmt.Println(resp.Value)
}

func doPut(client pb.KVServiceClient, key, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Error != nil && resp.Error.Code == pb.ErrorCode_ERROR_CODE_NOT_LEADER {
		fmt.Fprintf(os.Stderr, "Not leader. Try: RAFTKV_SERVER=%s client put %s %s\n", resp.LeaderAddress, key, value)
		os.Exit(1)
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Failed: %s\n", resp.Error.GetMessage())
		os.Exit(1)
	}

	fmt.Println("OK")
}

func doDelete(client pb.KVServiceClient, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Error != nil && resp.Error.Code == pb.ErrorCode_ERROR_CODE_NOT_LEADER {
		fmt.Fprintf(os.Stderr, "Not leader. Try: RAFTKV_SERVER=%s client delete %s\n", resp.LeaderAddress, key)
		os.Exit(1)
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Failed: %s\n", resp.Error.GetMessage())
		os.Exit(1)
	}

	if resp.Existed {
		fmt.Println("OK (deleted)")
	} else {
		fmt.Println("OK (key did not exist)")
	}
}

func printUsage() {
	fmt.Println(`raft-kv client - Distributed Key-Value Store CLI

Usage:
  client <command> [arguments]

Commands:
  get <key>           Get the value for a key
  put <key> <value>   Store a key-value pair
  delete <key>        Delete a key

Environment:
  RAFTKV_SERVER       Server address (default: localhost:9001)

Examples:
  client put mykey myvalue
  client get mykey
  client delete mykey`)
}
