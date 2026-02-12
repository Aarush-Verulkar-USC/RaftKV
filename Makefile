.PHONY: all build clean test proto server client run-cluster help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Binary names
SERVER_BINARY=bin/server
CLIENT_BINARY=bin/client

# Proto
PROTO_DIR=pkg/transport/proto
PROTO_FILES=$(wildcard $(PROTO_DIR)/*.proto)

all: build

## build: Build server and client binaries
build: proto
	@echo "Building server..."
	$(GOBUILD) -o $(SERVER_BINARY) ./cmd/server
	@echo "Building client..."
	$(GOBUILD) -o $(CLIENT_BINARY) ./cmd/client
	@echo "Build complete!"

## proto: Generate Go code from proto files
proto:
	@echo "Generating proto files..."
	@if command -v protoc > /dev/null 2>&1; then \
		protoc --go_out=. --go_opt=paths=source_relative \
			--go-grpc_out=. --go-grpc_opt=paths=source_relative \
			$(PROTO_FILES); \
		echo "Proto generation complete!"; \
	else \
		echo "Warning: protoc not found. Skipping proto generation."; \
		echo "Install with: brew install protobuf"; \
		echo "Then install Go plugins:"; \
		echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"; \
		echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"; \
	fi

## test: Run all tests
test:
	@echo "Running tests..."
	$(GOTEST) -v -race ./...

## test-raft: Run only Raft tests
test-raft:
	@echo "Running Raft tests..."
	$(GOTEST) -v -race ./pkg/raft/...

## test-harness: Run test harness scenarios
test-harness:
	@echo "Running test harness scenarios..."
	$(GOTEST) -v -race -run TestScenarios ./pkg/testharness/...

## bench: Run benchmarks
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem ./...

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf bin/
	rm -rf data/
	@echo "Clean complete!"

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy
	@echo "Dependencies ready!"

## server: Build and run a single server node
server: build
	@echo "Starting server..."
	./$(SERVER_BINARY) -id 1 -addr localhost:9001 -raft-addr localhost:8001

## client: Build the client
client: build
	@echo "Client built at $(CLIENT_BINARY)"

## run-cluster: Start a 3-node cluster locally
run-cluster: build
	@echo "Starting 3-node cluster..."
	./scripts/run-cluster.sh

## stop-cluster: Stop the local cluster
stop-cluster:
	@echo "Stopping cluster..."
	pkill -f "bin/server" || true
	@echo "Cluster stopped!"

## lint: Run linter
lint:
	@echo "Running linter..."
	@if command -v golangci-lint > /dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not found. Install with:"; \
		echo "  brew install golangci-lint"; \
	fi

## fmt: Format code
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

## vet: Run go vet
vet:
	@echo "Running go vet..."
	$(GOCMD) vet ./...

## coverage: Generate test coverage report
coverage:
	@echo "Generating coverage report..."
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

## help: Show this help message
help:
	@echo "raft-kv - Distributed Key-Value Store with Raft Consensus"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' Makefile | sed 's/## /  /'
