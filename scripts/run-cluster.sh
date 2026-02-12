#!/bin/bash

# run-cluster.sh - Start a local 3-node Raft cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$PROJECT_DIR/bin"
DATA_DIR="$PROJECT_DIR/data"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Cluster configuration
NODES=3
BASE_CLIENT_PORT=9001
BASE_RAFT_PORT=8001

# PID file for tracking
PID_FILE="$DATA_DIR/cluster.pids"

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Build peers string for a node
build_peers() {
    local exclude_id=$1
    local peers=""
    for i in $(seq 1 $NODES); do
        if [ $i -ne $exclude_id ]; then
            port=$((BASE_RAFT_PORT + i - 1))
            if [ -n "$peers" ]; then
                peers="$peers,"
            fi
            peers="${peers}${i}:localhost:${port}"
        fi
    done
    echo "$peers"
}

start_cluster() {
    log_info "Starting $NODES-node Raft cluster..."

    # Create data directory
    mkdir -p "$DATA_DIR"

    # Check if server binary exists
    if [ ! -f "$BIN_DIR/server" ]; then
        log_error "Server binary not found. Run 'make build' first."
        exit 1
    fi

    # Clear old PID file
    > "$PID_FILE"

    # Start each node
    for i in $(seq 1 $NODES); do
        client_port=$((BASE_CLIENT_PORT + i - 1))
        raft_port=$((BASE_RAFT_PORT + i - 1))
        peers=$(build_peers $i)
        node_data_dir="$DATA_DIR/node$i"

        mkdir -p "$node_data_dir"

        log_info "Starting node $i (client: $client_port, raft: $raft_port)..."

        "$BIN_DIR/server" \
            -id $i \
            -addr "localhost:$client_port" \
            -raft-addr "localhost:$raft_port" \
            -peers "$peers" \
            -data-dir "$node_data_dir" \
            > "$node_data_dir/server.log" 2>&1 &

        echo $! >> "$PID_FILE"
        log_info "  Node $i started with PID $!"
    done

    log_info "Cluster started successfully!"
    echo ""
    log_info "Client endpoints:"
    for i in $(seq 1 $NODES); do
        client_port=$((BASE_CLIENT_PORT + i - 1))
        echo "  Node $i: localhost:$client_port"
    done
    echo ""
    log_info "To interact with the cluster:"
    echo "  $BIN_DIR/client put mykey myvalue"
    echo "  $BIN_DIR/client get mykey"
    echo ""
    log_info "To stop the cluster:"
    echo "  make stop-cluster"
    echo "  # or: $0 stop"
}

stop_cluster() {
    log_info "Stopping cluster..."

    if [ -f "$PID_FILE" ]; then
        while read pid; do
            if kill -0 "$pid" 2>/dev/null; then
                log_info "Stopping process $pid..."
                kill "$pid" 2>/dev/null || true
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi

    # Also kill any stray server processes
    pkill -f "$BIN_DIR/server" 2>/dev/null || true

    log_info "Cluster stopped."
}

status_cluster() {
    log_info "Cluster status:"

    if [ ! -f "$PID_FILE" ]; then
        log_warn "No PID file found. Cluster may not be running."
        return
    fi

    running=0
    total=0

    while read pid; do
        total=$((total + 1))
        if kill -0 "$pid" 2>/dev/null; then
            echo "  Node (PID $pid): Running"
            running=$((running + 1))
        else
            echo "  Node (PID $pid): Stopped"
        fi
    done < "$PID_FILE"

    echo ""
    log_info "Running: $running/$total nodes"
}

logs_cluster() {
    local node_id=${1:-1}
    local log_file="$DATA_DIR/node$node_id/server.log"

    if [ -f "$log_file" ]; then
        log_info "Logs for node $node_id:"
        tail -f "$log_file"
    else
        log_error "Log file not found: $log_file"
    fi
}

# Main
case "${1:-start}" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    restart)
        stop_cluster
        sleep 1
        start_cluster
        ;;
    status)
        status_cluster
        ;;
    logs)
        logs_cluster "${2:-1}"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs [node_id]}"
        exit 1
        ;;
esac
