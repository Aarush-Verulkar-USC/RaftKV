#!/bin/bash

# benchmark.sh - Run performance benchmarks against the cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$PROJECT_DIR/bin"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Default settings
NUM_REQUESTS=1000
VALUE_SIZE=100
CONCURRENCY=10
SERVER="localhost:9001"

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -n NUM       Number of requests (default: $NUM_REQUESTS)"
    echo "  -s SIZE      Value size in bytes (default: $VALUE_SIZE)"
    echo "  -c CONC      Concurrency level (default: $CONCURRENCY)"
    echo "  --server ADDR Server address (default: $SERVER)"
    echo "  -h, --help   Show this help"
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n)
            NUM_REQUESTS="$2"
            shift 2
            ;;
        -s)
            VALUE_SIZE="$2"
            shift 2
            ;;
        -c)
            CONCURRENCY="$2"
            shift 2
            ;;
        --server)
            SERVER="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check if client exists
if [ ! -f "$BIN_DIR/client" ]; then
    log_warn "Client binary not found. Building..."
    make -C "$PROJECT_DIR" build
fi

# Generate random value of specified size
generate_value() {
    head -c "$VALUE_SIZE" /dev/urandom | base64 | head -c "$VALUE_SIZE"
}

log_info "Benchmark Configuration:"
echo "  Requests: $NUM_REQUESTS"
echo "  Value size: $VALUE_SIZE bytes"
echo "  Concurrency: $CONCURRENCY"
echo "  Server: $SERVER"
echo ""

# Run write benchmark
log_info "Running WRITE benchmark..."
export RAFTKV_SERVER="$SERVER"

start_time=$(date +%s.%N)

for i in $(seq 1 $NUM_REQUESTS); do
    key="bench-key-$i"
    value=$(generate_value)

    "$BIN_DIR/client" put "$key" "$value" > /dev/null 2>&1 &

    # Limit concurrency
    if [ $((i % CONCURRENCY)) -eq 0 ]; then
        wait
    fi
done
wait

end_time=$(date +%s.%N)
write_duration=$(echo "$end_time - $start_time" | bc)
write_ops_per_sec=$(echo "scale=2; $NUM_REQUESTS / $write_duration" | bc)

log_info "WRITE Results:"
echo "  Total time: ${write_duration}s"
echo "  Operations/sec: $write_ops_per_sec"
echo ""

# Run read benchmark
log_info "Running READ benchmark..."

start_time=$(date +%s.%N)

for i in $(seq 1 $NUM_REQUESTS); do
    key="bench-key-$i"

    "$BIN_DIR/client" get "$key" > /dev/null 2>&1 &

    # Limit concurrency
    if [ $((i % CONCURRENCY)) -eq 0 ]; then
        wait
    fi
done
wait

end_time=$(date +%s.%N)
read_duration=$(echo "$end_time - $start_time" | bc)
read_ops_per_sec=$(echo "scale=2; $NUM_REQUESTS / $read_duration" | bc)

log_info "READ Results:"
echo "  Total time: ${read_duration}s"
echo "  Operations/sec: $read_ops_per_sec"
echo ""

# Summary
log_info "Benchmark Summary:"
echo "  Write throughput: $write_ops_per_sec ops/sec"
echo "  Read throughput: $read_ops_per_sec ops/sec"
