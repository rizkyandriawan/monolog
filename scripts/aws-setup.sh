#!/bin/bash
# Monolog AWS Setup & Test Script
# Run on fresh Ubuntu instance

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[+]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err() { echo -e "${RED}[x]${NC} $1"; }

REPO_URL="${REPO_URL:-https://github.com/rizkyandriawan/monolog.git}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/monolog}"
GO_VERSION="1.22.0"
RESULTS_DIR="$HOME/monolog-test-results"

mkdir -p "$RESULTS_DIR"
REPORT="$RESULTS_DIR/report-$(date +%Y%m%d-%H%M%S).txt"

exec > >(tee -a "$REPORT") 2>&1

echo "=============================================="
echo "  Monolog AWS Setup & Test"
echo "  $(date)"
echo "=============================================="
echo ""

# System info
log "System Information"
echo "  OS: $(cat /etc/os-release | grep PRETTY_NAME | cut -d'"' -f2)"
echo "  Kernel: $(uname -r)"
echo "  CPU: $(nproc) cores"
echo "  RAM: $(free -h | awk '/^Mem:/{print $2}')"
echo "  Disk: $(df -h / | awk 'NR==2{print $4}') available"
echo ""

# ============================================
# 1. Install Dependencies
# ============================================
log "Installing dependencies..."

sudo apt-get update -qq
sudo apt-get install -y -qq git curl wget jq build-essential

# Install Go if not present
if ! command -v go &> /dev/null || [[ "$(go version | grep -oP '\d+\.\d+')" < "1.21" ]]; then
    log "Installing Go $GO_VERSION..."

    wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -O /tmp/go.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf /tmp/go.tar.gz
    rm /tmp/go.tar.gz

    # Add to PATH
    export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
    if ! grep -q '/usr/local/go/bin' ~/.bashrc; then
        echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.bashrc
    fi
fi

log "Go version: $(go version)"
echo ""

# ============================================
# 2. Clone and Build
# ============================================
log "Cloning monolog..."

if [ -d "$INSTALL_DIR" ]; then
    warn "Directory exists, pulling latest..."
    cd "$INSTALL_DIR"
    git pull
else
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
fi

log "Building monolog..."
go build -o monolog ./cmd/monolog

log "Build successful!"
./monolog version || echo "  (version command not available)"
echo ""

# ============================================
# 3. Run Tests
# ============================================
log "Starting test suite..."
echo ""

# Create test data directory
TEST_DATA="/tmp/monolog-aws-test"
rm -rf "$TEST_DATA"

# Start server
log "Starting monolog server..."
./monolog serve \
    -kafka-addr :9092 \
    -http-addr :8080 \
    -data-dir "$TEST_DATA" \
    -storage sqlite \
    -log-level error &
SERVER_PID=$!
sleep 3

# Verify server is running
if ! curl -s http://localhost:8080/api/topics > /dev/null; then
    err "Server failed to start!"
    exit 1
fi
log "Server started (PID: $SERVER_PID)"
echo ""

# ------------------------------
# Test 1: Basic Produce/Consume
# ------------------------------
log "Test 1: Basic Produce/Consume"

# Produce via HTTP
for i in $(seq 1 100); do
    curl -s -X POST http://localhost:8080/api/topics/test-basic/messages \
        -H "Content-Type: application/json" \
        -d "{\"key\":\"key-$i\",\"value\":\"message $i\"}" > /dev/null
done

# Check count
OFFSET=$(curl -s http://localhost:8080/api/topics/test-basic | jq -r '.latest_offset')
if [ "$OFFSET" == "99" ]; then
    echo "  PASS: 100 messages produced (offset 0-99)"
else
    echo "  FAIL: Expected offset 99, got $OFFSET"
fi
echo ""

# ------------------------------
# Test 2: Throughput (if stress test exists)
# ------------------------------
if [ -f "test/stress/main.go" ]; then
    log "Test 2: Throughput Benchmark (30s)"

    cd test/stress
    THROUGHPUT_RESULT=$(go run main.go \
        -broker localhost:9092 \
        -scenario produce \
        -rate 3000 \
        -duration 30s \
        -producers 10 \
        -ramp 0s \
        -report 30s 2>&1)

    echo "$THROUGHPUT_RESULT" | grep -E "(Avg produce|Latency|PASS|FAIL)" | head -10
    cd "$INSTALL_DIR"
    echo ""
fi

# ------------------------------
# Test 3: Kill Recovery
# ------------------------------
log "Test 3: Kill Recovery (3 runs)"

for run in 1 2 3; do
    echo "  Run $run/3:"

    # Produce more messages
    for i in $(seq 1 500); do
        curl -s -X POST http://localhost:8080/api/topics/test-recovery/messages \
            -H "Content-Type: application/json" \
            -d "{\"value\":\"recovery test $run-$i\"}" > /dev/null
    done

    BEFORE=$(curl -s http://localhost:8080/api/topics/test-recovery | jq -r '.latest_offset')

    # Kill server
    kill -9 $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
    sleep 1

    # Restart
    ./monolog serve \
        -kafka-addr :9092 \
        -http-addr :8080 \
        -data-dir "$TEST_DATA" \
        -storage sqlite \
        -log-level error &
    SERVER_PID=$!
    sleep 2

    AFTER=$(curl -s http://localhost:8080/api/topics/test-recovery | jq -r '.latest_offset')

    if [ "$AFTER" == "$BEFORE" ]; then
        echo "    PASS: Recovered $((AFTER + 1)) messages"
    else
        echo "    WARN: Before=$BEFORE After=$AFTER"
    fi
done
echo ""

# ------------------------------
# Test 4: Data Directory Locking
# ------------------------------
log "Test 4: Data Directory Locking"

# Try to start second instance
./monolog serve \
    -kafka-addr :9093 \
    -http-addr :8081 \
    -data-dir "$TEST_DATA" \
    -storage sqlite 2>&1 &
SECOND_PID=$!
sleep 2

if ps -p $SECOND_PID > /dev/null 2>&1; then
    echo "  FAIL: Second instance should not start"
    kill $SECOND_PID 2>/dev/null || true
else
    echo "  PASS: Second instance correctly rejected"
fi
echo ""

# ------------------------------
# Cleanup
# ------------------------------
log "Cleaning up..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

# Final stats
echo ""
echo "=============================================="
echo "  Test Complete"
echo "=============================================="
echo ""
echo "Results saved to: $REPORT"
echo ""

# Summary
log "Summary:"
curl -s http://localhost:8080/api/topics 2>/dev/null | jq '.' || echo "  (server stopped)"

echo ""
log "Monolog installed at: $INSTALL_DIR"
log "To run: cd $INSTALL_DIR && ./monolog serve"
echo ""
