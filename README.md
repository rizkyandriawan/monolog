# Monolog

A Kafka-compatible message broker in a single binary. Built for developers who need Kafka's protocol without Kafka's complexity.

```
┌─────────────────────────────────────────────────────────┐
│                       Monolog                           │
│                                                         │
│   Kafka Protocol (:9092)          HTTP API (:8080)      │
│         │                              │                │
│         ▼                              ▼                │
│   ┌──────────────────────────────────────────────┐      │
│   │                   Engine                     │      │
│   │         (topics, groups, offsets)            │      │
│   └──────────────────────────────────────────────┘      │
│                         │                               │
│                         ▼                               │
│   ┌──────────────────────────────────────────────┐      │
│   │              SQLite + WAL                    │      │
│   │         (synchronous=FULL, fsync)            │      │
│   └──────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────┘
```

## The Problem

**For local development:** Kafka requires Zookeeper (or KRaft), consumes 500MB+ RAM, and takes forever to start. Docker Compose helps, but you're still waiting 30+ seconds for something that should be instant.

**For small production:** A proper Kafka cluster needs 3 brokers minimum. On AWS, that's $300-500/month before you've even written any code. For a project doing 1,000 messages/second, that's massive overkill.

**What we actually need:**
- Kafka protocol compatibility (existing clients just work)
- Durability (data survives crashes)
- Simplicity (single binary, zero config)

## The Approach

**Language: Go**
- Single static binary (~5MB)
- No runtime dependencies
- Cross-platform (Linux, macOS, Windows)

**Storage: SQLite with WAL**
- Battle-tested durability
- `synchronous=FULL` mode (fsync per transaction)
- Automatic crash recovery

**Protocol: Kafka-compatible**
- Works with kafka-go, librdkafka, KafkaJS, Sarama
- Produce, Fetch, Consumer Groups, Offset Management
- Compression: gzip, snappy, lz4, zstd

**Intentional trade-offs:**
- Single node (no replication) → simpler, cheaper
- Single partition per topic → guaranteed ordering, simpler offset management
- No SASL auth → rely on network isolation

## Supported Kafka APIs

| API | Key | Status |
|-----|-----|--------|
| Produce | 0 | ✅ Supported |
| Fetch | 1 | ✅ Supported |
| ListOffsets | 2 | ✅ Supported |
| Metadata | 3 | ✅ Supported |
| OffsetCommit | 8 | ✅ Supported |
| OffsetFetch | 9 | ✅ Supported |
| FindCoordinator | 10 | ✅ Supported |
| JoinGroup | 11 | ✅ Supported |
| Heartbeat | 12 | ✅ Supported |
| LeaveGroup | 13 | ✅ Supported |
| SyncGroup | 14 | ✅ Supported |
| ApiVersions | 18 | ✅ Supported |
| CreateTopics | 19 | ✅ Supported |

**Not supported:** Transactions, Admin APIs (DescribeConfigs, AlterConfigs), ACLs, Quotas.

## Quick Start

```bash
# Build from source
git clone https://github.com/rizkyandriawan/monolog.git
cd monolog
go build -o monolog ./cmd/monolog

# Start server
./monolog serve

# That's it. Kafka on :9092, HTTP on :8080
```

### Producer Example (Go)

```go
writer := &kafka.Writer{
    Addr:     kafka.TCP("localhost:9092"),
    Topic:    "events",
    Balancer: &kafka.LeastBytes{},
}
defer writer.Close()

err := writer.WriteMessages(ctx, kafka.Message{
    Key:   []byte("user-123"),
    Value: []byte(`{"event": "signup"}`),
})
```

### Consumer Example (Go)

```go
reader := kafka.NewReader(kafka.ReaderConfig{
    Brokers:  []string{"localhost:9092"},
    Topic:    "events",
    GroupID:  "my-consumer-group",
    MinBytes: 1,
    MaxBytes: 10e6,
})
defer reader.Close()

for {
    msg, err := reader.ReadMessage(ctx)
    if err != nil {
        break
    }
    fmt.Printf("offset=%d key=%s value=%s\n", msg.Offset, msg.Key, msg.Value)
    // Offset is auto-committed after ReadMessage
}
```

### Quick Test with kcat

```bash
# Produce
echo '{"event":"test"}' | kcat -b localhost:9092 -t events -P

# Consume
kcat -b localhost:9092 -t events -C -o beginning
```

## Test Methodology & Results

All tests run on Linux with NVMe SSD, using kafka-go client.

### Throughput Test

**Method:** 10 concurrent producers, 512-byte messages, 60 seconds sustained.

| Metric | Result |
|--------|--------|
| Throughput | **2,906 msg/s** |
| Latency p50 | 3ms |
| Latency p95 | 5ms |
| Latency p99 | 7ms |
| Errors | 0 |

### Durability Test

**Method:** Produce under load → `kill -9` → restart → verify recovery. Repeated 30 times.

| Metric | Result |
|--------|--------|
| Test runs | 30 |
| Passed | **30 (100%)** |
| Data loss | **0 messages** |
| Total recovered | 811,086 messages |

Each run: 10 producers at 500 msg/s, kill after 5-14 seconds, restart, count recovered messages.

**What "zero data loss" means:** Messages that received an acknowledgment from the server were all recovered. Messages in-flight (not yet acked) at crash time may be lost—this is expected behavior.

### Throughput by Disk Type

Performance is **fsync-bound**. Disk type determines throughput:

| Disk Type | Expected Throughput |
|-----------|---------------------|
| NVMe SSD | 2,000-3,000 msg/s |
| SATA SSD | 500-800 msg/s |
| HDD | ~100 msg/s |

## Use Cases

### Good Fit

- **Local development** — instant startup, zero config
- **Integration tests / CI** — no container overhead, no Docker required
- **Production ≤2,000 msg/s** — with tolerance for brief restart downtime
- **Internal tools** — backoffice, admin systems, batch jobs
- **Startups** — before you need (or can afford) real Kafka

### Not a Fit

- Sustained throughput >5,000 msg/s
- High availability requirements (99.99% SLA)
- Multi-region / geo-redundancy needs
- Multi-partition topics for parallel consumers
- Compliance requirements (SOC2, HIPAA, etc.)

For these, use Apache Kafka, Redpanda, or managed services.

## Production Recommendations

Monolog is **durable but not highly available**. Single node means:
- Data survives crashes ✓
- Service unavailable during restarts ✗

Design your system accordingly:

### Producers

```go
writer := &kafka.Writer{
    Addr:         kafka.TCP("localhost:9092"),
    Topic:        "events",
    WriteTimeout: 10 * time.Second,
    RequiredAcks: kafka.RequireOne,
    // Async for higher throughput, Sync for guaranteed delivery
    Async: false,
}
```

- **Retry with backoff** — handle temporary unavailability
- **Set reasonable timeouts** — don't block forever (5-10s)
- **Use message keys for deduplication** — if consumer needs idempotency

### Consumers

- **Commit offset AFTER processing** — for at-least-once delivery
- **Make processing idempotent** — duplicates possible after crash recovery
- **Reconnect with backoff** — expect occasional disconnects during restarts

### Operations

- **Auto-restart:** Use systemd or Docker restart policy
- **Health check:** `curl http://localhost:8080/api/topics` → 200 = healthy
- **Backup:** Periodic rsync of data directory
- **Retention:** Configure `retention.max_age` to prevent unbounded disk growth (default: 24h)

### Hardware

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| Disk | SATA SSD | NVMe SSD |
| RAM | 256MB | 512MB |
| CPU | 1 core | 2 cores |

## Configuration

```bash
./monolog serve \
    -kafka-addr :9092 \
    -http-addr :8080 \
    -data-dir ./data \
    -storage sqlite \
    -log-level info
```

Or via environment:

```bash
export MONOLOG_KAFKA_ADDR=:9092
export MONOLOG_HTTP_ADDR=:8080
export MONOLOG_DATA_DIR=./data
./monolog serve
```

### Retention

Messages are retained for 24 hours by default. Configure in YAML:

```yaml
retention:
  enabled: true
  max_age: 24h        # delete messages older than this
  check_interval: 1m  # how often to run cleanup
```

## Limitations

| Limitation | Reason |
|------------|--------|
| Single node only | Simplicity over availability |
| Single partition per topic | Guaranteed ordering, simpler consumer logic |
| No SASL/TLS on Kafka port | Use network isolation or VPN |
| ~3,000 msg/s ceiling | fsync-bound (design choice for durability) |
| No transactions | Not implemented |

## HTTP API

```bash
# List topics
curl http://localhost:8080/api/topics

# Produce
curl -X POST http://localhost:8080/api/topics/my-topic/messages \
    -H "Content-Type: application/json" \
    -d '{"key":"k1", "value":"hello"}'

# Consume
curl "http://localhost:8080/api/topics/my-topic/messages?offset=0&limit=10"

# Topic info
curl http://localhost:8080/api/topics/my-topic

# Delete topic
curl -X DELETE http://localhost:8080/api/topics/my-topic
```

## License

MIT
