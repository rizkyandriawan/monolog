# Monolog

A lightweight, single-node message broker that speaks the Kafka protocol. Drop-in compatible with existing Kafka clients for development and testing scenarios.

## Why Monolog?

Running Kafka locally is a pain. You need Zookeeper (or KRaft), deal with JVM memory overhead, wait for slow startups, and manage multiple processes just to test a simple producer/consumer flow. Docker Compose helps, but you're still looking at 500MB+ RAM for something that should be simple.

Monolog exists because sometimes you just want to:

- **Develop locally** without spinning up infrastructure
- **Run integration tests** in CI without container overhead
- **Prototype quickly** without Kafka's operational complexity
- **Learn the Kafka protocol** with a readable, hackable codebase

It's not a Kafka replacement. It's a Kafka stand-in for when you don't need distributed consensus, replication, or horizontal scaling - you just need something that speaks the protocol.

~5MB binary. ~20MB RAM. Starts instantly. Works with your existing Kafka clients.

### Production for Light Workloads

Monolog is also suitable for **production use in light workload scenarios**.

If you're running a hobby project or an early-stage startup, a proper Kafka cluster (3 brokers minimum for fault tolerance) on AWS can easily cost $300-500/month. For many small projects doing <100 messages/second, that's massive overkill.

Monolog can run on a $5/month VPS alongside your app. No cluster coordination, no ZooKeeper, no JVM tuning. Just a single binary that handles your message queue needs until you actually need to scale.

## Benchmark Results

Tested on Linux (NVMe SSD) with kafka-go client, SQLite backend with full durability (`synchronous=FULL`):

### Throughput & Latency

| Metric | Result |
|--------|--------|
| Throughput (sustained) | **2,900+ msg/s** |
| Latency p50 | 3ms |
| Latency p95 | 5ms |
| Latency p99 | 7ms |
| Latency max | 17ms |
| Memory | ~25 MB |
| Error rate | 0% |

### Durability Test Results

**Test 1.3: Kill During Heavy Write (30 repetitions)**

| Metric | Result |
|--------|--------|
| Test runs | 30 |
| Passed | **30 (100%)** |
| Failed | 0 |
| Data loss | **0 messages** |
| Total recovered | 811,086 messages |

Each run: 10 concurrent producers at 500 msg/s, kill -9 after 5-14 seconds, restart and verify recovery.

**Verdict:** Zero data loss across all crash/recovery cycles. SQLite WAL with fsync provides strong durability guarantees.

## Features

- **Kafka Protocol Compatible** - Works with kafka-go, librdkafka, KafkaJS, Sarama
- **Durable by Default** - SQLite with WAL mode and fsync per transaction
- **Web UI** - Built-in dashboard for topic/message inspection
- **Consumer Groups** - Full support for group coordination and offset management
- **Compression** - Transparent support for gzip, snappy, lz4, zstd
- **Long Polling** - Efficient fetch with configurable wait times
- **Message Retention** - Configurable time-based cleanup
- **Data Locking** - Prevents corruption from concurrent instances
- **Zero Dependencies** - Single binary, no external services

## Quick Start

```bash
# Build
go build -o monolog ./cmd/monolog

# Run with defaults (SQLite, port 9092)
./monolog serve

# Or with options
./monolog serve \
  -kafka-addr :9092 \
  -http-addr :8080 \
  -data-dir ./data \
  -storage sqlite
```

Access:
- Kafka protocol: `localhost:9092`
- Web UI: `http://localhost:8080`

### Usage with Kafka Clients

**Go (kafka-go)**
```go
writer := &kafka.Writer{
    Addr:  kafka.TCP("localhost:9092"),
    Topic: "my-topic",
}
writer.WriteMessages(ctx, kafka.Message{Value: []byte("hello")})
```

**Node.js (KafkaJS)**
```javascript
const kafka = new Kafka({ brokers: ['localhost:9092'] })
const producer = kafka.producer()
await producer.send({ topic: 'my-topic', messages: [{ value: 'hello' }] })
```

## Configuration

### Command Line Flags

```bash
./monolog serve \
  -config config.yaml \
  -kafka-addr :9092 \
  -http-addr :8080 \
  -data-dir ./data \
  -storage sqlite \
  -log-level info
```

### Environment Variables

```bash
MONOLOG_KAFKA_ADDR=:9092
MONOLOG_HTTP_ADDR=:8080
MONOLOG_DATA_DIR=./data
MONOLOG_STORAGE_BACKEND=sqlite
MONOLOG_LOG_LEVEL=info
MONOLOG_AUTH_TOKEN=secret  # Enables HTTP auth
```

### Config File (YAML)

```yaml
server:
  kafka_addr: ":9092"
  http_addr: ":8080"

storage:
  backend: "sqlite"        # or "sqlite:memory" for non-persistent
  data_dir: "./data"

topics:
  auto_create: true

limits:
  max_connections: 100
  max_message_size: 1048576  # 1MB
  max_fetch_bytes: 10485760  # 10MB

retention:
  enabled: true
  max_age: 24h
  check_interval: 1m

groups:
  session_timeout: 30s
  heartbeat_interval: 3s

logging:
  level: "info"
  format: "text"
```

## Storage Backends

### SQLite Disk (Default)

```bash
./monolog serve -storage sqlite
```

- WAL mode with `synchronous=FULL` for durability
- Survives kill -9 with zero data loss (verified)
- ~25MB memory for typical workloads
- Recommended for development and light production

### SQLite Memory

```bash
./monolog serve -storage sqlite:memory
```

- No persistence, data lost on restart
- Maximum performance for testing
- Useful for CI/CD pipelines

## Durability Guarantees

With SQLite disk backend:

- **Fsync per transaction** - Messages durable once acknowledged
- **WAL recovery** - Automatic recovery after crash
- **Data locking** - Prevents concurrent access corruption
- **Tested** - 30/30 kill-during-heavy-write tests passed with zero data loss

What can be lost:
- In-flight messages not yet committed (typically <10 messages)

## HTTP API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/topics` | GET | List all topics |
| `/api/topics` | POST | Create topic |
| `/api/topics/{name}` | GET | Get topic details |
| `/api/topics/{name}` | DELETE | Delete topic |
| `/api/topics/{name}/messages` | GET | Read messages |
| `/api/topics/{name}/messages` | POST | Produce message |
| `/api/groups` | GET | List consumer groups |
| `/api/groups/{id}` | GET | Get group details |
| `/api/groups/{id}` | DELETE | Delete group |

## Supported Kafka APIs

| API | Key | Description |
|-----|-----|-------------|
| Produce | 0 | Write messages |
| Fetch | 1 | Read messages |
| ListOffsets | 2 | Get earliest/latest offsets |
| Metadata | 3 | Topic and broker discovery |
| OffsetCommit | 8 | Commit consumer offsets |
| OffsetFetch | 9 | Fetch committed offsets |
| FindCoordinator | 10 | Find group coordinator |
| JoinGroup | 11 | Join consumer group |
| Heartbeat | 12 | Keep membership alive |
| LeaveGroup | 13 | Leave consumer group |
| SyncGroup | 14 | Synchronize group state |
| ApiVersions | 18 | Version negotiation |
| CreateTopics | 19 | Create new topics |

### Compression Codecs

All standard Kafka compression codecs are supported:
- None (0)
- Gzip (1)
- Snappy (2)
- LZ4 (3)
- Zstd (4)

Messages are stored compressed and decompressed by client libraries transparently.

## Limitations

- Single node only (no replication)
- Single partition per topic
- No SASL authentication on Kafka protocol
- No TLS on Kafka protocol

For high-throughput or distributed workloads, use Apache Kafka or Redpanda.

## Building

```bash
# Binary only
go build -o monolog ./cmd/monolog

# With web UI
cd web && npm install && npm run build && cd ..
go build -o monolog ./cmd/monolog
```

## License

MIT
