# FlowDB

A high-performance time-series storage engine written in Rust, powered by an LSM-tree architecture with WAL, SSTables, and Bloom filters.

## Benchmark Results

FlowDB vs RocksDB comparison (100K records, 128B values, batch=100, release build, Apple M-series):

| Category | FlowDB | RocksDB | Result |
|---|---|---|---|
| Sequential Write | 5.7M ops/s | 3.0M ops/s | **FlowDB 1.92x faster** |
| Concurrent Write (8 threads) | 6.7M ops/s | 4.1M ops/s | **FlowDB 1.63x faster** |
| Point Query | 6.6M ops/s | 524K ops/s | **FlowDB 12.7x faster** |
| Prefix Scan (~200 recs) | 71K ops/s | 10.7K ops/s | **FlowDB 6.6x faster** |
| Full Scan (200K recs) | 65 ops/s | 39 ops/s | **FlowDB 1.67x faster** |
| Storage | 1.9MB | 1.8MB | ~same |

```bash
cargo run --release --example flowdb-vs-rocksdb
```

## Features

- LSM-tree storage with WAL (write-ahead log) for crash recovery
- **Per-record WAL checksums** — corruption detected on replay, bad records rejected
- **Config validation** — invalid configs (e.g. `time_bucket_secs=0`) rejected at startup instead of crashing
- **Frozen memtable backpressure** — writes stall when flush can't keep up, preventing unbounded memory growth
- **HTTP batch & size limits** — max 10 000 records/batch, 4 KB keys, 64 KB values (matches UDP protocol)
- **Graceful shutdown** — SIGTERM/SIGINT triggers flush + clean exit
- **Bounded UDP rate limiter** — cap at 100 K entries with periodic idle cleanup
- **SST reader eviction** — stale readers removed after GC/compaction
- **Lazy scan iterator** (RocksDB-style `ScanIterator`) for bounded-memory range scans
- **`get_latest(key)`** for retrieving the most recent record by key
- Bloom filters for fast point query negative checks
- Dual compression: lz4 for flush (speed), zstd for compaction (ratio)
- Buffered WAL writes (256KB buffer) for reduced syscall overhead
- WAL pre-encoding outside the write lock for better concurrency
- Time-bucketed block index with binary search
- **LRU block cache** (64 shards, powered by `lru` crate) with true LRU eviction
- Log-based active memtable (Vec push) with on-freeze BTreeMap conversion
- Hash-based memtable index (avoids key clone on insert)
- Zero-copy owned write path (`write_batch_owned`)
- Synchronous write path (`write_batch_sync`) for non-async callers
- **Size-tiered compaction** with streaming heap merge (low memory footprint)
- **Range tombstones** (`delete_range`) for efficient bulk key-range deletion
- Garbage collection (TTL expiry), and point deletes
- HTTP + UDP ingest APIs (feature-gated under `server`)
- Admin dashboard, metrics, and stats endpoints
- API key authentication
- Config file support (TOML)

## Cargo Features

| Feature | Default | Description |
|---|---|---|
| `server` | Yes | HTTP/UDP servers, admin UI, auth (axum, tower-http, base64, crc16) |

```toml
# Full server (default)
flowdb = "0.2"

# Embedded engine only (no HTTP/UDP/auth, smaller binary)
flowdb = { version = "0.2", default-features = false }
```

## Quick Start

### Build

```bash
cargo build --release
```

### Run Server

```bash
# With defaults
./target/release/flowdb-server

# With CLI flags
./target/release/flowdb-server --data-dir ./data --http-addr 0.0.0.0:8080 --api-key mysecret

# With a config file
./target/release/flowdb-server --config flowdb.toml
```

### Config File (TOML)

```toml
http_addr = "0.0.0.0:8080"
udp_addr = "0.0.0.0:9090"
api_keys = ["my-api-key"]
max_udp_packet_size = 1400

[engine]
data_dir = "./data"
memtable_size_mb = 64
block_size = 8192
zstd_level = 3
bloom_bits_per_key = 10
wal_segment_size_mb = 64
compaction_threshold = 2
max_frozen_memtables = 2
flush_interval_ms = 1000
gc_interval_secs = 3600
default_ttl_secs = 86400
time_bucket_secs = 3600
index_memory_budget_mb = 256
block_cache_capacity_mb = 128
create_if_missing = true
```

## Configuration Reference

### Server Config

| Parameter | Default | Description |
|---|---|---|
| `http_addr` | `"0.0.0.0:8080"` | HTTP listen address |
| `udp_addr` | `"0.0.0.0:9090"` | UDP listen address |
| `api_keys` | `[]` | API keys for authentication (empty = no auth) |
| `udp_api_key` | `None` | Separate API key for UDP |
| `max_udp_packet_size` | `1400` | Maximum UDP packet size in bytes |

### Engine Config

| Parameter | Default | Description |
|---|---|---|
| `data_dir` | `"./data"` | Data directory path |
| `create_if_missing` | `true` | Create data directory if it doesn't exist |
| `memtable_size_mb` | `64` | Active memtable size threshold (MB) before flush |
| `max_frozen_memtables` | `2` | Max frozen memtables before writes block |
| `block_size` | `8192` | SSTable block size in bytes (number of records per block) |
| `zstd_level` | `3` | Zstd compression level (1-22, higher = better compression, slower) |
| `bloom_bits_per_key` | `10` | Bloom filter bits per key (higher = fewer false positives, more memory) |
| `wal_segment_size_mb` | `64` | WAL segment file size before rotation (MB) |
| `compaction_threshold` | `2` | Number of SSTables to trigger compaction |
| `flush_interval_ms` | `1000` | Background flush interval (ms) |
| `gc_interval_secs` | `3600` | Garbage collection interval (seconds) |
| `default_ttl_secs` | `None` | Default TTL for records without explicit expiry (seconds) |
| `time_bucket_secs` | `3600` | Time bucket granularity for block index (seconds) |
| `index_memory_budget_mb` | `256` | Memory budget for block index (MB) |
| `block_cache_capacity_mb` | `128` | Block cache capacity (MB) |

## HTTP API

### Write Records

```bash
# JSON write
curl -X POST http://localhost:8080/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer my-api-key" \
  -d '{"records": [{"key": "sensor.temp", "ts": 1700000000, "value": "22.5"}]}'

# Binary write (UDP frame format)
curl -X POST http://localhost:8080/write/binary \
  -H "Authorization: Bearer my-api-key" \
  --data-binary @frame.bin
```

### Query Records

```bash
# Prefix query
curl "http://localhost:8080/query?prefix=sensor.temp"

# Key range query
curl "http://localhost:8080/query?key_start=sensor.temp&key_end=sensor.tempz"

# Time range query
curl "http://localhost:8080/query?ts_start=1700000000&ts_end=1700003600"

# Prefix + time range
curl "http://localhost:8080/query?prefix=sensor&ts_start=1700000000&ts_end=1700003600"
```

### Delete / Patch

```bash
# Delete a single record
curl -X DELETE "http://localhost:8080/record?key=sensor.temp&ts=1700000000"

# Delete a key range (range tombstone, all keys in [key_start, key_end))
curl -X DELETE "http://localhost:8080/range?key_start=sensor.a&key_end=sensor.z"

curl -X PATCH http://localhost:8080/record \
  -H "Content-Type: application/json" \
  -d '{"key": "sensor.temp", "ts": 1700000000, "value": "23.1"}'
```

### Admin & Monitoring

```bash
curl http://localhost:8080/health          # Health check
curl http://localhost:8080/stats           # Engine stats (JSON)
curl http://localhost:8080/metrics         # Prometheus-style metrics
curl http://localhost:8080/admin           # Web dashboard

curl -X POST http://localhost:8080/admin/flush     # Force memtable flush
curl -X POST http://localhost:8080/admin/compact   # Trigger compaction
curl -X POST http://localhost:8080/admin/gc        # Run garbage collection
```

## Rust Library Usage

```rust
use flowdb::{
    Engine, Config, Record, Query, ScanRange, ScanIterator, ReadOptions,
};

let config = Config::default();
let engine = Engine::open(config).await?;

// Write
let records = vec![Record {
    key: "sensor.temp".into(),
    ts: 1700000000,
    expire_at: i64::MAX,
    value: b"22.5".to_vec(),
}];
engine.write_batch(&records).await?;

// Zero-copy write (moves key/value, no clones)
engine.write_batch_owned(records).await?;

// Delete a range of keys (range tombstone)
engine.delete_range("sensor.a", "sensor.z").await?;

// ── Eager query (returns Vec<Record>) ──
let results = engine.query_by_prefix("sensor.").await?;
let results = engine.query_prefix_time_range("sensor.", 1700000000, 1700003600).await?;

// ── Lazy scan iterator (RocksDB-style, recommended for large ranges) ──

// Prefix scan — yields records one-at-a-time, bounded memory
let iter: ScanIterator = engine.scan_prefix("sensor.")?;
for result in iter {
    let record = result?;
    println!("{} @ {} = {:?}", record.key, record.ts, record.value);
}

// Prefix + time range scan
let iter = engine.scan_prefix_time_range("sensor.", 1700000000, 1700003600)?;

// Full key+time range scan with ReadOptions
let iter = engine.scan_opt(
    ScanRange::prefix_time_range("sensor.", 1700000000, 1700003600),
    &ReadOptions { fill_cache: true, verify_checksums: true },
)?;

// Key range scan
let iter = engine.scan(ScanRange::key_range("sensor.a", "sensor.z"))?;

// Full scan
let iter = engine.scan(ScanRange::all())?;

// Take only first N records (lazy — doesn't read the rest)
let first_10: Vec<Record> = engine
    .scan_prefix("sensor.")?
    .take(10)
    .map(|r| r.unwrap())
    .collect();

// ── get_latest: retrieve the most recent record for a key ──
let latest = engine.get_latest("sensor.temp").await?; // Option<Record>

// Shutdown
engine.shutdown().await?;
```

### ScanRange Builders

| Method | Description |
|---|---|
| `ScanRange::prefix(p)` | All records with key prefix `p` |
| `ScanRange::time_range(t1, t2)` | All records in time range `[t1, t2]` |
| `ScanRange::prefix_time_range(p, t1, t2)` | Prefix + time range |
| `ScanRange::key_range(k1, k2)` | Key range `[k1, k2]` |
| `ScanRange::key_time_range(k1, k2, t1, t2)` | Key range + time range |
| `ScanRange::all()` | Full scan |

### Engine API Reference

| Method | Returns | Description |
|---|---|---|
| `scan(range)` | `Result<ScanIterator>` | Lazy iterator scan |
| `scan_opt(range, opts)` | `Result<ScanIterator>` | Lazy scan with `ReadOptions` |
| `scan_prefix(p)` | `Result<ScanIterator>` | Prefix scan (convenience) |
| `scan_prefix_time_range(p, t1, t2)` | `Result<ScanIterator>` | Prefix + time scan (convenience) |
| `get_latest(key)` | `Result<Option<Record>>` | Latest record for key |
| `query(query)` | `Result<Vec<Record>>` | Eager query (backward compat) |
| `get(key, ts)` | `Result<Option<Record>>` | Point get by exact `(key, ts)` |

## Architecture

```
Write Path:
  Client → encode_batch() (outside lock) → WriteWorker mutex → WAL (buffered) + MemTable
                                                                          ↓ (when full)
                                                                      Flush → SSTable

Read Path:
  Query → Active MemTable → Frozen MemTables → Block Index → Bloom Filter → SSTable (LRU cached)
  Scan   → ScanIterator (lazy merge heap over memtable + SST block sources)

Background:
  Flush:    MemTable → SSTable (sorted, lz4-compressed, bloom-filtered)
  Compact:  Size-tiered merge (streaming heap merge, zstd-compressed)
  GC:       Remove fully-expired SSTables
  Delete:   Point deletes (tombstones) + Range deletes (range tombstones)
```

## Benchmarks

```bash
# Stress test
cargo run --release --bin flowdb-stress

# FlowDB vs RocksDB comparison
cargo run --release --example flowdb-vs-rocksdb
```

## License

MIT.
