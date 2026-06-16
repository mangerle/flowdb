# FlowDB

A high-performance embedded storage engine written in Rust, powered by an LSM-tree architecture with WAL, SSTables, and Bloom filters. Now includes **JsonDB** — a built-in IndexedDB-compatible JSON document database layer with ACID transactions and secondary indexes.

**Fully synchronous API** — no async runtime required. FlowDB uses plain OS threads for background maintenance, making it runtime-agnostic. Use it from Tokio, async-std, smol, or plain synchronous code without any wrappers.

## Benchmark Results (v0.4.0)

### LSM Engine vs RocksDB (100K records, 128B values, batch=100, release, Apple M-series)

| Category | FlowDB | RocksDB | Result |
|---|---|---|---|
| Sequential Write | 4.5M ops/s | 3.1M ops/s | **FlowDB 1.42x faster** |
| Concurrent Write (8 threads) | 9.4M ops/s | 4.7M ops/s | **FlowDB 2.02x faster** |
| Point Query | 6.0M ops/s | 549K ops/s | **FlowDB 10.95x faster** |
| Prefix Scan (~200 recs) | 72K ops/s | 11K ops/s | **FlowDB 6.39x faster** |
| Full Scan (200K recs) | 65 ops/s | 40 ops/s | **FlowDB 1.63x faster** |
| Storage | 2.0MB | 1.8MB | ~same |

### JsonDB Document Layer (release, Apple M-series, SyncMode::Always)

| Category | Throughput |
|---|---|
| Sequential write (single doc) | ~121 docs/s |
| Batch write (100 docs/batch) | ~7,057 docs/s |
| Point read by primary key | ~244,741 ops/s |
| Index lookups (equality) | ~9,402 queries/s |
| Auto-increment | ~53 ops/s |

> Note: Write throughput is bottlenecked by WAL fsync (`SyncMode::Always`).
> For higher throughput, use batch writes (transaction) or `SyncMode::IntervalMs`.

## Features

### LSM Engine
- LSM-tree storage with WAL (write-ahead log) for crash recovery
- **Fully synchronous API** — no `async`, no Tokio dependency, runtime-agnostic
- **Background maintenance on OS threads** — flush, compaction, GC via `std::thread`
- **Per-record WAL checksums** — corruption detected on replay, bad records rejected
- **Config validation** — invalid configs rejected at startup instead of crashing
- **Frozen memtable backpressure** — writes stall when flush can't keep up
- **Lazy scan iterator** (RocksDB-style `ScanIterator`) for bounded-memory range scans
- Bloom filters for fast point query negative checks
- lz4 compression for all SST blocks (flush + compaction)
- Buffered WAL writes (256KB buffer) for reduced syscall overhead
- WAL pre-encoding outside the write lock for better concurrency
- Time-bucketed block index with binary search
- **LRU block cache** (64 shards) with true LRU eviction
- Vec-based active memtable for O(1) writes
- **Size-tiered compaction** with streaming heap merge (low memory footprint)
- **Range tombstones** for efficient bulk key-range deletion
- Garbage collection (TTL expiry), and point deletes
- **Graceful shutdown** — flushes WAL + memtables before exit
- **Engine stats** — structured counters + Prometheus-format metrics

### JsonDB (IndexedDB-compatible layer)
- **ACID transactions** with atomic batch commit (OCC optimistic concurrency)
- **Secondary indexes** with automatic maintenance on CRUD
- **Unique constraint** enforcement on indexed fields
- **Auto-increment** primary keys
- **Read-your-writes** consistency within transactions
- **Snapshot isolation** via MVCC sequence numbers
- **Schema persistence** across restarts (automatic recovery)
- **IndexedDB-like API** — `create_object_store`, `create_index`, `put`, `get`, `delete`, `transaction`
- **Index queries** — point lookup (`get_by_index`) and range queries (`range_by_index`)
- **Multi-field indexes** via dotted key paths (e.g. `"address.city"`)

## Quick Start

### LSM Engine API

```toml
[dependencies]
flowdb = "0.4"
```

```rust
use flowdb::{Engine, Config, Record, Query, ScanRange};

let config = Config::default();
let engine = Engine::open(config)?;

// Write
let records = vec![Record {
    key: "sensor.temp".into(),
    ts: 1700000000,
    expire_at: i64::MAX,
    value: b"22.5".to_vec(),
}];
engine.write_batch(&records)?;

// Read (lazy scan iterator)
for result in engine.scan_prefix("sensor.")? {
    let record = result?;
    println!("{} @ {} = {:?}", record.key, record.ts, record.value);
}

engine.shutdown()?;
```

### JsonDB API (IndexedDB-compatible)

```toml
[dependencies]
flowdb = "0.4"
serde_json = "1"
```

```rust
use flowdb::jsondb::{JsonDB, TransactionMode, SortDir};
use serde_json::json;

let db = JsonDB::open(Default::default())?;

// Define schema (like IndexedDB onupgradeneeded)
db.create_object_store("users", "id")?;
db.create_index("users", "by_email", &["email"], true)?;   // unique single-field
db.create_index("users", "by_age",   &["age"],   false)?;  // non-unique single-field
db.create_index("users", "by_city_age", &["city", "age"], false)?; // composite!

// Simple operations (auto-commit)
db.put("users", json!({"id": "u1", "email": "a@b.com", "age": 30, "city": "NYC"}))?;
let doc = db.get("users", &json!("u1"))?;

// Index queries (single-field)
let docs = db.get_by_index("users", "by_email", &json!("a@b.com"))?;

// Index queries (composite — exact match on all fields)
let docs = db.get_by_index("users", "by_city_age", &json!(["NYC", 30]))?;

// QueryBuilder — type-safe composite queries with filters, order_by, limit
let docs: Vec<serde_json::Value> = db.query("users")
    .where_eq("city", json!("NYC"))
    .where_range("age", json!(25), json!(35))
    .order_by("age", SortDir::Asc)
    .limit(10)
    .collect()?;

// Explicit transaction (atomic batch commit)
let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite)?;
tx.put("users", json!({"id": "u2", "email": "b@c.com", "age": 25}))?;
tx.commit()?; // all-or-nothing

// Generic (serde) API — work with typed structs instead of serde_json::Value
#[derive(serde::Serialize, serde::Deserialize)]
struct User {
    id: String,
    email: String,
    age: u32,
}

let alice = User { id: "u3".into(), email: "alice@c.com".into(), age: 28 };

// Insert a typed struct
db.put_doc("users", &alice)?;

// Retrieve as a typed struct
let user: Option<User> = db.get_doc("users", "u3")?;

// Query with typed results
let users: Vec<User> = db.query("users")
    .where_eq("age", json!(28))
    .collect_doc()?;
```

## Configuration

```rust
use flowdb::Config;

let config = Config {
    data_dir: "./data".into(),
    memtable_size_mb: 64,
    wal_sync_mode: flowdb::SyncMode::Always, // default — fsync every batch
    ..Default::default()
};
```

| Parameter | Default | Description |
|---|---|---|
| `data_dir` | `"./data"` | Data directory path |
| `wal_sync_mode` | `Always` | WAL fsync: `Always` (safe) or `IntervalMs(n)` (fast) |
| `memtable_size_mb` | `64` | Active memtable flush threshold |
| `block_cache_capacity_mb` | `128` | Block cache capacity (MB) |
| ... | ... | (full table in source docs) |

## Architecture

```
LSM Engine:
  Write Path:  Client → encode → WriteWorker → WAL (fsync) + MemTable → (flush) SST
  Read Path:   Client → MemTable → Block Index → Bloom → SST (LRU cached)

JsonDB Layer (built-in):
  Document:   D\x00{store}\x00{primary_key} → serialized JSON
  Index:      I\x00{store}\x00{index}\x00{encoded_value}\x00{primary_key} → primary_key
  Schema:     S\x00{store} → serialized StoreDef
```

## Project Layout

```
src/
  engine.rs           – LSM Engine + ScanIterator
  memtable.rs         – in-memory write buffer
  wal.rs              – write-ahead log (checksummed)
  sstable.rs          – on-disk sorted-string table
  block_meta_index.rs – block-level index
  bloom.rs            – bloom filter
  cache.rs            – LRU block cache
  compaction.rs       – size-tiered compaction
  manifest.rs         – append-only manifest log
  jsondb/             – JsonDB layer (IndexedDB-compatible)
    mod.rs            – JsonDB, Transaction, API
    encoding.rs       – key/value encoding, field extraction
    schema.rs         – StoreDef, IndexDef, persistence
  bin/
    flowdb-stress.rs  – stress-testing binary
  lib.rs              – public API surface
```

## License

MIT.
