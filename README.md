# FlowDB

A high-performance embedded time-series storage engine written in Rust, powered by an LSM-tree architecture with WAL, SSTables, and Bloom filters.

## Benchmark Results

FlowDB vs RocksDB comparison (100K records, 128B values, batch=100, release build, Apple M-series):

| Category | FlowDB | RocksDB | Result |
|---|---|---|---|
| Sequential Write | 2.0M ops/s | 3.1M ops/s | RocksDB 1.58x faster |
| Concurrent Write (8 threads) | 3.2M ops/s | 4.4M ops/s | RocksDB 1.38x faster |
| Point Query | 4.7M ops/s | 539K ops/s | **FlowDB 8.7x faster** |
| Prefix Scan (~200 recs) | 71K ops/s | 11K ops/s | **FlowDB 6.3x faster** |
| Full Scan (200K recs) | 73 ops/s | 41 ops/s | **FlowDB 1.77x faster** |
| Storage | 2.0MB | 1.8MB | ~same |

FlowDB is **read-optimized** — significantly faster than RocksDB on point queries and scans, at the cost of moderately lower write throughput due to per-record WAL checksums and single-writer serialization.

```bash
cargo run --release --example flowdb-vs-rocksdb
```

## Features

- LSM-tree storage with WAL (write-ahead log) for crash recovery
- **Per-record WAL checksums** — corruption detected on replay, bad records rejected
- **Config validation** — invalid configs rejected at startup instead of crashing
- **Frozen memtable backpressure** — writes stall when flush can't keep up
- **Lazy scan iterator** (RocksDB-style `ScanIterator`) for bounded-memory range scans
- **`get_latest(key)`** for retrieving the most recent record by key
- Bloom filters for fast point query negative checks
- Dual compression: lz4 for flush (speed), zstd for compaction (ratio)
- Buffered WAL writes (256KB buffer) for reduced syscall overhead
- WAL pre-encoding outside the write lock for better concurrency
- Time-bucketed block index with binary search
- **LRU block cache** (64 shards, powered by `lru` crate) with true LRU eviction
- BTreeMap-based active memtable for O(log n) operations
- Zero-copy owned write path (`write_batch_owned`)
- Synchronous write path (`write_batch_sync`) for non-async callers
- **Size-tiered compaction** with streaming heap merge (low memory footprint)
- **Range tombstones** (`delete_range`) for efficient bulk key-range deletion
- Garbage collection (TTL expiry), and point deletes
- **Graceful shutdown** — `shutdown()` flushes WAL + memtables before exit
- **Engine stats** — `engine.stats()` returns structured counters; `engine.metrics_text()` returns Prometheus-format string

## Quick Start

```toml
[dependencies]
flowdb = "0.2"
```

### Rust Library Usage

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

// Graceful shutdown (flushes WAL + memtables)
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
| `write_batch(recs)` | `Result<()>` | Batch write |
| `write_batch_owned(recs)` | `Result<()>` | Zero-copy batch write |
| `delete_batch(recs)` | `Result<()>` | Batch point deletes |
| `delete_range(start, end)` | `Result<()>` | Range tombstone delete |
| `patch_record(...)` | `Result<Record>` | Update value/TTL of existing record |
| `flush()` | `Result<()>` | Force memtable flush to SSTable |
| `trigger_gc()` | `Result<usize>` | Run garbage collection |
| `trigger_compaction()` | `Result<bool>` | Trigger size-tiered compaction |
| `shutdown()` | `Result<()>` | Graceful shutdown (flush + cleanup) |
| `stats()` | `EngineStats` | Structured engine counters |
| `metrics_text()` | `String` | Prometheus-format metrics string |

## Configuration

```rust
use flowdb::Config;

let config = Config {
    data_dir: "./data".into(),
    memtable_size_mb: 64,        // Active memtable flush threshold
    max_frozen_memtables: 2,     // Backpressure limit
    block_size: 8192,            // SSTable block size (records per block)
    zstd_level: 3,               // Compaction compression level (1-22)
    bloom_bits_per_key: 10,      // Bloom filter precision
    wal_segment_size_mb: 64,     // WAL segment rotation size
    compaction_threshold: 2,     // SSTable count to trigger compaction
    flush_interval_ms: 1000,     // Background flush interval
    gc_interval_secs: 3600,      // GC interval
    time_bucket_secs: 3600,      // Block index time granularity
    block_cache_capacity_mb: 128,// LRU block cache size
    index_memory_budget_mb: 256, // Block index memory budget
    ..Default::default()
};
```

| Parameter | Default | Description |
|---|---|---|
| `data_dir` | `"./data"` | Data directory path |
| `create_if_missing` | `true` | Create data directory if it doesn't exist |
| `memtable_size_mb` | `64` | Active memtable size threshold (MB) before flush |
| `max_frozen_memtables` | `2` | Max frozen memtables before writes block |
| `block_size` | `8192` | SSTable block size (number of records per block) |
| `zstd_level` | `3` | Zstd compression level (1-22) |
| `bloom_bits_per_key` | `10` | Bloom filter bits per key |
| `wal_segment_size_mb` | `64` | WAL segment file size before rotation (MB) |
| `compaction_threshold` | `2` | Number of SSTables to trigger compaction |
| `flush_interval_ms` | `1000` | Background flush interval (ms) |
| `gc_interval_secs` | `3600` | Garbage collection interval (seconds) |
| `default_ttl_secs` | `None` | Default TTL for records without explicit expiry |
| `time_bucket_secs` | `3600` | Time bucket granularity for block index |
| `index_memory_budget_mb` | `256` | Memory budget for block index (MB) |
| `block_cache_capacity_mb` | `128` | Block cache capacity (MB) |
| `wal_sync_mode` | `IntervalMs(1000)` | WAL fsync mode (`Always`, `IntervalMs(n)`, `None`) |

## Architecture

```
Write Path:
  Client → encode_batch() (outside lock) → WriteWorker mutex → WAL (buffered + checksummed) + MemTable
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

# Micro-benchmarks
cargo bench

# Coverage
cargo llvm-cov --summary-only
```

## Project Layout

```
src/
  lib.rs              – public API surface (Config, Engine, Record, Query, ...)
  engine.rs           – Engine + ScanIterator (the core)
  memtable.rs         – in-memory write buffer (MemTables)
  wal.rs              – write-ahead log (checksummed)
  sstable.rs          – on-disk sorted-string table reader/writer
  block_meta_index.rs – fine-grained block-level index
  bloom.rs            – bloom filter for SST point queries
  cache.rs            – block cache (LRU)
  compaction.rs       – size-tiered compaction
  gc.rs               – expired-SST garbage collection
  manifest.rs         – append-only manifest log
  record.rs           – Record / InternalRecord / Query / Config types
  write_worker.rs     – single-writer worker driving WAL + memtable
  stats.rs            – engine stats + Prometheus exporter
  error.rs            – FlowError / Result
  bin/
    flowdb-stress.rs  – stress-testing / benchmarking binary
```

## License

MIT.
