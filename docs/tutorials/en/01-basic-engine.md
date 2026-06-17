# FlowDB Engine Tutorial — Getting Started with the LSM Engine

[« Back to Tutorials](../index.md)

---

### Objective

Learn how to use the FlowDB LSM engine: open, write, read, query, delete, flush, compact, and shut down.

### Prerequisites

Add `flowdb` to your `Cargo.toml`:

```toml
[dependencies]
flowdb = "0.6"
tempfile = "3"  # for temp directories in examples
```

### Step-by-Step

#### 1. Open the Engine

```rust
use flowdb::{Config, Engine};

let config = Config {
    data_dir: "./my_data".into(),
    auto_background: true,   // auto flush/compact/GC
    ..Default::default()
};
let engine = Engine::open(config).unwrap();
```

- `create_if_missing: true` (default) creates the directory if absent.
- `auto_background: true` spawns an OS thread for periodic flush, compaction, and GC.

#### 2. Write Records

Records have a `key` (binary), `ts` (microsecond timestamp), `expire_at`, and `value` (binary):

```rust
use flowdb::Record;

let records = vec![
    Record::new("sensor:temp", 1_700_000_000_000_000, b"22.5".to_vec()),
    Record::new("sensor:hum",  1_700_000_000_001_000, b"60%".to_vec()),
];
engine.write_batch_owned(records).unwrap();
```

- `write_batch_owned` takes `Vec<Record>` (owned).
- `write_batch` takes `&[Record]` (borrowed).
- `write_batch_sync` is similar but bypasses the background writer.

#### 3. Point Lookup

```rust
let rec = engine.get("sensor:temp", 1_700_000_000_000_000).unwrap();
match rec {
    Some(r) => println!("value = {}", String::from_utf8_lossy(&r.value)),
    None => println!("not found"),
}
```

#### 4. Get Latest Version

```rust
let latest = engine.get_latest("sensor:temp").unwrap();
```

This returns the record with the highest `ts` across memtables and SSTs.

#### 5. Prefix Query

```rust
use flowdb::Query;

let results = engine.query(Query::prefix("sensor:")).unwrap();
```

#### 6. Key Range Query

```rust
let results = engine.query(Query::key_range("sensor:a", "sensor:z")).unwrap();
```

#### 7. Time Range Query

```rust
let results = engine.query(Query::time_range(start_ts, end_ts)).unwrap();
```

#### 8. Combined Query (Prefix + Time Range)

```rust
let results = engine.query(Query::prefix_time_range("sensor:", start_ts, end_ts)).unwrap();
```

#### 9. Lazy Scan Iterator

For large datasets, use `ScanIterator` (implements `Iterator<Item=Result<Record>>`):

```rust
use flowdb::ScanRange;

let mut iter = engine.scan(ScanRange::prefix("sensor:")).unwrap();
while let Some(Ok(rec)) = iter.next() {
    // process one record at a time — no full materialisation
}
```

#### 10. Delete

```rust
engine.delete_batch(&[("sensor:temp".into(), 1_700_000_000_000_000)]).unwrap();
engine.delete_range("sensor:old_a", "sensor:old_z").unwrap();
```

#### 11. Flush & Compact

```rust
engine.flush().unwrap();                                   // memtable → SST
engine.trigger_compaction().unwrap();                      // merge SSTs
engine.trigger_gc().unwrap();                              // purge expired SSTs
```

#### 12. Stats

```rust
let s = engine.stats();
println!("{} sstables, {} MB written", s.sstable_count, s.total_bytes_written / 1024 / 1024);
```

#### 13. Shutdown

```rust
engine.shutdown().unwrap();
```

### Full Working Example

See [`examples/basic_engine.rs`](https://github.com/restsend/flowdb/blob/main/examples/basic_engine.rs).

Run it:

```bash
cargo run --example basic_engine
```
