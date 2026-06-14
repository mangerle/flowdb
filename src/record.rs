use serde::{Deserialize, Serialize};
use std::ops::Bound;
use std::path::PathBuf;

/// Serde adapter for `Record.key` (`Vec<u8>`).
///
/// Serialises as a UTF-8 **string** for HTTP / JSON API compatibility (lossy
/// replacement bytes `U+FFFD` for non-UTF-8 keys). On deserialise, accepts
/// either a JSON string (converted to its UTF-8 bytes) or a JSON array of
/// bytes (preserved as-is). This keeps old clients that send `"key": "abc"`
/// working while letting future binary-key clients send raw bytes.
pub(crate) mod key_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(key: &[u8], s: S) -> Result<S::Ok, S::Error> {
        let cow = String::from_utf8_lossy(key);
        s.serialize_str(&cow)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum KeyRepr {
            Str(String),
            Bytes(Vec<u8>),
        }
        match KeyRepr::deserialize(d)? {
            KeyRepr::Str(s) => Ok(s.into_bytes()),
            KeyRepr::Bytes(b) => Ok(b),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    #[serde(with = "key_serde")]
    pub key: Vec<u8>,
    pub ts: i64,
    pub expire_at: i64,
    pub value: Vec<u8>,
}

impl Record {
    /// Convenience constructor mirroring the old `String`-keyed API.
    pub fn new(key: impl Into<Vec<u8>>, ts: i64, value: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            ts,
            expire_at: i64::MAX,
            value,
        }
    }

    /// View the key as a UTF-8 string slice when callers know keys are text.
    /// Returns the raw bytes as a lossy string for non-UTF-8 keys.
    pub fn key_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.key)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Op {
    #[default]
    Put = 0,
    Delete = 1,
    DeleteRange = 2,
}

impl Op {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Op::Delete,
            2 => Op::DeleteRange,
            _ => Op::Put,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InternalRecord {
    pub seq: u64,
    pub op: Op,
    pub key: Vec<u8>,
    pub ts: i64,
    pub expire_at: i64,
    pub value: Vec<u8>,
    pub range_end: Option<Vec<u8>>,
}

impl InternalRecord {
    pub fn from_record(rec: &Record, seq: u64) -> Self {
        Self {
            seq,
            op: Op::Put,
            key: rec.key.clone(),
            ts: rec.ts,
            expire_at: rec.expire_at,
            value: rec.value.clone(),
            range_end: None,
        }
    }

    pub fn delete(key: Vec<u8>, ts: i64, seq: u64) -> Self {
        Self {
            seq,
            op: Op::Delete,
            key,
            ts,
            expire_at: i64::MAX,
            value: vec![],
            range_end: None,
        }
    }

    pub fn delete_range(start_key: Vec<u8>, end_key: Vec<u8>, seq: u64) -> Self {
        Self {
            seq,
            op: Op::DeleteRange,
            key: start_key,
            ts: 0,
            expire_at: i64::MAX,
            value: vec![],
            range_end: Some(end_key),
        }
    }

    pub fn to_record(&self) -> Record {
        Record {
            key: self.key.clone(),
            ts: self.ts,
            expire_at: self.expire_at,
            value: self.value.clone(),
        }
    }

    pub fn into_record_owned(self) -> Record {
        Record {
            key: self.key,
            ts: self.ts,
            expire_at: self.expire_at,
            value: self.value,
        }
    }

    pub fn estimated_size(&self) -> usize {
        let base = 8 + 1 + 2 + self.key.len() + 8 + 8 + 4 + self.value.len();
        match &self.range_end {
            Some(re) => base + 2 + re.len(),
            None => base,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    /// fsync the WAL after every write batch. Maximum durability, lower
    /// throughput (each batch incurs a synchronous disk I/O).
    Always,
    /// fsync the WAL on a periodic tick (milliseconds). Balances durability
    /// with throughput by coalescing multiple batches into a single fsync.
    /// Requires the background maintenance task to be running.
    IntervalMs(u64),
}

impl Default for SyncMode {
    fn default() -> Self {
        SyncMode::Always
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
    pub default_ttl_secs: Option<u64>,
    pub gc_interval_secs: u64,
    pub memtable_size_mb: usize,
    pub max_frozen_memtables: usize,
    pub block_size: usize,
    pub zstd_level: i32,
    pub flush_interval_ms: u64,
    pub time_bucket_secs: u64,
    pub index_memory_budget_mb: usize,
    pub block_cache_capacity_mb: usize,
    pub bloom_bits_per_key: usize,
    pub wal_segment_size_mb: u64,
    pub compaction_threshold: usize,
    pub create_if_missing: bool,
    /// Controls how the WAL is synchronised to stable storage.
    /// `Always` guarantees every acknowledged write is on disk before
    /// returning. `IntervalMs(n)` batches fsync calls up to every _n_
    /// milliseconds for higher throughput at the cost of potentially
    /// losing the most recent writes on power failure.
    #[serde(default)]
    pub wal_sync_mode: SyncMode,
    /// When true, the engine automatically spawns a background task that
    /// periodically flushes the memtable, compacts SSTables, garbage-collects
    /// expired data, and syncs the WAL (for `IntervalMs` mode). Set to false
    /// in tests that want full control over when flush/compact/GC occur.
    #[serde(default = "default_background")]
    pub auto_background: bool,
}

fn default_background() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            default_ttl_secs: None,
            gc_interval_secs: 3600,
            memtable_size_mb: 64,
            max_frozen_memtables: 2,
            block_size: 8192,
            zstd_level: 3,
            flush_interval_ms: 1000,
            time_bucket_secs: 3600,
            index_memory_budget_mb: 256,
            block_cache_capacity_mb: 128,
            bloom_bits_per_key: 10,
            wal_segment_size_mb: 64,
            compaction_threshold: 2,
            create_if_missing: true,
            wal_sync_mode: SyncMode::Always,
            auto_background: true,
        }
    }
}

#[derive(Debug, Clone)]
pub enum KeyFilter {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
    All,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub key_filter: KeyFilter,
    pub time_range: Option<(i64, i64)>,
}

impl Query {
    pub fn prefix(key: impl Into<String>) -> Self {
        Self {
            key_filter: KeyFilter::Prefix(key.into().into_bytes()),
            time_range: None,
        }
    }

    pub fn key_range(start: impl Into<String>, end: impl Into<String>) -> Self {
        Self {
            key_filter: KeyFilter::Range {
                start: start.into().into_bytes(),
                end: end.into().into_bytes(),
            },
            time_range: None,
        }
    }

    pub fn time_range(start: i64, end: i64) -> Self {
        Self {
            key_filter: KeyFilter::All,
            time_range: Some((start, end)),
        }
    }

    pub fn prefix_time_range(key: impl Into<String>, start: i64, end: i64) -> Self {
        Self {
            key_filter: KeyFilter::Prefix(key.into().into_bytes()),
            time_range: Some((start, end)),
        }
    }

    pub fn key_time_range(
        start_key: impl Into<String>,
        end_key: impl Into<String>,
        start: i64,
        end: i64,
    ) -> Self {
        Self {
            key_filter: KeyFilter::Range {
                start: start_key.into().into_bytes(),
                end: end_key.into().into_bytes(),
            },
            time_range: Some((start, end)),
        }
    }
}

/// Read options for scan / get operations (RocksDB-style).
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Whether to populate the block cache on reads (default: true).
    pub fill_cache: bool,
    /// Whether to verify checksums on SST reads (default: true).
    pub verify_checksums: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            fill_cache: true,
            verify_checksums: true,
        }
    }
}

/// Scan range specifying key + time bounds for `Engine::scan`.
#[derive(Debug, Clone)]
pub struct ScanRange {
    pub key_start: Bound<Vec<u8>>,
    pub key_end: Bound<Vec<u8>>,
    pub ts_start: Bound<i64>,
    pub ts_end: Bound<i64>,
}

impl ScanRange {
    /// Prefix scan: `[prefix, next_prefix)` with unbounded time.
    pub fn prefix(p: impl AsRef<str>) -> Self {
        let bytes = p.as_ref().as_bytes().to_vec();
        let end = increment_prefix_bytes(&bytes);
        Self {
            key_start: Bound::Included(bytes.clone()),
            key_end: Bound::Excluded(end),
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        }
    }

    /// Time range scan with unbounded key.
    pub fn time_range(start: i64, end: i64) -> Self {
        Self {
            key_start: Bound::Unbounded,
            key_end: Bound::Unbounded,
            ts_start: Bound::Included(start),
            ts_end: Bound::Included(end),
        }
    }

    /// Prefix + time range scan.
    pub fn prefix_time_range(p: impl AsRef<str>, ts_start: i64, ts_end: i64) -> Self {
        let bytes = p.as_ref().as_bytes().to_vec();
        let end = increment_prefix_bytes(&bytes);
        Self {
            key_start: Bound::Included(bytes.clone()),
            key_end: Bound::Excluded(end),
            ts_start: Bound::Included(ts_start),
            ts_end: Bound::Included(ts_end),
        }
    }

    /// Key range scan: `[start, end]` with unbounded time.
    pub fn key_range(start: impl AsRef<str>, end: impl AsRef<str>) -> Self {
        Self {
            key_start: Bound::Included(start.as_ref().as_bytes().to_vec()),
            key_end: Bound::Included(end.as_ref().as_bytes().to_vec()),
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        }
    }

    /// Key range + time range scan.
    pub fn key_time_range(
        start: impl AsRef<str>,
        end: impl AsRef<str>,
        ts_start: i64,
        ts_end: i64,
    ) -> Self {
        Self {
            key_start: Bound::Included(start.as_ref().as_bytes().to_vec()),
            key_end: Bound::Included(end.as_ref().as_bytes().to_vec()),
            ts_start: Bound::Included(ts_start),
            ts_end: Bound::Included(ts_end),
        }
    }

    /// Full scan (all keys, all times).
    pub fn all() -> Self {
        Self {
            key_start: Bound::Unbounded,
            key_end: Bound::Unbounded,
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        }
    }

    /// Convert to the internal `KeyFilter` + optional `(ts_start, ts_end)`.
    pub(crate) fn to_query_params(&self) -> (KeyFilter, Option<(i64, i64)>) {
        let kf = match (&self.key_start, &self.key_end) {
            (Bound::Included(s), Bound::Excluded(e)) if is_prefix_range(s, e) => {
                KeyFilter::Prefix(s.clone())
            }
            (Bound::Included(s), Bound::Included(e)) => KeyFilter::Range {
                start: s.clone(),
                end: e.clone(),
            },
            (Bound::Included(s), Bound::Excluded(e)) => KeyFilter::Range {
                start: s.clone(),
                end: e.clone(),
            },
            _ => KeyFilter::All,
        };
        let tr = match (&self.ts_start, &self.ts_end) {
            (Bound::Included(s), Bound::Included(e)) => Some((*s, *e)),
            _ => None,
        };
        (kf, tr)
    }
}

fn increment_prefix_bytes(key: &[u8]) -> Vec<u8> {
    let mut bytes = key.to_vec();
    while let Some(last) = bytes.last_mut() {
        if *last < 255 {
            *last += 1;
            return bytes;
        }
        bytes.pop();
    }
    let mut sentinel = key.to_vec();
    sentinel.push(0);
    sentinel
}

fn is_prefix_range(start: &[u8], end: &[u8]) -> bool {
    end == increment_prefix_bytes(start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_record_estimated_size() {
        let rec = Record {
            key: b"hello".to_vec(),
            ts: 100,
            expire_at: i64::MAX,
            value: vec![1, 2, 3, 4, 5],
        };
        let irec = InternalRecord::from_record(&rec, 1);
        let expected = 8 + 1 + 2 + 5 + 8 + 8 + 4 + 5;
        assert_eq!(irec.estimated_size(), expected);
    }

    #[test]
    fn test_internal_record_estimated_size_empty() {
        let rec = Record {
            key: Vec::new(),
            ts: 0,
            expire_at: 0,
            value: Vec::new(),
        };
        let irec = InternalRecord::from_record(&rec, 0);
        let expected = (8 + 1 + 2) + 8 + 8 + 4;
        assert_eq!(irec.estimated_size(), expected);
    }

    #[test]
    fn test_query_key_time_range_constructor() {
        let q = Query::key_time_range("a", "z", 100, 200);
        match &q.key_filter {
            KeyFilter::Range { start, end } => {
                assert_eq!(start, b"a".as_slice());
                assert_eq!(end, b"z".as_slice());
            }
            _ => panic!("expected range"),
        }
        assert_eq!(q.time_range, Some((100, 200)));
    }

    #[test]
    fn test_query_prefix_time_range_constructor() {
        let q = Query::prefix_time_range("key", 100, 200);
        match &q.key_filter {
            KeyFilter::Prefix(k) => assert_eq!(k, b"key".as_slice()),
            _ => panic!("expected prefix"),
        }
        assert_eq!(q.time_range, Some((100, 200)));
    }

    #[test]
    fn test_query_time_range_constructor() {
        let q = Query::time_range(100, 200);
        assert!(matches!(q.key_filter, KeyFilter::All));
        assert_eq!(q.time_range, Some((100, 200)));
    }

    #[test]
    fn test_config_default_values() {
        let config = Config::default();
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.default_ttl_secs, None);
        assert_eq!(config.gc_interval_secs, 3600);
        assert_eq!(config.memtable_size_mb, 64);
        assert_eq!(config.max_frozen_memtables, 2);
        assert_eq!(config.block_size, 8192);
        assert_eq!(config.zstd_level, 3);
        assert_eq!(config.flush_interval_ms, 1000);
        assert_eq!(config.time_bucket_secs, 3600);
        assert_eq!(config.index_memory_budget_mb, 256);
        assert_eq!(config.block_cache_capacity_mb, 128);
        assert_eq!(config.bloom_bits_per_key, 10);
        assert_eq!(config.wal_segment_size_mb, 64);
        assert_eq!(config.compaction_threshold, 2);
        assert!(config.create_if_missing);
    }

    #[test]
    fn test_internal_record_from_to_record_roundtrip() {
        let rec = Record {
            key: b"test".to_vec(),
            ts: 42,
            expire_at: 100,
            value: vec![7, 8, 9],
        };
        let irec = InternalRecord::from_record(&rec, 99);
        assert_eq!(irec.seq, 99);
        assert_eq!(irec.key, b"test".as_slice());
        assert_eq!(irec.ts, 42);
        assert_eq!(irec.expire_at, 100);
        assert_eq!(irec.value, vec![7, 8, 9]);

        let back = irec.to_record();
        assert_eq!(back.key, rec.key);
        assert_eq!(back.ts, rec.ts);
        assert_eq!(back.expire_at, rec.expire_at);
        assert_eq!(back.value, rec.value);
    }

    #[test]
    fn test_read_options_default() {
        let opts = ReadOptions::default();
        assert!(opts.fill_cache);
        assert!(opts.verify_checksums);
    }

    #[test]
    fn test_scan_range_prefix() {
        let r = ScanRange::prefix("foo");
        assert!(matches!(r.key_start, Bound::Included(_)));
        assert!(matches!(r.key_end, Bound::Excluded(_)));
        assert!(matches!(r.ts_start, Bound::Unbounded));
        let (kf, tr) = r.to_query_params();
        assert!(matches!(kf, KeyFilter::Prefix(_)));
        assert!(tr.is_none());
    }

    #[test]
    fn test_scan_range_time_range() {
        let r = ScanRange::time_range(10, 20);
        assert!(matches!(r.key_start, Bound::Unbounded));
        let (kf, tr) = r.to_query_params();
        assert!(matches!(kf, KeyFilter::All));
        assert_eq!(tr, Some((10, 20)));
    }

    #[test]
    fn test_scan_range_prefix_time_range() {
        let r = ScanRange::prefix_time_range("bar", 1, 100);
        let (kf, tr) = r.to_query_params();
        assert!(matches!(kf, KeyFilter::Prefix(_)));
        assert_eq!(tr, Some((1, 100)));
    }

    #[test]
    fn test_scan_range_key_range() {
        let r = ScanRange::key_range("a", "z");
        let (kf, tr) = r.to_query_params();
        match kf {
            KeyFilter::Range { start, end } => {
                assert_eq!(start, b"a");
                assert_eq!(end, b"z");
            }
            _ => panic!("expected range"),
        }
        assert!(tr.is_none());
    }

    #[test]
    fn test_scan_range_all() {
        let r = ScanRange::all();
        let (kf, tr) = r.to_query_params();
        assert!(matches!(kf, KeyFilter::All));
        assert!(tr.is_none());
    }

    #[test]
    fn test_increment_prefix_bytes() {
        assert_eq!(increment_prefix_bytes(b"abc"), b"abd");
        assert_eq!(increment_prefix_bytes(b"a\xff"), b"b");
        // \xff\xff → all bytes overflow, sentinel [0xff, 0xff, 0] appended
        assert_eq!(increment_prefix_bytes(b"\xff\xff"), vec![0xff, 0xff, 0]);
    }
}
