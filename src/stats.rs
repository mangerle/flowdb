use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Snapshot of engine statistics.
///
/// Returned by [`Engine::stats`]. All latency values are in microseconds.
#[derive(Debug, Serialize, Clone)]
pub struct EngineStats {
    /// Total number of records written since engine start.
    pub total_records_written: u64,
    /// Total bytes written (raw record values, excluding WAL overhead).
    pub total_bytes_written: u64,
    /// Total number of records returned by queries.
    pub total_records_read: u64,
    /// Total number of records that expired and were skipped.
    pub total_records_expired: u64,
    /// Number of memtable-to-SST flushes completed.
    pub total_flushes: u64,
    /// Number of garbage-collection runs completed.
    pub total_gc_runs: u64,
    /// Number of compaction runs completed.
    pub total_compaction_runs: u64,
    /// Total records purged by garbage collection.
    pub records_purged_by_gc: u64,
    /// UDP packets received (only applicable when UDP metrics transport is active).
    pub udp_packets_received: u64,
    /// UDP packets dropped (buffer full or transport error).
    pub udp_packets_dropped: u64,
    /// Total HTTP requests (only applicable when HTTP metrics endpoint is active).
    pub http_requests_total: u64,
    /// Current number of records in the active memtable.
    pub memtable_records: usize,
    /// Current bytes used by the active memtable.
    pub memtable_bytes: usize,
    /// Number of frozen (immutable) memtables waiting to be flushed.
    pub frozen_memtable_count: usize,
    /// Number of SST files on disk (active + compacting).
    pub sstable_count: usize,
    /// Total bytes of all SST files on disk.
    pub sstable_bytes: u64,
    /// Total bytes of all WAL segments on disk.
    pub wal_bytes: u64,
    /// Number of entries in the block meta index.
    pub block_meta_index_entries: usize,
    /// Number of time buckets in the block meta index.
    pub time_index_buckets: usize,
    /// Block cache hit rate as a fraction `[0.0, 1.0]`.
    pub block_cache_hit_rate: f64,
    /// Ratio of compressed block size to uncompressed block size.
    pub compression_ratio: f64,
    /// Write latency p50 in microseconds.
    pub write_latency_p50_us: u64,
    /// Write latency p90 in microseconds.
    pub write_latency_p90_us: u64,
    /// Write latency p99 in microseconds.
    pub write_latency_p99_us: u64,
    /// Query latency p50 in microseconds.
    pub query_latency_p50_us: u64,
    /// Query latency p90 in microseconds.
    pub query_latency_p90_us: u64,
    /// Query latency p99 in microseconds.
    pub query_latency_p99_us: u64,
    /// Flush latency p50 in microseconds.
    pub flush_latency_p50_us: u64,
    /// Flush latency p90 in microseconds.
    pub flush_latency_p90_us: u64,
    /// Flush latency p99 in microseconds.
    pub flush_latency_p99_us: u64,
    /// Engine uptime in seconds.
    pub uptime_secs: u64,
    /// Unix timestamp (seconds) of the last GC run. 0 if never run.
    pub last_gc_at: i64,
    /// Unix timestamp (seconds) of the last flush. 0 if never flushed.
    pub last_flush_at: i64,
    /// Unix timestamp (seconds) of the last compaction. 0 if never compacted.
    pub last_compaction_at: i64,
}

pub struct StatsCounters {
    total_records_written: AtomicU64,
    total_bytes_written: AtomicU64,
    total_records_read: AtomicU64,
    total_records_expired: AtomicU64,
    total_flushes: AtomicU64,
    total_gc_runs: AtomicU64,
    total_compaction_runs: AtomicU64,
    records_purged_by_gc: AtomicU64,
    pub udp_packets_received: AtomicU64,
    pub udp_packets_dropped: AtomicU64,
    pub http_requests_total: AtomicU64,
    memtable_records: AtomicUsize,
    memtable_bytes: AtomicUsize,
    frozen_memtable_count: AtomicUsize,
    sstable_count: AtomicUsize,
    sstable_bytes: AtomicU64,
    wal_bytes: AtomicU64,
    block_meta_index_entries: AtomicUsize,
    time_index_buckets: AtomicUsize,
    compression_ratio_permille: AtomicU64,
    started_at: Instant,
    write_latency: Mutex<Histogram<u64>>,
    query_latency: Mutex<Histogram<u64>>,
    flush_latency: Mutex<Histogram<u64>>,
    last_gc_at: Mutex<i64>,
    last_flush_at: Mutex<i64>,
    last_compaction_at: Mutex<i64>,
}

impl Default for StatsCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsCounters {
    pub fn new() -> Self {
        Self {
            total_records_written: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            total_records_read: AtomicU64::new(0),
            total_records_expired: AtomicU64::new(0),
            total_flushes: AtomicU64::new(0),
            total_gc_runs: AtomicU64::new(0),
            total_compaction_runs: AtomicU64::new(0),
            records_purged_by_gc: AtomicU64::new(0),
            udp_packets_received: AtomicU64::new(0),
            udp_packets_dropped: AtomicU64::new(0),
            http_requests_total: AtomicU64::new(0),
            memtable_records: AtomicUsize::new(0),
            memtable_bytes: AtomicUsize::new(0),
            frozen_memtable_count: AtomicUsize::new(0),
            sstable_count: AtomicUsize::new(0),
            sstable_bytes: AtomicU64::new(0),
            wal_bytes: AtomicU64::new(0),
            block_meta_index_entries: AtomicUsize::new(0),
            time_index_buckets: AtomicUsize::new(0),
            compression_ratio_permille: AtomicU64::new(1000),
            started_at: Instant::now(),
            write_latency: Mutex::new(Histogram::new(3).unwrap()),
            query_latency: Mutex::new(Histogram::new(3).unwrap()),
            flush_latency: Mutex::new(Histogram::new(3).unwrap()),
            last_gc_at: Mutex::new(0),
            last_flush_at: Mutex::new(0),
            last_compaction_at: Mutex::new(0),
        }
    }

    pub fn records_written(&self, count: u64, bytes: u64) {
        self.total_records_written
            .fetch_add(count, Ordering::Relaxed);
        self.total_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_written(&self, bytes: u64) {
        self.records_written(1, bytes);
    }

    pub fn records_read(&self, count: u64) {
        self.total_records_read.fetch_add(count, Ordering::Relaxed);
    }

    pub fn records_expired(&self, count: u64) {
        self.total_records_expired
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn flush_done(&self) {
        self.total_flushes.fetch_add(1, Ordering::Relaxed);
        *self.last_flush_at.lock() = chrono_now_us();
    }

    pub fn gc_done(&self, purged: u64) {
        self.total_gc_runs.fetch_add(1, Ordering::Relaxed);
        self.records_purged_by_gc
            .fetch_add(purged, Ordering::Relaxed);
        *self.last_gc_at.lock() = chrono_now_us();
    }

    pub fn compaction_done(&self) {
        self.total_compaction_runs.fetch_add(1, Ordering::Relaxed);
        *self.last_compaction_at.lock() = chrono_now_us();
    }

    pub fn set_memtable(&self, records: usize, bytes: usize) {
        self.memtable_records.store(records, Ordering::Relaxed);
        self.memtable_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn set_frozen_count(&self, count: usize) {
        self.frozen_memtable_count.store(count, Ordering::Relaxed);
    }

    pub fn set_sstable(&self, count: usize, bytes: u64) {
        self.sstable_count.store(count, Ordering::Relaxed);
        self.sstable_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn add_wal_bytes(&self, bytes: u64) {
        self.wal_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn set_index_stats(&self, entries: usize, buckets: usize) {
        self.block_meta_index_entries
            .store(entries, Ordering::Relaxed);
        self.time_index_buckets.store(buckets, Ordering::Relaxed);
    }

    pub fn set_compression_ratio(&self, ratio: f64) {
        self.compression_ratio_permille
            .store((ratio * 1000.0) as u64, Ordering::Relaxed);
    }

    pub fn record_write_latency(&self, us: u64) {
        if let Some(mut h) = self.write_latency.try_lock() {
            let _ = h.record(us);
        }
    }

    pub fn record_query_latency(&self, us: u64) {
        if let Some(mut h) = self.query_latency.try_lock() {
            let _ = h.record(us);
        }
    }

    pub fn record_flush_latency(&self, us: u64) {
        if let Some(mut h) = self.flush_latency.try_lock() {
            let _ = h.record(us);
        }
    }

    pub fn snapshot(&self, cache_hit_rate: f64) -> EngineStats {
        let cr = self.compression_ratio_permille.load(Ordering::Relaxed) as f64 / 1000.0;

        let (wp50, wp90, wp99) = histogram_percentiles(&self.write_latency);
        let (qp50, qp90, qp99) = histogram_percentiles(&self.query_latency);
        let (fp50, fp90, fp99) = histogram_percentiles(&self.flush_latency);

        EngineStats {
            total_records_written: self.total_records_written.load(Ordering::Relaxed),
            total_bytes_written: self.total_bytes_written.load(Ordering::Relaxed),
            total_records_read: self.total_records_read.load(Ordering::Relaxed),
            total_records_expired: self.total_records_expired.load(Ordering::Relaxed),
            total_flushes: self.total_flushes.load(Ordering::Relaxed),
            total_gc_runs: self.total_gc_runs.load(Ordering::Relaxed),
            total_compaction_runs: self.total_compaction_runs.load(Ordering::Relaxed),
            records_purged_by_gc: self.records_purged_by_gc.load(Ordering::Relaxed),
            udp_packets_received: self.udp_packets_received.load(Ordering::Relaxed),
            udp_packets_dropped: self.udp_packets_dropped.load(Ordering::Relaxed),
            http_requests_total: self.http_requests_total.load(Ordering::Relaxed),
            memtable_records: self.memtable_records.load(Ordering::Relaxed),
            memtable_bytes: self.memtable_bytes.load(Ordering::Relaxed),
            frozen_memtable_count: self.frozen_memtable_count.load(Ordering::Relaxed),
            sstable_count: self.sstable_count.load(Ordering::Relaxed),
            sstable_bytes: self.sstable_bytes.load(Ordering::Relaxed),
            wal_bytes: self.wal_bytes.load(Ordering::Relaxed),
            block_meta_index_entries: self.block_meta_index_entries.load(Ordering::Relaxed),
            time_index_buckets: self.time_index_buckets.load(Ordering::Relaxed),
            block_cache_hit_rate: cache_hit_rate,
            compression_ratio: cr,
            write_latency_p50_us: wp50,
            write_latency_p90_us: wp90,
            write_latency_p99_us: wp99,
            query_latency_p50_us: qp50,
            query_latency_p90_us: qp90,
            query_latency_p99_us: qp99,
            flush_latency_p50_us: fp50,
            flush_latency_p90_us: fp90,
            flush_latency_p99_us: fp99,
            uptime_secs: self.started_at.elapsed().as_secs(),
            last_gc_at: *self.last_gc_at.lock(),
            last_flush_at: *self.last_flush_at.lock(),
            last_compaction_at: *self.last_compaction_at.lock(),
        }
    }

    pub fn to_prometheus(&self, cache_hit_rate: f64) -> String {
        let s = self.snapshot(cache_hit_rate);
        format!(
            include_str!("prometheus_template.txt"),
            s.total_records_written,
            s.total_bytes_written,
            s.total_records_read,
            s.total_records_expired,
            s.total_flushes,
            s.total_gc_runs,
            s.total_compaction_runs,
            s.records_purged_by_gc,
            s.udp_packets_received,
            s.udp_packets_dropped,
            s.http_requests_total,
            s.memtable_records,
            s.memtable_bytes,
            s.frozen_memtable_count,
            s.sstable_count,
            s.sstable_bytes,
            s.wal_bytes,
            s.block_meta_index_entries,
            s.time_index_buckets,
            s.block_cache_hit_rate,
            s.compression_ratio,
            s.write_latency_p50_us,
            s.write_latency_p90_us,
            s.write_latency_p99_us,
            s.query_latency_p50_us,
            s.query_latency_p90_us,
            s.query_latency_p99_us,
            s.flush_latency_p50_us,
            s.flush_latency_p90_us,
            s.flush_latency_p99_us,
            s.uptime_secs,
        )
    }
}

fn histogram_percentiles(h: &Mutex<Histogram<u64>>) -> (u64, u64, u64) {
    let h = h.lock();
    (
        h.value_at_percentile(50.0),
        h.value_at_percentile(90.0),
        h.value_at_percentile(99.0),
    )
}

fn chrono_now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}
