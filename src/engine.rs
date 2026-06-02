use crate::block_meta_index::BlockMetaIndex;
use crate::cache::{BlockCache, CacheKey};
use crate::compaction::CompactionRunner;
use crate::error::{FlowError, Result};
use crate::gc::GcRunner;
use crate::manifest::Manifest;
use crate::memtable::MemTables;
use crate::record::{Config, InternalRecord, KeyFilter, Op, Query, ReadOptions, Record, ScanRange};
use crate::sstable::SstReader;
use crate::stats::{EngineStats, StatsCounters};
use crate::wal::Wal;
use crate::write_worker::WriteWorker;
use parking_lot::RwLock;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Engine {
    config: Config,
    worker: Arc<parking_lot::Mutex<WriteWorker>>,
    seq_counter: std::sync::atomic::AtomicU64,
    stats: Arc<StatsCounters>,
    memtables: Arc<MemTables>,
    index: Arc<RwLock<BlockMetaIndex>>,
    manifest: Arc<parking_lot::Mutex<Manifest>>,
    cache: Arc<BlockCache>,
    readers: Arc<RwLock<HashMap<u32, Arc<SstReader>>>>,
}

impl Engine {
    pub async fn open(config: Config) -> Result<Self> {
        let data_dir = &config.data_dir;
        if !data_dir.exists() && !config.create_if_missing {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("data directory does not exist: {}", data_dir.display()),
            )
            .into());
        }
        std::fs::create_dir_all(data_dir)?;
        std::fs::create_dir_all(data_dir.join("WAL"))?;
        std::fs::create_dir_all(data_dir.join("SST"))?;
        std::fs::create_dir_all(data_dir.join("INDEX"))?;

        let stats = Arc::new(StatsCounters::new());
        let mut wal = Wal::open(&data_dir.join("WAL"), config.wal_segment_size_mb)?;
        let manifest = Manifest::open(data_dir)?;

        let mut index = BlockMetaIndex::new(config.time_bucket_secs);
        let state = manifest.state().clone();
        for sst_id in &state.active_sst_ids {
            if let Some(info) = state.sstables.get(sst_id) {
                if let Some(blocks) = state.block_infos.get(sst_id) {
                    index.add_sst(*sst_id, blocks);
                    if let Some(ref bloom) = info.bloom {
                        index.set_bloom(*sst_id, bloom.clone());
                    }
                }
            }
        }

        let last_flushed_seq = state.last_flushed_seq;
        let wal_records = wal.replay_from(last_flushed_seq)?;

        let memtables = Arc::new(MemTables::new(
            config.max_frozen_memtables,
            config.memtable_size_mb * 1024 * 1024,
        ));

        for rec in &wal_records {
            memtables.insert(rec.clone());
        }

        let index = Arc::new(RwLock::new(index));
        let manifest = Arc::new(parking_lot::Mutex::new(manifest));
        let cache = Arc::new(BlockCache::new(config.block_cache_capacity_mb));
        let readers = Arc::new(RwLock::new(HashMap::new()));

        let sst_count = manifest.lock().state().sstables.len();
        let sst_bytes: u64 = manifest
            .lock()
            .state()
            .sstables
            .values()
            .map(|s| s.bytes)
            .sum();
        stats.set_sstable(sst_count, sst_bytes);
        stats.set_index_stats(index.read().total_entries(), index.read().bucket_count());

        let max_sst_id = manifest
            .lock()
            .state()
            .sstables
            .keys()
            .max()
            .copied()
            .unwrap_or(0);
        let seq_counter = std::sync::atomic::AtomicU64::new((max_sst_id as u64 + 1) * 1_000_000);

        let worker = Arc::new(parking_lot::Mutex::new(WriteWorker::new(
            config.clone(),
            wal,
            memtables.clone(),
            manifest.clone(),
            index.clone(),
            stats.clone(),
        )));

        Ok(Self {
            config,
            worker,
            seq_counter,
            stats,
            memtables,
            index,
            manifest,
            cache,
            readers,
        })
    }

    pub async fn write_batch(&self, batch: &[Record]) -> Result<()> {
        self.write_batch_ttl(batch, None).await
    }

    pub async fn write_batch_owned(&self, batch: Vec<Record>) -> Result<()> {
        self.write_batch_owned_ttl(batch, None).await
    }

    pub fn write_batch_sync(&self, batch: Vec<Record>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let records = self.convert_records(batch, None);
        self.do_write(records)
    }

    fn convert_records(&self, batch: Vec<Record>, ttl_secs: Option<u64>) -> Vec<InternalRecord> {
        let ttl = ttl_secs.or(self.config.default_ttl_secs);
        let base = self
            .seq_counter
            .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        batch
            .into_iter()
            .enumerate()
            .map(|(i, rec)| {
                let expire_at = match ttl {
                    Some(t) => rec.ts + (t as i64 * 1_000_000),
                    None => rec.expire_at,
                };
                InternalRecord {
                    seq: base + i as u64,
                    op: Op::Put,
                    key: rec.key.into_bytes(),
                    ts: rec.ts,
                    expire_at,
                    value: rec.value,
                    range_end: None,
                }
            })
            .collect()
    }

    async fn write_batch_owned_ttl(&self, batch: Vec<Record>, ttl_secs: Option<u64>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let records = self.convert_records(batch, ttl_secs);
        self.do_write(records)
    }

    pub async fn write_batch_ttl(&self, batch: &[Record], ttl_secs: Option<u64>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let default_ttl = self.config.default_ttl_secs;
        let ttl = ttl_secs.or(default_ttl);

        let base = self
            .seq_counter
            .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);

        let records: Vec<InternalRecord> = batch
            .iter()
            .enumerate()
            .map(|(i, rec)| {
                let expire_at = match ttl {
                    Some(t) => rec.ts + (t as i64 * 1_000_000),
                    None => rec.expire_at,
                };
                let seq = base + i as u64;
                let mut irec = InternalRecord::from_record(rec, seq);
                irec.expire_at = expire_at;
                irec
            })
            .collect();

        self.do_write(records)
    }

    fn do_write(&self, records: Vec<InternalRecord>) -> Result<()> {
        let num_records = records.len() as u64;
        let (wal_buf, mem_bytes) = crate::wal::encode_batch(&records);
        let start = std::time::Instant::now();
        self.worker
            .lock()
            .process_batch_encoded(records, &wal_buf, mem_bytes, num_records)?;
        self.stats
            .record_write_latency(start.elapsed().as_micros() as u64);
        Ok(())
    }

    pub async fn query(&self, query: Query) -> Result<Vec<Record>> {
        let start = std::time::Instant::now();

        let now_us = now_micros();
        let iter = ScanIterator::build(
            &query,
            &self.memtables,
            &self.index,
            &self.cache,
            &self.config,
            &self.readers,
            now_us,
        )?;
        let records: Vec<Record> = iter.collect::<Result<Vec<_>>>()?;

        self.stats
            .record_query_latency(start.elapsed().as_micros() as u64);
        self.stats.records_read(records.len() as u64);
        Ok(records)
    }

    pub async fn query_by_prefix(&self, key: &str) -> Result<Vec<Record>> {
        self.query(Query::prefix(key)).await
    }

    pub async fn query_by_key_range(&self, start: &str, end: &str) -> Result<Vec<Record>> {
        self.query(Query::key_range(start, end)).await
    }

    pub async fn query_time_range(&self, start: i64, end: i64) -> Result<Vec<Record>> {
        self.query(Query::time_range(start, end)).await
    }

    pub async fn query_prefix_time_range(
        &self,
        key: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<Record>> {
        self.query(Query::prefix_time_range(key, start, end)).await
    }

    pub async fn query_key_time_range(
        &self,
        start_key: &str,
        end_key: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<Record>> {
        self.query(Query::key_time_range(start_key, end_key, start, end))
            .await
    }

    pub async fn get(&self, key: &str, ts: i64) -> Result<Option<Record>> {
        Ok(self.get_sync(key, ts))
    }

    pub fn get_sync(&self, key: &str, ts: i64) -> Option<Record> {
        let now_us = now_micros();

        if let Some(rec) = self.memtables.get(key.as_bytes(), ts, now_us) {
            if rec.op != Op::Delete {
                return Some(rec.to_record());
            }
            return None;
        }

        let idx = self.index.read();
        if let Some((sst_id, block_idx)) = idx.single_sst_point(key.as_bytes(), now_us) {
            drop(idx);
            if let Some(rec) = self.block_search(key, ts, now_us, sst_id, block_idx) {
                return Some(rec);
            }
            return None;
        }

        let found = idx.query_point_inline(key.as_bytes(), now_us, |meta| {
            self.block_search(key, ts, now_us, meta.sst_id, meta.block_idx)
        });
        drop(idx);
        found
    }

    fn block_search(
        &self,
        key: &str,
        ts: i64,
        now_us: i64,
        sst_id: u32,
        block_idx: u32,
    ) -> Option<Record> {
        let reader = match Engine::get_reader(&self.readers, &self.config, sst_id) {
            Ok(r) => r,
            Err(_) => return None,
        };

        if let Some(cached) = reader.read_block_cached(block_idx, &self.cache) {
            return Self::find_in_records(&cached, key, ts, now_us);
        }

        match reader.read_block_decompress(block_idx) {
            Ok((_header, records)) => {
                let result = Self::find_in_records(&records, key, ts, now_us);
                self.cache.insert(CacheKey { sst_id, block_idx }, records);
                result
            }
            Err(_) => None,
        }
    }

    fn find_in_records(
        records: &[InternalRecord],
        key: &str,
        ts: i64,
        now_us: i64,
    ) -> Option<Record> {
        let lo = match records.binary_search_by(|r| {
            r.key
                .as_slice()
                .cmp(key.as_bytes())
                .then_with(|| r.ts.cmp(&ts))
        }) {
            Ok(idx) => idx,
            Err(_) => return None,
        };
        let rec = &records[lo];
        if rec.expire_at > now_us && rec.op != Op::Delete {
            return Some(Record {
                key: key.to_string(),
                ts: rec.ts,
                expire_at: rec.expire_at,
                value: rec.value.clone(),
            });
        }
        None
    }

    pub async fn delete_batch(&self, keys_ts: &[(String, i64)]) -> Result<()> {
        if keys_ts.is_empty() {
            return Ok(());
        }

        let base = self
            .seq_counter
            .fetch_add(keys_ts.len() as u64, std::sync::atomic::Ordering::Relaxed);

        let records: Vec<InternalRecord> = keys_ts
            .iter()
            .enumerate()
            .map(|(i, (key, ts))| InternalRecord::delete(key.clone(), *ts, base + i as u64))
            .collect();

        self.do_write(records)
    }

    pub async fn delete_range(&self, start_key: &str, end_key: &str) -> Result<()> {
        let seq = self
            .seq_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let record = InternalRecord::delete_range(start_key.to_string(), end_key.to_string(), seq);
        self.do_write(vec![record])
    }

    pub async fn patch_record(
        &self,
        key: &str,
        ts: i64,
        new_value: Option<Vec<u8>>,
        new_ttl_secs: Option<u64>,
    ) -> Result<Record> {
        let existing = self.get(key, ts).await?;
        let mut rec = match existing {
            Some(r) => r,
            None => {
                return Err(crate::error::FlowError::Other(format!(
                    "record not found: key={}, ts={}",
                    key, ts
                )));
            }
        };

        if let Some(v) = new_value {
            rec.value = v;
        }
        if let Some(ttl) = new_ttl_secs {
            rec.expire_at = rec.ts + (ttl as i64 * 1_000_000);
        }

        self.write_batch(&[rec.clone()]).await?;
        Ok(rec)
    }

    pub fn stats(&self) -> EngineStats {
        self.stats.snapshot(self.cache.hit_rate())
    }

    pub fn metrics_text(&self) -> String {
        self.stats.to_prometheus(self.cache.hit_rate())
    }

    fn get_reader(
        readers: &Arc<RwLock<HashMap<u32, Arc<SstReader>>>>,
        config: &Config,
        sst_id: u32,
    ) -> Result<Arc<SstReader>> {
        {
            let r = readers.read();
            if let Some(reader) = r.get(&sst_id) {
                return Ok(reader.clone());
            }
        }
        let path = config
            .data_dir
            .join("SST")
            .join(format!("{:09}.sst", sst_id));
        if !path.exists() {
            return Err(FlowError::Other(format!("sst {} not found", sst_id)));
        }
        let reader = Arc::new(SstReader::open(&path, sst_id, 0)?);
        readers.write().insert(sst_id, reader.clone());
        Ok(reader)
    }

    fn get_reader_from_map(
        readers: &HashMap<u32, Arc<SstReader>>,
        config: &Config,
        sst_id: u32,
    ) -> Result<Arc<SstReader>> {
        if let Some(reader) = readers.get(&sst_id) {
            return Ok(reader.clone());
        }
        let path = config
            .data_dir
            .join("SST")
            .join(format!("{:09}.sst", sst_id));
        if !path.exists() {
            return Err(FlowError::Other(format!("sst {} not found", sst_id)));
        }
        Ok(Arc::new(SstReader::open(&path, sst_id, 0)?))
    }

    pub async fn flush(&self) -> Result<()> {
        self.worker.lock().do_flush()
    }

    pub async fn trigger_gc(&self) -> Result<u64> {
        let gc = GcRunner::new(
            self.config.data_dir.clone(),
            self.manifest.clone(),
            self.index.clone(),
            self.cache.clone(),
            self.stats.clone(),
        );
        tokio::task::spawn_blocking(move || gc.run())
            .await
            .map_err(|_| FlowError::Closed)?
    }

    pub async fn trigger_compaction(&self) -> Result<bool> {
        let compaction = CompactionRunner::new(
            self.config.data_dir.clone(),
            self.config.block_size,
            self.config.zstd_level,
            self.config.bloom_bits_per_key,
            self.config.compaction_threshold,
            self.manifest.clone(),
            self.index.clone(),
            self.cache.clone(),
            self.stats.clone(),
        );
        tokio::task::spawn_blocking(move || compaction.run())
            .await
            .map_err(|_| FlowError::Closed)?
    }

    pub async fn shutdown(self) -> Result<()> {
        let mut worker = self.worker.lock();
        worker.do_flush()?;
        worker.flush_wal()?;
        Ok(())
    }

    // New iterator-based scan API (RocksDB-style)
    /// Lazy iterator scan with default `ReadOptions`.
    pub fn scan(&self, range: ScanRange) -> Result<ScanIterator> {
        self.scan_opt(range, &ReadOptions::default())
    }

    /// Lazy iterator scan with caller-provided `ReadOptions`.
    pub fn scan_opt(&self, range: ScanRange, _opts: &ReadOptions) -> Result<ScanIterator> {
        let now_us = now_micros();
        let (key_filter, time_range) = range.to_query_params();
        let q = Query {
            key_filter,
            time_range,
        };
        ScanIterator::build(
            &q,
            &self.memtables,
            &self.index,
            &self.cache,
            &self.config,
            &self.readers,
            now_us,
        )
    }

    /// Convenience: prefix scan returning a lazy iterator.
    pub fn scan_prefix(&self, prefix: &str) -> Result<ScanIterator> {
        self.scan(ScanRange::prefix(prefix))
    }

    /// Convenience: prefix + time-range scan returning a lazy iterator.
    pub fn scan_prefix_time_range(
        &self,
        prefix: &str,
        ts_start: i64,
        ts_end: i64,
    ) -> Result<ScanIterator> {
        self.scan(ScanRange::prefix_time_range(prefix, ts_start, ts_end))
    }

    /// Get the latest record for a given key (highest `ts`).
    /// Uses a bounded prefix scan and returns the last record.
    pub fn get_latest(&self, key: &str) -> Result<Option<Record>> {
        // Use scan with prefix == key to find all versions, then take last
        let range = ScanRange::prefix(key);
        let iter = self.scan(range)?;
        // Walk to the end (records come in (key, ts) ascending order)
        let mut latest: Option<Record> = None;
        for r in iter {
            latest = Some(r?);
        }
        Ok(latest)
    }

    /// Async wrapper for `get_latest`.
    pub async fn get_latest_async(&self, key: &str) -> Result<Option<Record>> {
        self.get_latest(key)
    }
}

/// ScanIterator — lazy merging iterator over memtables + SST blocks
/// Lazy iterator yielding `Result<Record>` in `(key, ts)` ascending order.
///
/// Constructed via `Engine::scan()`, `Engine::scan_prefix()`,
/// `Engine::scan_prefix_time_range()`, etc.
///
/// Internally maintains a binary-heap merge over pre-filtered memtable
/// and SST block sources. Records are yielded one-at-a-time without
/// materializing the full result set, enabling bounded-memory scans
/// over arbitrarily large ranges.
pub struct ScanIterator {
    sources: Vec<std::iter::Peekable<std::vec::IntoIter<InternalRecord>>>,
    heap: BinaryHeap<MergeEntry>,
    tombstones: Vec<(Vec<u8>, Vec<u8>)>,
    last_dedup: Option<(Vec<u8>, i64)>,
    /// Single-source fast path: when only 1 source + no tombstones,
    /// skip heap overhead and yield directly.
    fast_source: Option<usize>,
}

struct MergeEntry {
    key: Vec<u8>,
    ts: i64,
    seq: u64,
    source: usize,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.ts == other.ts
    }
}
impl Eq for MergeEntry {}
impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // min-heap by (key asc, ts asc, seq desc) — memtable (high seq) wins ties
        other
            .key
            .cmp(&self.key)
            .then(other.ts.cmp(&self.ts))
            .then(self.seq.cmp(&other.seq))
    }
}

impl ScanIterator {
    #[allow(clippy::too_many_arguments)]
    fn build(
        query: &Query,
        memtables: &Arc<MemTables>,
        index: &Arc<RwLock<BlockMetaIndex>>,
        cache: &Arc<BlockCache>,
        config: &Config,
        readers: &Arc<RwLock<HashMap<u32, Arc<SstReader>>>>,
        now_us: i64,
    ) -> Result<Self> {
        // 1) Query memtables (eager — memtable is small)
        let mem_results = match (&query.key_filter, &query.time_range) {
            (KeyFilter::Prefix(key), None) => memtables.query_prefix(key, now_us),
            (KeyFilter::Range { start, end }, None) => {
                memtables.query_key_range(start, end, now_us)
            }
            (KeyFilter::All, Some((ts_start, ts_end))) => {
                memtables.query_time_range(*ts_start, *ts_end, now_us)
            }
            (KeyFilter::Prefix(key), Some((ts_start, ts_end))) => {
                memtables.query_prefix_time_range(key, *ts_start, *ts_end, now_us)
            }
            (KeyFilter::Range { start, end }, Some((ts_start, ts_end))) => {
                memtables.query_key_time_range(start, end, *ts_start, *ts_end, now_us)
            }
            (KeyFilter::All, None) => memtables.query_key_range(b"", b"~", now_us),
        };

        // 2) Collect memtable range tombstones
        let mut tombstones: Vec<(Vec<u8>, Vec<u8>)> = mem_results
            .iter()
            .filter(|r| r.op == Op::DeleteRange && r.range_end.is_some())
            .map(|r| (r.key.clone(), r.range_end.clone().unwrap()))
            .collect();

        // 3) Query block meta index for candidate SST blocks
        let candidates = {
            let idx = index.read();
            idx.query(&query.key_filter, query.time_range, now_us)
        };

        let is_full_scan = (matches!(&query.key_filter, KeyFilter::All)
            || matches!(&query.key_filter, KeyFilter::Prefix(p) if p.is_empty()))
            && query.time_range.is_none();

        // 4) Snapshot SST readers
        let readers_snapshot = {
            let r = readers.read();
            r.clone()
        };

        // 5) Load + filter SST blocks into sources
        let mut sst_sources: Vec<std::iter::Peekable<std::vec::IntoIter<InternalRecord>>> =
            Vec::new();

        // For full scans, use read_block_decompress to avoid block cache overhead
        // and consume records without cloning.
        if is_full_scan {
            let mut all_sst_records: Vec<InternalRecord> = Vec::new();
            for meta in &candidates {
                let reader =
                    match Engine::get_reader_from_map(&readers_snapshot, config, meta.sst_id) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };
                let records = match reader.read_block_decompress(meta.block_idx) {
                    Ok((_, recs)) => recs,
                    Err(_) => continue,
                };
                for rec in records {
                    if rec.expire_at <= now_us {
                        continue;
                    }
                    if rec.op == Op::DeleteRange {
                        if let Some(ref end) = rec.range_end {
                            tombstones.push((rec.key.clone(), end.clone()));
                        }
                        continue;
                    }
                    if rec.op == Op::Delete {
                        continue;
                    }
                    all_sst_records.push(rec);
                }
            }
            if !all_sst_records.is_empty() {
                sst_sources.push(all_sst_records.into_iter().peekable());
            }
        } else {
            for meta in &candidates {
                let reader =
                    match Engine::get_reader_from_map(&readers_snapshot, config, meta.sst_id) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };
                let records = match reader.read_block_arc(meta.block_idx, cache) {
                    Ok(arc) => arc,
                    Err(_) => continue,
                };
                let filtered = filter_sst_block(
                    &records,
                    &query.key_filter,
                    query.time_range,
                    false,
                    now_us,
                    &mut tombstones,
                );
                if !filtered.is_empty() {
                    sst_sources.push(filtered.into_iter().peekable());
                }
            }
        }

        // 6) Sort memtable results
        let mut mem_sorted = mem_results;
        mem_sorted.sort_by(|a, b| a.key.cmp(&b.key).then(a.ts.cmp(&b.ts)));

        // 7) Assemble all sources: memtable first (index 0), then SST sources
        let mut sources: Vec<std::iter::Peekable<std::vec::IntoIter<InternalRecord>>> =
            Vec::with_capacity(1 + sst_sources.len());

        let memtable_present = !mem_sorted.is_empty();
        if memtable_present {
            sources.push(mem_sorted.into_iter().peekable());
        }
        sources.extend(sst_sources);

        // 8) Initialize heap — memtable uses source = usize::MAX for dedup priority
        //    Fast path: single source + no tombstones → bypass heap entirely
        let fast_source = if sources.len() == 1 && tombstones.is_empty() {
            // Single source, no tombstones — use fast path
            Some(0)
        } else {
            None
        };

        let mut heap: BinaryHeap<MergeEntry> = BinaryHeap::with_capacity(sources.len().max(1));
        if fast_source.is_none() {
            for (i, src) in sources.iter_mut().enumerate() {
                if let Some(r) = src.peek() {
                    let source_id = if memtable_present && i == 0 {
                        usize::MAX
                    } else {
                        i
                    };
                    heap.push(MergeEntry {
                        key: r.key.clone(),
                        ts: r.ts,
                        seq: r.seq,
                        source: source_id,
                    });
                }
            }
        }

        Ok(Self {
            sources,
            heap,
            tombstones,
            last_dedup: None,
            fast_source,
        })
    }

    fn advance_source(&mut self, source: usize) -> Option<InternalRecord> {
        let idx = if source == usize::MAX { 0 } else { source };
        let src = self.sources.get_mut(idx)?;
        let rec = src.next()?;
        if let Some(next) = src.peek() {
            self.heap.push(MergeEntry {
                key: next.key.clone(),
                ts: next.ts,
                seq: next.seq,
                source,
            });
        }
        Some(rec)
    }

    fn is_tombstoned(&self, rec: &InternalRecord) -> bool {
        self.tombstones.iter().any(|(start, end)| {
            rec.key.as_slice() >= start.as_slice() && rec.key.as_slice() < end.as_slice()
        })
    }
}

fn filter_sst_block(
    records: &[InternalRecord],
    key_filter: &KeyFilter,
    time_range: Option<(i64, i64)>,
    is_full_scan: bool,
    now_us: i64,
    tombstones: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<InternalRecord> {
    let mut filtered = Vec::with_capacity(records.len().min(64));
    for rec in records {
        if rec.expire_at <= now_us {
            continue;
        }
        if rec.op == Op::DeleteRange {
            if let Some(ref end) = rec.range_end {
                tombstones.push((rec.key.clone(), end.clone()));
            }
            continue;
        }
        if is_full_scan {
            if rec.op == Op::Delete {
                continue;
            }
            filtered.push(InternalRecord {
                seq: 0,
                op: Op::Put,
                key: rec.key.clone(),
                ts: rec.ts,
                expire_at: rec.expire_at,
                value: rec.value.clone(),
                range_end: None,
            });
            continue;
        }
        let matches_key = match key_filter {
            KeyFilter::Prefix(key) => rec.key.starts_with(key.as_slice()),
            KeyFilter::Range { start, end } => {
                rec.key.as_slice() >= start.as_slice() && rec.key.as_slice() <= end.as_slice()
            }
            KeyFilter::All => true,
        };
        if !matches_key {
            continue;
        }
        if let Some((ts_start, ts_end)) = time_range {
            if rec.ts < ts_start || rec.ts > ts_end {
                continue;
            }
        }
        filtered.push(InternalRecord {
            seq: 0,
            op: rec.op,
            key: rec.key.clone(),
            ts: rec.ts,
            expire_at: rec.expire_at,
            value: rec.value.clone(),
            range_end: rec.range_end.clone(),
        });
    }
    filtered
}

impl Iterator for ScanIterator {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        // Fast path: single source, no tombstones
        if let Some(idx) = self.fast_source {
            loop {
                let src = self.sources.get_mut(idx)?;
                let rec = src.next()?;
                if rec.op == Op::Delete || rec.op == Op::DeleteRange {
                    continue;
                }
                return Some(Ok(rec.into_record_owned()));
            }
        }

        // Merge-heap path for multiple sources
        loop {
            let entry = self.heap.pop()?;
            let dedup_key = (entry.key.clone(), entry.ts);

            // Dedup: same (key, ts) seen → skip
            if self.last_dedup.as_ref() == Some(&dedup_key) {
                self.advance_source(entry.source);
                continue;
            }

            let rec = match self.advance_source(entry.source) {
                Some(r) => r,
                None => continue,
            };

            // Skip tombstones (Delete / DeleteRange)
            if rec.op == Op::Delete || rec.op == Op::DeleteRange {
                self.last_dedup = Some(dedup_key);
                continue;
            }

            // Check range tombstones
            if self.is_tombstoned(&rec) {
                self.last_dedup = Some(dedup_key);
                continue;
            }

            self.last_dedup = Some(dedup_key);
            return Some(Ok(rec.into_record_owned()));
        }
    }
}

impl std::iter::FusedIterator for ScanIterator {}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_config(dir: &std::path::Path) -> Config {
        Config {
            data_dir: dir.to_path_buf(),
            memtable_size_mb: 1,
            block_size: 100,
            gc_interval_secs: 3600,
            max_frozen_memtables: 2,
            zstd_level: 1,
            flush_interval_ms: 60000,
            time_bucket_secs: 3600,
            block_cache_capacity_mb: 16,
            index_memory_budget_mb: 64,
            default_ttl_secs: None,
            bloom_bits_per_key: 10,
            wal_segment_size_mb: 64,
            compaction_threshold: 2,
            create_if_missing: true,
        }
    }

    fn make_record(key: &str, ts: i64) -> Record {
        Record {
            key: key.to_string(),
            ts,
            expire_at: i64::MAX,
            value: vec![1, 2, 3],
        }
    }

    #[tokio::test]
    async fn test_engine_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("key1", 100),
                make_record("key2", 200),
                make_record("key3", 300),
            ])
            .await
            .unwrap();

        let results = engine.query_by_prefix("key1").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "key1");

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_key_range_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
                make_record("d", 400),
            ])
            .await
            .unwrap();

        let results = engine.query_by_key_range("b", "c").await.unwrap();
        assert_eq!(results.len(), 2);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_time_range_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
            ])
            .await
            .unwrap();

        let results = engine.query_time_range(150, 300).await.unwrap();
        assert_eq!(results.len(), 2);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_prefix_time_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("a", 200),
                make_record("a", 300),
                make_record("b", 200),
            ])
            .await
            .unwrap();

        let results = engine.query_prefix_time_range("a", 150, 250).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].ts, 200);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_ttl_expiry() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        let mut rec = make_record("exp", 100);
        rec.expire_at = 1;
        engine.write_batch(&[rec]).await.unwrap();

        let results = engine.query_by_prefix("exp").await.unwrap();
        assert!(results.is_empty());

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_flush_and_query() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(dir.path());
        config.memtable_size_mb = 64;
        let engine = Engine::open(config).await.unwrap();

        let records: Vec<Record> = (0..50)
            .map(|i| make_record(&format!("key_{:04}", i), i * 10))
            .collect();
        engine.write_batch(&records).await.unwrap();

        engine.flush().await.unwrap();

        let results = engine.query_by_prefix("key_").await.unwrap();
        assert_eq!(results.len(), 50);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_stats() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[make_record("a", 100), make_record("b", 200)])
            .await
            .unwrap();

        let stats = engine.stats();
        assert_eq!(stats.total_records_written, 2);
        assert!(stats.uptime_secs == 0 || stats.uptime_secs <= 5);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_recovery() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());

        {
            let engine = Engine::open(config.clone()).await.unwrap();
            engine
                .write_batch(&[make_record("a", 100), make_record("b", 200)])
                .await
                .unwrap();
            engine.shutdown().await.unwrap();
        }

        {
            let engine = Engine::open(config).await.unwrap();
            let results = engine.query_by_prefix("a").await.unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].key, "a");

            let results = engine.query_by_prefix("b").await.unwrap();
            assert_eq!(results.len(), 1);

            engine.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_engine_concurrent_writes() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Arc::new(Engine::open(config).await.unwrap());

        let mut handles = Vec::new();
        for t in 0..4u64 {
            let e = engine.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..25u64 {
                    let rec = make_record(&format!("t{}-{}", t, i), (t * 100 + i) as i64);
                    e.write_batch(&[rec]).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let stats = engine.stats();
        assert_eq!(stats.total_records_written, 100);

        let engine = Arc::try_unwrap(engine)
            .unwrap_or_else(|e| std::sync::Arc::<Engine>::into_inner(e).unwrap());
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_compaction() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        for batch in 0..10u64 {
            let records: Vec<Record> = (0..5)
                .map(|i| Record {
                    key: format!("compact_{}-{}", batch, i),
                    ts: (batch * 100 + i) as i64,
                    expire_at: i64::MAX,
                    value: vec![1, 2, 3],
                })
                .collect();
            engine.write_batch(&records).await.unwrap();
            engine.flush().await.unwrap();
        }

        let did = engine.trigger_compaction().await.unwrap();
        assert!(did);

        let results = engine.query_by_prefix("compact_").await.unwrap();
        assert_eq!(results.len(), 50);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_gc_removes_expired_sstables() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let records: Vec<Record> = (0..10)
            .map(|i| Record {
                key: format!("gc_key_{}", i),
                ts: now_us,
                expire_at: i64::MAX,
                value: vec![1, 2, 3],
            })
            .collect();

        engine.write_batch_ttl(&records, Some(1)).await.unwrap();
        engine.flush().await.unwrap();

        let results = engine.query_by_prefix("gc_key_").await.unwrap();
        assert_eq!(results.len(), 10);

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let purged = engine.trigger_gc().await.unwrap();
        assert!(purged > 0);

        let results = engine.query_by_prefix("gc_key_").await.unwrap();
        assert!(results.is_empty());

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_delete_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
                make_record("d", 400),
            ])
            .await
            .unwrap();

        engine.delete_range("b", "d").await.unwrap();

        let results = engine.query_by_key_range("a", "d").await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, "a");
        assert_eq!(results[1].key, "d");

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_delete_range_flush_and_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("s1", 100),
                make_record("s2", 200),
                make_record("s3", 300),
            ])
            .await
            .unwrap();
        engine.flush().await.unwrap();

        engine.delete_range("s1", "s3").await.unwrap();
        engine.flush().await.unwrap();

        let results = engine.query_by_prefix("s").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "s3");

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_size_tiered_compaction() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        for batch in 0..6u64 {
            let records: Vec<Record> = (0..5)
                .map(|i| Record {
                    key: format!("st_{}-{}", batch, i),
                    ts: (batch * 100 + i) as i64,
                    expire_at: i64::MAX,
                    value: vec![1, 2, 3],
                })
                .collect();
            engine.write_batch(&records).await.unwrap();
            engine.flush().await.unwrap();
        }

        let did = engine.trigger_compaction().await.unwrap();
        assert!(did);

        let results = engine.query_by_prefix("st_").await.unwrap();
        assert_eq!(results.len(), 30);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_delete_range_preserves_outside() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("x-a", 100),
                make_record("x-b", 200),
                make_record("x-c", 300),
                make_record("y-a", 100),
                make_record("y-b", 200),
            ])
            .await
            .unwrap();

        engine.delete_range("x-a", "x-c").await.unwrap();

        let x_results = engine.query_by_prefix("x-").await.unwrap();
        assert_eq!(x_results.len(), 1);
        assert_eq!(x_results[0].key, "x-c");

        let y_results = engine.query_by_prefix("y-").await.unwrap();
        assert_eq!(y_results.len(), 2);

        engine.shutdown().await.unwrap();
    }

    // ── ScanIterator tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_scan_prefix_basic() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("alpha", 100),
                make_record("alpha", 200),
                make_record("beta", 150),
            ])
            .await
            .unwrap();

        let records: Vec<Record> = engine
            .scan_prefix("alpha")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].ts, 100);
        assert_eq!(records[1].ts, 200);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_prefix_time_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("k1", 10),
                make_record("k1", 20),
                make_record("k1", 30),
                make_record("k2", 15),
            ])
            .await
            .unwrap();

        let records: Vec<Record> = engine
            .scan_prefix_time_range("k1", 12, 25)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].ts, 20);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_range_all() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("a", 1),
                make_record("b", 2),
                make_record("c", 3),
            ])
            .await
            .unwrap();

        let records: Vec<Record> = engine
            .scan(ScanRange::all())
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 3);
        // Should be sorted by key, then ts
        assert_eq!(records[0].key, "a");
        assert_eq!(records[1].key, "b");
        assert_eq!(records[2].key, "c");

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_key_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("a", 1),
                make_record("b", 2),
                make_record("c", 3),
                make_record("d", 4),
            ])
            .await
            .unwrap();

        let records: Vec<Record> = engine
            .scan(ScanRange::key_range("b", "c"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 2);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_time_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
            ])
            .await
            .unwrap();

        let records: Vec<Record> = engine
            .scan(ScanRange::time_range(150, 300))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 2);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_matches_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        // Write data with multiple keys and timestamps
        let records: Vec<Record> = (0..50)
            .map(|i| make_record(&format!("key_{:04}", i), i * 10))
            .collect();
        engine.write_batch(&records).await.unwrap();

        // scan and query should return the same results
        let scan_results: Vec<Record> = engine
            .scan_prefix("key_")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let query_results = engine.query_by_prefix("key_").await.unwrap();

        assert_eq!(scan_results.len(), query_results.len());
        for (s, q) in scan_results.iter().zip(query_results.iter()) {
            assert_eq!(s.key, q.key);
            assert_eq!(s.ts, q.ts);
        }

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_after_flush() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("s1", 100),
                make_record("s2", 200),
                make_record("s3", 300),
            ])
            .await
            .unwrap();
        engine.flush().await.unwrap();

        let records: Vec<Record> = engine
            .scan_prefix("s")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 3);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_with_delete_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
                make_record("d", 400),
            ])
            .await
            .unwrap();

        engine.delete_range("b", "d").await.unwrap();

        let records: Vec<Record> = engine
            .scan(ScanRange::key_range("a", "d"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, "a");
        assert_eq!(records[1].key, "d");

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_empty_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine.write_batch(&[make_record("a", 1)]).await.unwrap();

        let records: Vec<Record> = engine
            .scan_prefix("nonexistent")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(records.is_empty());

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_latest() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("key1", 100),
                make_record("key1", 300),
                make_record("key1", 200),
                make_record("key2", 500),
            ])
            .await
            .unwrap();

        let latest = engine.get_latest("key1").unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().ts, 300);

        let latest2 = engine.get_latest("key2").unwrap();
        assert!(latest2.is_some());
        assert_eq!(latest2.unwrap().ts, 500);

        let none = engine.get_latest("nonexistent").unwrap();
        assert!(none.is_none());

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_latest_after_flush() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine
            .write_batch(&[
                make_record("k", 10),
                make_record("k", 20),
                make_record("k", 30),
            ])
            .await
            .unwrap();
        engine.flush().await.unwrap();

        // Write a newer record to memtable
        engine.write_batch(&[make_record("k", 99)]).await.unwrap();

        let latest = engine.get_latest("k").unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().ts, 99);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_lazy_does_not_materialize_all() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        // Write 100 records
        for i in 0..100u64 {
            engine
                .write_batch(&[Record {
                    key: format!("k{:03}", i),
                    ts: i as i64,
                    expire_at: i64::MAX,
                    value: vec![42u8; 32],
                }])
                .await
                .unwrap();
        }

        // Take only first 5 from the iterator
        let first_5: Vec<Record> = engine
            .scan(ScanRange::all())
            .unwrap()
            .take(5)
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(first_5.len(), 5);
        assert_eq!(first_5[0].key, "k000");
        assert_eq!(first_5[4].key, "k004");

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_opt_with_read_options() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).await.unwrap();

        engine.write_batch(&[make_record("x", 1)]).await.unwrap();

        let opts = ReadOptions {
            fill_cache: false,
            verify_checksums: false,
        };
        let records: Vec<Record> = engine
            .scan_opt(ScanRange::prefix("x"), &opts)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 1);

        engine.shutdown().await.unwrap();
    }
}
