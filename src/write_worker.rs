use crate::block_meta_index::BlockMetaIndex;
use crate::error::Result;
use crate::manifest::{Manifest, ManifestEntry, SstInfo};
use crate::memtable::MemTables;
use crate::record::{Config, InternalRecord};
use crate::sstable::SstWriter;
use crate::stats::StatsCounters;
use crate::wal::Wal;
use std::sync::Arc;

pub(crate) struct WriteWorker {
    config: Config,
    pub(crate) wal: Wal,
    pub(crate) memtables: Arc<MemTables>,
    manifest: Arc<parking_lot::Mutex<Manifest>>,
    index: Arc<parking_lot::RwLock<BlockMetaIndex>>,
    stats: Arc<StatsCounters>,
}

impl WriteWorker {
    pub fn new(
        config: Config,
        wal: Wal,
        memtables: Arc<MemTables>,
        manifest: Arc<parking_lot::Mutex<Manifest>>,
        index: Arc<parking_lot::RwLock<BlockMetaIndex>>,
        stats: Arc<StatsCounters>,
    ) -> Self {
        Self {
            config,
            wal,
            memtables,
            manifest,
            index,
            stats,
        }
    }

    pub fn flush_wal(&mut self) -> Result<()> {
        self.wal.flush()
    }

    pub fn process_batch_encoded(
        &mut self,
        records: Vec<InternalRecord>,
        wal_buf: &[u8],
        mem_bytes: u64,
        num_records: u64,
    ) -> Result<()> {
        let bytes_written = mem_bytes;

        self.wal.write_encoded(wal_buf)?;

        {
            let mut active = self.memtables.active_for_batch();
            for rec in records {
                active.insert(rec);
            }
        }

        self.stats.add_wal_bytes(bytes_written);
        let (rec_count, byte_count) = self.memtables.active_stats();
        self.stats.set_memtable(rec_count, byte_count);
        self.stats.set_frozen_count(self.memtables.frozen_count());
        self.stats.records_written(num_records, bytes_written);

        if self.memtables.should_flush() && !self.memtables.frozen_is_full() {
            self.do_flush()?;
        }

        Ok(())
    }

    pub fn do_flush(&mut self) -> Result<()> {
        let did_freeze = self.memtables.freeze();
        if !did_freeze {
            return Ok(());
        }

        let frozen = match self.memtables.pop_frozen() {
            Some(f) => f,
            None => return Ok(()),
        };

        let start = std::time::Instant::now();
        let mut all_records: Vec<InternalRecord> = frozen.iter_sorted().cloned().collect();
        let now_us = now_micros();
        all_records.retain(|r| r.expire_at > now_us);
        all_records.sort_by(|a, b| a.key.cmp(&b.key).then(a.ts.cmp(&b.ts)));

        if all_records.is_empty() {
            self.stats.set_frozen_count(self.memtables.frozen_count());
            return Ok(());
        }

        let sst_id;
        {
            let mf = self.manifest.lock();
            sst_id = mf.next_sst_id();
        }

        let sst_dir = self.config.data_dir.join("SST");
        std::fs::create_dir_all(&sst_dir)?;
        let sst_path = sst_dir.join(format!("{:09}.sst", sst_id));
        let tmp_path = sst_path.with_extension("sst.tmp");

        let (sst_bytes, block_infos, bloom) = SstWriter::write(
            &tmp_path,
            &all_records,
            self.config.block_size,
            self.config.zstd_level,
            self.config.bloom_bits_per_key,
            false,
        )?;

        std::fs::rename(&tmp_path, &sst_path)?;

        let min_ts = all_records.iter().map(|r| r.ts).min().unwrap_or(0);
        let max_ts = all_records.iter().map(|r| r.ts).max().unwrap_or(0);
        let min_expire = all_records.iter().map(|r| r.expire_at).min().unwrap_or(0);
        let max_expire = all_records.iter().map(|r| r.expire_at).max().unwrap_or(0);
        let last_seq = all_records.iter().map(|r| r.seq).max().unwrap_or(0);

        let raw_bytes: u64 = all_records.iter().map(|r| r.estimated_size() as u64).sum();
        if raw_bytes > 0 {
            self.stats
                .set_compression_ratio(sst_bytes as f64 / raw_bytes as f64);
        }

        let sst_info = SstInfo {
            id: sst_id,
            records: all_records.len() as u64,
            bytes: sst_bytes,
            min_ts,
            max_ts,
            min_expire,
            max_expire,
            bloom: Some(bloom.clone()),
        };

        {
            let mut mf = self.manifest.lock();
            mf.append(&ManifestEntry::Flush {
                seq: last_seq,
                sst: sst_info,
                blocks: block_infos.clone(),
            })?;
            mf.append(&ManifestEntry::Checkpoint {
                last_flushed_seq: last_seq,
            })?;
        }

        {
            let mut idx = self.index.write();
            idx.add_sst(sst_id, &block_infos);
            idx.set_bloom(sst_id, bloom);
        }

        {
            let _ = self.wal.truncate_before(last_seq);
        }

        let elapsed = start.elapsed();
        self.stats.flush_done();
        self.stats.record_flush_latency(elapsed.as_micros() as u64);

        let mf = self.manifest.lock();
        let sst_count = mf.state().sstables.len();
        let total_sst_bytes: u64 = mf.state().sstables.values().map(|s| s.bytes).sum();
        self.stats.set_sstable(sst_count, total_sst_bytes);

        self.stats.set_index_stats(
            self.index.read().total_entries(),
            self.index.read().bucket_count(),
        );

        let (rec_count, byte_count) = self.memtables.active_stats();
        self.stats.set_memtable(rec_count, byte_count);
        self.stats.set_frozen_count(self.memtables.frozen_count());

        Ok(())
    }
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}
