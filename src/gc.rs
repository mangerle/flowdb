use crate::block_meta_index::BlockMetaIndex;
use crate::cache::BlockCache;
use crate::error::Result;
use crate::manifest::{Manifest, ManifestEntry};
use crate::stats::StatsCounters;
use std::sync::Arc;

pub(crate) struct GcRunner {
    data_dir: std::path::PathBuf,
    manifest: Arc<parking_lot::Mutex<Manifest>>,
    index: Arc<parking_lot::RwLock<BlockMetaIndex>>,
    cache: Arc<BlockCache>,
    stats: Arc<StatsCounters>,
}

impl GcRunner {
    pub fn new(
        data_dir: std::path::PathBuf,
        manifest: Arc<parking_lot::Mutex<Manifest>>,
        index: Arc<parking_lot::RwLock<BlockMetaIndex>>,
        cache: Arc<BlockCache>,
        stats: Arc<StatsCounters>,
    ) -> Self {
        Self {
            data_dir,
            manifest,
            index,
            cache,
            stats,
        }
    }

    pub fn run(&self) -> Result<u64> {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;

        let mut purged: u64 = 0;
        let mut to_delete = Vec::new();

        {
            let mf = self.manifest.lock();
            let state = mf.state();
            for (sst_id, info) in &state.sstables {
                if info.max_expire < now_us {
                    to_delete.push(*sst_id);
                    purged += info.records;
                }
            }
        }

        for sst_id in to_delete {
            self.cache.invalidate_sst(sst_id);
            {
                let mut idx = self.index.write();
                idx.remove_sst(sst_id);
            }
            {
                let mut mf = self.manifest.lock();
                mf.append(&ManifestEntry::GcDeleteSst { sst_id })?;
            }
            let sst_path = self.data_dir.join("SST").join(format!("{:09}.sst", sst_id));
            if let Err(e) = std::fs::remove_file(&sst_path) {
                tracing::warn!("GC: failed to delete SST file {:?}: {}", sst_path, e);
            }
        }

        self.stats.gc_done(purged);
        self.refresh_stats();
        Ok(purged)
    }

    fn refresh_stats(&self) {
        let mf = self.manifest.lock();
        let sst_count = mf.state().sstables.len();
        let total_bytes: u64 = mf.state().sstables.values().map(|s| s.bytes).sum();
        self.stats.set_sstable(sst_count, total_bytes);
        drop(mf);

        let idx = self.index.read();
        self.stats
            .set_index_stats(idx.total_entries(), idx.bucket_count());
    }
}
