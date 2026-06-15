pub mod cache;
pub mod engine;
pub mod error;
pub mod jsondb;
pub mod record;
pub mod stats;

mod block_meta_index;
mod bloom;
mod compaction;
mod gc;
mod manifest;
mod memtable;
mod sstable;
mod wal;
mod write_worker;

pub use engine::{Engine, MaintenanceHandle, ScanIterator};
pub use error::{FlowError, Result};
pub use record::{Config, KeyFilter, Op, Query, ReadOptions, Record, ScanRange, SyncMode};
pub use stats::EngineStats;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_values() {
        let config = Config::default();
        assert_eq!(config.data_dir, std::path::PathBuf::from("./data"));
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
}
