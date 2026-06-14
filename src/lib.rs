pub mod cache;
pub mod engine;
pub mod error;
pub mod record;
pub mod stats;

#[cfg(feature = "server")]
pub mod admin;
#[cfg(feature = "server")]
pub mod auth;
#[cfg(feature = "server")]
pub mod http;
#[cfg(feature = "server")]
pub mod udp;

mod block_meta_index;
mod bloom;
mod compaction;
mod gc;
mod manifest;
mod memtable;
mod sstable;
mod wal;
mod write_worker;

pub use engine::{Engine, ScanIterator};
pub use error::{FlowError, Result};
pub use record::{Config, KeyFilter, Op, Query, ReadOptions, Record, ScanRange, SyncMode};
pub use stats::EngineStats;

#[cfg(feature = "server")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "server")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub http_addr: String,
    pub udp_addr: String,
    pub api_keys: Vec<String>,
    pub udp_api_key: Option<String>,
    pub max_udp_packet_size: usize,
    pub engine: Config,
}

#[cfg(feature = "server")]
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: "0.0.0.0:8080".into(),
            udp_addr: "0.0.0.0:9090".into(),
            api_keys: Vec::new(),
            udp_api_key: None,
            max_udp_packet_size: 1400,
            engine: Config::default(),
        }
    }
}

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

    #[cfg(feature = "server")]
    #[test]
    fn test_server_config_serde_json_roundtrip() {
        let config = ServerConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: ServerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.http_addr, config.http_addr);
        assert_eq!(parsed.udp_addr, config.udp_addr);
        assert_eq!(parsed.api_keys, config.api_keys);
        assert_eq!(parsed.udp_api_key, config.udp_api_key);
        assert_eq!(parsed.max_udp_packet_size, config.max_udp_packet_size);
    }

    #[cfg(feature = "server")]
    #[test]
    fn test_server_config_toml_roundtrip() {
        let config = ServerConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        let parsed: ServerConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.http_addr, config.http_addr);
        assert_eq!(parsed.udp_addr, config.udp_addr);
        assert_eq!(parsed.max_udp_packet_size, config.max_udp_packet_size);
    }

    #[cfg(feature = "server")]
    #[test]
    fn test_server_config_custom_values() {
        let config = ServerConfig {
            http_addr: "127.0.0.1:3000".into(),
            udp_addr: "127.0.0.1:4000".into(),
            api_keys: vec!["key1".into(), "key2".into()],
            udp_api_key: Some("udp_key".into()),
            max_udp_packet_size: 2000,
            engine: Config::default(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: ServerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.http_addr, "127.0.0.1:3000");
        assert_eq!(parsed.api_keys, vec!["key1", "key2"]);
        assert_eq!(parsed.udp_api_key, Some("udp_key".to_string()));
        assert_eq!(parsed.max_udp_packet_size, 2000);
    }
}
