use crate::bloom::BloomFilter;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SstInfo {
    pub id: u32,
    pub records: u64,
    pub bytes: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_expire: i64,
    pub max_expire: i64,
    #[serde(default)]
    pub bloom: Option<BloomFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BlockInfo {
    pub block_idx: u32,
    pub min_key: String,
    pub max_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_expire: i64,
    pub max_expire: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum ManifestEntry {
    #[serde(rename = "flush")]
    Flush {
        seq: u64,
        sst: SstInfo,
        blocks: Vec<BlockInfo>,
    },
    #[serde(rename = "delete_sst")]
    DeleteSst { sst_id: u32 },
    #[serde(rename = "compaction")]
    Compaction {
        removed: Vec<u32>,
        added: Vec<SstInfo>,
        blocks: Vec<(u32, Vec<BlockInfo>)>,
    },
    #[serde(rename = "checkpoint")]
    Checkpoint { last_flushed_seq: u64 },
    #[serde(rename = "gc_delete_sst")]
    GcDeleteSst { sst_id: u32 },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ManifestState {
    pub last_flushed_seq: u64,
    pub sstables: HashMap<u32, SstInfo>,
    pub block_infos: HashMap<u32, Vec<BlockInfo>>,
    pub active_sst_ids: Vec<u32>,
}

pub(crate) struct Manifest {
    path: PathBuf,
    state: ManifestState,
}

impl Manifest {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("MANIFEST");
        let mut state = ManifestState::default();

        if path.exists() {
            let content = std::fs::read_to_string(&path)?;
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Ok(entry) = serde_json::from_str::<ManifestEntry>(line) {
                    apply_entry(&mut state, &entry);
                }
            }
        }

        Ok(Self { path, state })
    }

    pub fn append(&mut self, entry: &ManifestEntry) -> Result<()> {
        let line = serde_json::to_string(entry)?;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;
        use std::io::Write;
        writeln!(file, "{}", line)?;
        file.flush()?;

        apply_entry(&mut self.state, entry);
        Ok(())
    }

    pub fn state(&self) -> &ManifestState {
        &self.state
    }

    pub fn next_sst_id(&self) -> u32 {
        self.state.sstables.keys().max().copied().unwrap_or(0) + 1
    }
}

fn apply_entry(state: &mut ManifestState, entry: &ManifestEntry) {
    match entry {
        ManifestEntry::Flush { seq, sst, blocks } => {
            state.last_flushed_seq = state.last_flushed_seq.max(*seq);
            state.sstables.insert(sst.id, sst.clone());
            state.block_infos.insert(sst.id, blocks.clone());
            if !state.active_sst_ids.contains(&sst.id) {
                state.active_sst_ids.push(sst.id);
            }
        }
        ManifestEntry::DeleteSst { sst_id } | ManifestEntry::GcDeleteSst { sst_id } => {
            state.sstables.remove(sst_id);
            state.block_infos.remove(sst_id);
            state.active_sst_ids.retain(|id| id != sst_id);
        }
        ManifestEntry::Compaction {
            removed,
            added,
            blocks,
        } => {
            for id in removed {
                state.sstables.remove(id);
                state.block_infos.remove(id);
                state.active_sst_ids.retain(|sid| sid != id);
            }
            for info in added {
                state.sstables.insert(info.id, info.clone());
                if !state.active_sst_ids.contains(&info.id) {
                    state.active_sst_ids.push(info.id);
                }
            }
            for (sst_id, blks) in blocks {
                state.block_infos.insert(*sst_id, blks.clone());
            }
        }
        ManifestEntry::Checkpoint { last_flushed_seq } => {
            state.last_flushed_seq = state.last_flushed_seq.max(*last_flushed_seq);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_append_and_replay() {
        let dir = TempDir::new().unwrap();
        let sst_dir = dir.path().join("data");
        std::fs::create_dir_all(&sst_dir).unwrap();

        {
            let mut mf = Manifest::open(&sst_dir).unwrap();
            mf.append(&ManifestEntry::Flush {
                seq: 100,
                sst: SstInfo {
                    id: 1,
                    records: 100,
                    bytes: 4096,
                    min_ts: 1000,
                    max_ts: 2000,
                    min_expire: i64::MAX,
                    max_expire: i64::MAX,
                    bloom: None,
                },
                blocks: vec![BlockInfo {
                    block_idx: 0,
                    min_key: "a".into(),
                    max_key: "b".into(),
                    min_ts: 1000,
                    max_ts: 2000,
                    min_expire: i64::MAX,
                    max_expire: i64::MAX,
                }],
            })
            .unwrap();

            mf.append(&ManifestEntry::Flush {
                seq: 200,
                sst: SstInfo {
                    id: 2,
                    records: 50,
                    bytes: 2048,
                    min_ts: 2000,
                    max_ts: 3000,
                    min_expire: i64::MAX,
                    max_expire: i64::MAX,
                    bloom: None,
                },
                blocks: vec![],
            })
            .unwrap();

            assert_eq!(mf.state().sstables.len(), 2);
            assert_eq!(mf.state().last_flushed_seq, 200);
        }

        let mf2 = Manifest::open(&sst_dir).unwrap();
        assert_eq!(mf2.state().sstables.len(), 2);
        assert_eq!(mf2.state().last_flushed_seq, 200);
    }

    #[test]
    fn test_manifest_delete() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let mut mf = Manifest::open(&data_dir).unwrap();
        mf.append(&ManifestEntry::Flush {
            seq: 100,
            sst: SstInfo {
                id: 1,
                records: 10,
                bytes: 100,
                min_ts: 0,
                max_ts: 0,
                min_expire: 0,
                max_expire: 0,
                bloom: None,
            },
            blocks: vec![],
        })
        .unwrap();
        mf.append(&ManifestEntry::DeleteSst { sst_id: 1 }).unwrap();

        assert!(!mf.state().sstables.contains_key(&1));
        assert!(mf.state().active_sst_ids.is_empty());
    }
}
