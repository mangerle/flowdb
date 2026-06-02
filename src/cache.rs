use crate::record::InternalRecord;
use lru::LruCache;
use parking_lot::RwLock;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const NUM_SHARDS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CacheKey {
    pub sst_id: u32,
    pub block_idx: u32,
}

impl CacheKey {
    fn shard(&self) -> usize {
        let hash = self.sst_id.wrapping_mul(0x9e3779b9) ^ self.block_idx;
        (hash as usize) % NUM_SHARDS
    }
}

struct CacheShard {
    lru: RwLock<LruCache<CacheKey, Arc<Vec<InternalRecord>>>>,
}

pub(crate) struct BlockCache {
    shards: Vec<CacheShard>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity_mb: usize) -> Self {
        let per_shard = (capacity_mb * 1024 * 1024 / 256).max(1) / NUM_SHARDS;
        let cap = NonZeroUsize::new(per_shard.max(1)).unwrap();
        let shards = (0..NUM_SHARDS)
            .map(|_| CacheShard {
                lru: RwLock::new(LruCache::new(cap)),
            })
            .collect();
        Self {
            shards,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<Arc<Vec<InternalRecord>>> {
        let shard = &self.shards[key.shard()];
        let result = shard.lru.write().get(key).cloned();
        if result.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub fn insert(&self, key: CacheKey, records: Vec<InternalRecord>) {
        let shard = &self.shards[key.shard()];
        shard.lru.write().put(key, Arc::new(records));
    }

    pub fn invalidate_sst(&self, sst_id: u32) {
        for shard in &self.shards {
            let mut cache = shard.lru.write();
            let keys_to_remove: Vec<CacheKey> = cache
                .iter()
                .filter(|(k, _)| k.sst_id == sst_id)
                .map(|(k, _)| k.clone())
                .collect();
            for k in keys_to_remove {
                cache.pop(&k);
            }
        }
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let total = hits + self.misses.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;

    fn make_rec(key: &str) -> InternalRecord {
        InternalRecord::from_record(
            &Record {
                key: key.to_string(),
                ts: 0,
                expire_at: i64::MAX,
                value: vec![1],
            },
            0,
        )
    }

    #[test]
    fn test_cache_miss() {
        let cache = BlockCache::new(16);
        let key = CacheKey {
            sst_id: 1,
            block_idx: 0,
        };
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_cache_insert_get_hit() {
        let cache = BlockCache::new(16);
        let key = CacheKey {
            sst_id: 1,
            block_idx: 0,
        };
        cache.insert(key.clone(), vec![make_rec("a")]);
        let result = cache.get(&key).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"a".as_slice());
    }

    #[test]
    fn test_cache_invalidate_sst() {
        let cache = BlockCache::new(16);
        let key1 = CacheKey {
            sst_id: 1,
            block_idx: 0,
        };
        let key2 = CacheKey {
            sst_id: 2,
            block_idx: 0,
        };
        cache.insert(key1.clone(), vec![make_rec("a")]);
        cache.insert(key2.clone(), vec![make_rec("b")]);
        cache.invalidate_sst(1);
        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_some());
    }

    #[test]
    fn test_cache_overwrite() {
        let cache = BlockCache::new(16);
        let key = CacheKey {
            sst_id: 1,
            block_idx: 0,
        };
        cache.insert(key.clone(), vec![make_rec("a")]);
        cache.insert(key.clone(), vec![make_rec("b")]);
        let result = cache.get(&key).unwrap();
        assert_eq!(result[0].key, b"b".as_slice());
    }

    #[test]
    fn test_lru_eviction() {
        let cap = NonZeroUsize::new(3).unwrap();
        let mut lru: LruCache<CacheKey, Arc<Vec<InternalRecord>>> = LruCache::new(cap);
        let k1 = CacheKey {
            sst_id: 1,
            block_idx: 0,
        };
        let k2 = CacheKey {
            sst_id: 1,
            block_idx: 1,
        };
        let k3 = CacheKey {
            sst_id: 1,
            block_idx: 2,
        };
        let k4 = CacheKey {
            sst_id: 1,
            block_idx: 3,
        };
        lru.put(k1.clone(), Arc::new(vec![make_rec("a")]));
        lru.put(k2.clone(), Arc::new(vec![make_rec("b")]));
        lru.put(k3.clone(), Arc::new(vec![make_rec("c")]));
        lru.put(k4.clone(), Arc::new(vec![make_rec("d")]));
        assert!(lru.get(&k1).is_none(), "oldest should be evicted");
        assert!(lru.get(&k4).is_some(), "newest should survive");
        assert_eq!(lru.len(), 3);
    }

    #[test]
    fn test_lru_access_promotes() {
        let cap = NonZeroUsize::new(3).unwrap();
        let mut lru: LruCache<CacheKey, Arc<Vec<InternalRecord>>> = LruCache::new(cap);
        let k1 = CacheKey {
            sst_id: 1,
            block_idx: 0,
        };
        let k2 = CacheKey {
            sst_id: 1,
            block_idx: 1,
        };
        let k3 = CacheKey {
            sst_id: 1,
            block_idx: 2,
        };
        let k4 = CacheKey {
            sst_id: 1,
            block_idx: 3,
        };
        lru.put(k1.clone(), Arc::new(vec![make_rec("a")]));
        lru.put(k2.clone(), Arc::new(vec![make_rec("b")]));
        lru.put(k3.clone(), Arc::new(vec![make_rec("c")]));

        let _ = lru.get(&k1);

        lru.put(k4.clone(), Arc::new(vec![make_rec("d")]));
        assert!(lru.get(&k1).is_some(), "accessed entry should survive");
        assert!(lru.get(&k2).is_none(), "unaccessed LRU should be evicted");
    }
}
