use crate::bloom::BloomFilter;
use crate::manifest::BlockInfo;
use crate::record::KeyFilter;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub(crate) struct BlockMeta {
    pub sst_id: u32,
    pub block_idx: u32,
    pub min_key: String,
    pub max_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub max_expire: i64,
}

impl BlockMeta {
    pub fn from_block_info(sst_id: u32, bi: &BlockInfo) -> Self {
        Self {
            sst_id,
            block_idx: bi.block_idx,
            min_key: bi.min_key.clone(),
            max_key: bi.max_key.clone(),
            min_ts: bi.min_ts,
            max_ts: bi.max_ts,
            max_expire: bi.max_expire,
        }
    }

    fn overlaps_key_prefix(&self, prefix: &[u8]) -> bool {
        let prefix_end = increment_prefix(prefix);
        self.min_key.as_bytes() < prefix_end.as_slice() && self.max_key.as_bytes() >= prefix
    }

    fn overlaps_key_range(&self, start: &[u8], end: &[u8]) -> bool {
        self.min_key.as_bytes() <= end && self.max_key.as_bytes() >= start
    }

    fn overlaps_time(&self, ts_start: i64, ts_end: i64) -> bool {
        self.min_ts <= ts_end && self.max_ts >= ts_start
    }

    fn is_expired(&self, now_us: i64) -> bool {
        self.max_expire < now_us
    }
}

pub(crate) struct BlockMetaIndex {
    by_key: BTreeMap<Vec<u8>, Vec<BlockMeta>>,
    by_time: BTreeMap<i64, Vec<BlockMeta>>,
    time_bucket_us: i64,
    blooms: HashMap<u32, BloomFilter>,
    sst_blocks: BTreeMap<u32, Vec<BlockMeta>>,
}

impl BlockMetaIndex {
    pub fn new(time_bucket_secs: u64) -> Self {
        Self {
            by_key: BTreeMap::new(),
            by_time: BTreeMap::new(),
            time_bucket_us: time_bucket_secs as i64 * 1_000_000,
            blooms: HashMap::new(),
            sst_blocks: BTreeMap::new(),
        }
    }

    pub fn add_sst(&mut self, sst_id: u32, blocks: &[BlockInfo]) {
        for bi in blocks {
            let meta = BlockMeta::from_block_info(sst_id, bi);
            let key = meta.min_key.as_bytes().to_vec();
            self.by_key.entry(key).or_default().push(meta.clone());

            let bucket = meta.min_ts / self.time_bucket_us;
            self.by_time.entry(bucket).or_default().push(meta);
        }

        let sorted: Vec<BlockMeta> = blocks
            .iter()
            .map(|bi| BlockMeta::from_block_info(sst_id, bi))
            .collect();
        self.sst_blocks.insert(sst_id, sorted);
    }

    pub fn set_bloom(&mut self, sst_id: u32, bloom: BloomFilter) {
        self.blooms.insert(sst_id, bloom);
    }

    pub fn remove_sst(&mut self, sst_id: u32) {
        for (_, metas) in self.by_key.iter_mut() {
            metas.retain(|m| m.sst_id != sst_id);
        }
        self.by_key.retain(|_, v| !v.is_empty());

        for (_, metas) in self.by_time.iter_mut() {
            metas.retain(|m| m.sst_id != sst_id);
        }
        self.by_time.retain(|_, v| !v.is_empty());

        self.blooms.remove(&sst_id);
        self.sst_blocks.remove(&sst_id);
    }

    pub fn query_point_inline<F>(
        &self,
        key: &[u8],
        now_us: i64,
        mut f: F,
    ) -> Option<crate::record::Record>
    where
        F: FnMut(&BlockMeta) -> Option<crate::record::Record>,
    {
        for (sst_id, blocks) in &self.sst_blocks {
            if !self.bloom_may_contain(*sst_id, key) {
                continue;
            }
            if let Some(meta) = Self::binary_search_block(blocks, key) {
                if meta.is_expired(now_us) {
                    continue;
                }
                if let Some(rec) = f(meta) {
                    return Some(rec);
                }
            }
        }
        None
    }

    pub fn single_sst_point(&self, key: &[u8], now_us: i64) -> Option<(u32, u32)> {
        if self.sst_blocks.len() != 1 {
            return None;
        }
        let (_, blocks) = self.sst_blocks.first_key_value()?;
        if let Some(meta) = Self::binary_search_block(blocks, key) {
            if !meta.is_expired(now_us) {
                return Some((meta.sst_id, meta.block_idx));
            }
        }
        None
    }

    fn binary_search_block<'a>(blocks: &'a [BlockMeta], key: &[u8]) -> Option<&'a BlockMeta> {
        let mut lo = 0usize;
        let mut hi = blocks.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let b = &blocks[mid];
            if b.max_key.as_bytes() < key {
                lo = mid + 1;
            } else if b.min_key.as_bytes() > key {
                hi = mid;
            } else {
                return Some(b);
            }
        }
        if lo < blocks.len()
            && blocks[lo].min_key.as_bytes() <= key
            && blocks[lo].max_key.as_bytes() >= key
        {
            return Some(&blocks[lo]);
        }
        None
    }

    pub fn query(
        &self,
        key_filter: &KeyFilter,
        time_range: Option<(i64, i64)>,
        now_us: i64,
    ) -> Vec<BlockMeta> {
        let key_candidates = self.collect_by_key(key_filter, now_us);
        match time_range {
            Some((ts_start, ts_end)) => {
                let time_set = self.collect_time_set(ts_start, ts_end, now_us);
                key_candidates
                    .into_iter()
                    .filter(|m| time_set.contains(&(m.sst_id, m.block_idx)))
                    .collect()
            }
            None => key_candidates,
        }
    }

    fn collect_by_key(&self, key_filter: &KeyFilter, now_us: i64) -> Vec<BlockMeta> {
        match key_filter {
            KeyFilter::Prefix(key) => {
                let prefix_end = increment_prefix(key.as_slice());
                self.by_key
                    .range(..prefix_end)
                    .flat_map(|(_, metas)| metas.iter())
                    .filter(|m| !m.is_expired(now_us) && m.overlaps_key_prefix(key.as_slice()))
                    .cloned()
                    .collect()
            }
            KeyFilter::Range { start, end } => {
                let end_key = increment_prefix(end.as_slice());
                self.by_key
                    .range(..end_key)
                    .flat_map(|(_, metas)| metas.iter())
                    .filter(|m| {
                        !m.is_expired(now_us)
                            && m.overlaps_key_range(start.as_slice(), end.as_slice())
                    })
                    .cloned()
                    .collect()
            }
            KeyFilter::All => self
                .by_key
                .values()
                .flat_map(|metas| metas.iter())
                .filter(|m| !m.is_expired(now_us))
                .cloned()
                .collect(),
        }
    }

    fn collect_time_set(
        &self,
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> std::collections::HashSet<(u32, u32)> {
        let bucket_start = ts_start / self.time_bucket_us;
        let bucket_end = ts_end / self.time_bucket_us;
        self.by_time
            .range(bucket_start..=bucket_end)
            .flat_map(|(_, metas)| metas.iter())
            .filter(|m| !m.is_expired(now_us) && m.overlaps_time(ts_start, ts_end))
            .map(|m| (m.sst_id, m.block_idx))
            .collect()
    }

    pub fn total_entries(&self) -> usize {
        self.by_key.values().map(|v| v.len()).sum()
    }

    pub fn bucket_count(&self) -> usize {
        self.by_time.len()
    }

    fn bloom_may_contain(&self, sst_id: u32, key: &[u8]) -> bool {
        match self.blooms.get(&sst_id) {
            Some(filter) => filter.may_contain(key),
            None => true,
        }
    }
}

fn increment_prefix(s: &[u8]) -> Vec<u8> {
    let mut bytes = s.to_vec();
    while let Some(last) = bytes.last_mut() {
        if *last < 255 {
            *last += 1;
            return bytes;
        }
        bytes.pop();
    }
    vec![0xEF, 0xBF, 0xBF]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block_info(
        idx: u32,
        min_key: &str,
        max_key: &str,
        min_ts: i64,
        max_ts: i64,
    ) -> BlockInfo {
        BlockInfo {
            block_idx: idx,
            min_key: min_key.to_string(),
            max_key: max_key.to_string(),
            min_ts,
            max_ts,
            min_expire: i64::MAX,
            max_expire: i64::MAX,
        }
    }

    #[test]
    fn test_index_prefix_query() {
        let mut idx = BlockMetaIndex::new(3600);
        idx.add_sst(
            1,
            &[
                make_block_info(0, "call-a", "call-c", 1000, 2000),
                make_block_info(1, "call-d", "call-f", 3000, 4000),
            ],
        );

        let result = idx.query(&KeyFilter::Prefix(b"call-a".to_vec()), None, 0);
        assert!(!result.is_empty());
        assert_eq!(result[0].sst_id, 1);
        assert_eq!(result[0].block_idx, 0);
    }

    #[test]
    fn test_index_key_range_query() {
        let mut idx = BlockMetaIndex::new(3600);
        idx.add_sst(
            1,
            &[
                make_block_info(0, "a", "c", 1000, 2000),
                make_block_info(1, "d", "f", 3000, 4000),
            ],
        );

        let result = idx.query(
            &KeyFilter::Range {
                start: b"b".to_vec(),
                end: b"e".to_vec(),
            },
            None,
            0,
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_index_time_range_query() {
        let mut idx = BlockMetaIndex::new(3600);
        let bucket_us: i64 = 3_600_000_000;
        idx.add_sst(
            1,
            &[
                make_block_info(0, "a", "b", bucket_us, bucket_us * 2),
                make_block_info(1, "c", "d", bucket_us * 3, bucket_us * 4),
            ],
        );

        let result = idx.query(&KeyFilter::All, Some((bucket_us, bucket_us * 3)), 0);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_index_remove_sst() {
        let mut idx = BlockMetaIndex::new(3600);
        idx.add_sst(1, &[make_block_info(0, "a", "b", 1000, 2000)]);
        idx.add_sst(2, &[make_block_info(0, "c", "d", 3000, 4000)]);

        idx.remove_sst(1);

        let result = idx.query(&KeyFilter::Prefix(b"a".to_vec()), None, 0);
        assert!(result.is_empty());

        let result = idx.query(&KeyFilter::Prefix(b"c".to_vec()), None, 0);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_index_expiry_filter() {
        let mut idx = BlockMetaIndex::new(3600);
        let mut bi = make_block_info(0, "a", "b", 1000, 2000);
        bi.min_expire = 100;
        bi.max_expire = 200;
        idx.add_sst(1, &[bi]);

        let result = idx.query(&KeyFilter::Prefix(b"a".to_vec()), None, 300);
        assert!(result.is_empty());
    }

    #[test]
    fn test_index_combined_query() {
        let mut idx = BlockMetaIndex::new(3600);
        idx.add_sst(
            1,
            &[
                make_block_info(0, "call-a", "call-b", 1000, 2000),
                make_block_info(1, "call-a", "call-b", 3000, 4000),
            ],
        );

        let result = idx.query(
            &KeyFilter::Prefix(b"call-a".to_vec()),
            Some((1500, 3500)),
            0,
        );
        assert_eq!(result.len(), 2);
    }
}
