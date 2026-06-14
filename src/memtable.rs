use crate::record::InternalRecord;
use ahash::AHashMap;
use parking_lot::RwLock;
use std::collections::BTreeMap;

fn hash_key(key: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = ahash::AHasher::default();
    key.hash(&mut hasher);
    hasher.finish()
}

enum MemTableData {
    Log {
        records: Vec<InternalRecord>,
        index: AHashMap<(u64, i64), usize>,
    },
    Map(BTreeMap<(Vec<u8>, i64, u64), InternalRecord>),
}

pub(crate) struct MemTable {
    data: MemTableData,
    bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            data: MemTableData::Log {
                records: Vec::new(),
                index: AHashMap::new(),
            },
            bytes: 0,
        }
    }

    pub fn insert(&mut self, rec: InternalRecord) {
        let size = rec.estimated_size();
        self.bytes += size;
        match &mut self.data {
            MemTableData::Log { records, index } => {
                let pos = records.len();
                let h = hash_key(&rec.key);
                index.insert((h, rec.ts), pos);
                records.push(rec);
            }
            MemTableData::Map(map) => {
                let key = (rec.key.clone(), rec.ts, rec.seq);
                if let Some(old) = map.insert(key, rec) {
                    self.bytes = self.bytes.saturating_sub(old.estimated_size());
                }
            }
        }
    }

    pub fn get(&self, key: &[u8], ts: i64) -> Option<&InternalRecord> {
        match &self.data {
            MemTableData::Log { records, index } => {
                let h = hash_key(key);
                if let Some(&pos) = index.get(&(h, ts)) {
                    if let Some(rec) = records.get(pos) {
                        if rec.key == key && rec.ts == ts {
                            return Some(rec);
                        }
                    }
                }
                records.iter().find(|r| r.key == key && r.ts == ts)
            }
            MemTableData::Map(map) => map
                .range((key.to_vec(), ts, u64::MIN)..=(key.to_vec(), ts, u64::MAX))
                .next()
                .map(|(_, v)| v),
        }
    }

    pub fn query_prefix(&self, key: &[u8], now_us: i64) -> Vec<&InternalRecord> {
        match &self.data {
            MemTableData::Log { records, .. } => records
                .iter()
                .filter(|r| r.key.starts_with(key) && r.expire_at >= now_us)
                .collect(),
            MemTableData::Map(map) => {
                let start = (key.to_vec(), i64::MIN, u64::MIN);
                let prefix_end = increment_prefix(key);
                let end = (prefix_end, i64::MIN, u64::MIN);
                map.range(start..end)
                    .filter(|(_, v)| v.expire_at >= now_us)
                    .map(|(_, v)| v)
                    .collect()
            }
        }
    }

    pub fn query_key_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        now_us: i64,
    ) -> Vec<&InternalRecord> {
        match &self.data {
            MemTableData::Log { records, .. } => records
                .iter()
                .filter(|r| {
                    r.key.as_slice() >= start_key
                        && r.key.as_slice() <= end_key
                        && r.expire_at >= now_us
                })
                .collect(),
            MemTableData::Map(map) => {
                let start = (start_key.to_vec(), i64::MIN, u64::MIN);
                let end = (end_key.to_vec(), i64::MAX, u64::MAX);
                map.range(start..=end)
                    .filter(|(_, v)| v.expire_at >= now_us)
                    .map(|(_, v)| v)
                    .collect()
            }
        }
    }

    pub fn query_time_range(
        &self,
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<&InternalRecord> {
        match &self.data {
            MemTableData::Log { records, .. } => records
                .iter()
                .filter(|r| r.ts >= ts_start && r.ts <= ts_end && r.expire_at >= now_us)
                .collect(),
            MemTableData::Map(map) => map
                .iter()
                .filter(|((_, ts, _), v)| *ts >= ts_start && *ts <= ts_end && v.expire_at >= now_us)
                .map(|(_, v)| v)
                .collect(),
        }
    }

    pub fn query_prefix_time_range(
        &self,
        key: &[u8],
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<&InternalRecord> {
        match &self.data {
            MemTableData::Log { records, .. } => records
                .iter()
                .filter(|r| {
                    r.key.starts_with(key)
                        && r.ts >= ts_start
                        && r.ts <= ts_end
                        && r.expire_at >= now_us
                })
                .collect(),
            MemTableData::Map(map) => {
                let start = (key.to_vec(), ts_start, u64::MIN);
                let prefix_end = increment_prefix(key);
                let end = (prefix_end, i64::MIN, u64::MIN);
                map.range(start..end)
                    .filter(|((k, ts, _), v)| {
                        k.starts_with(key)
                            && *ts >= ts_start
                            && *ts <= ts_end
                            && v.expire_at >= now_us
                    })
                    .map(|(_, v)| v)
                    .collect()
            }
        }
    }

    pub fn query_key_time_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<&InternalRecord> {
        match &self.data {
            MemTableData::Log { records, .. } => records
                .iter()
                .filter(|r| {
                    r.key.as_slice() >= start_key
                        && r.key.as_slice() <= end_key
                        && r.ts >= ts_start
                        && r.ts <= ts_end
                        && r.expire_at >= now_us
                })
                .collect(),
            MemTableData::Map(map) => map
                .iter()
                .filter(|((k, ts, _), v)| {
                    k.as_slice() >= start_key
                        && k.as_slice() <= end_key
                        && *ts >= ts_start
                        && *ts <= ts_end
                        && v.expire_at >= now_us
                })
                .map(|(_, v)| v)
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.data {
            MemTableData::Log { records, .. } => records.is_empty(),
            MemTableData::Map(map) => map.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match &self.data {
            MemTableData::Log { records, .. } => records.len(),
            MemTableData::Map(map) => map.len(),
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn iter_sorted(&self) -> impl Iterator<Item = &InternalRecord> {
        match &self.data {
            MemTableData::Log { .. } => {
                unreachable!("iter_sorted called on log memtable - should be frozen first")
            }
            MemTableData::Map(map) => map.values(),
        }
    }

    fn convert_to_map(&mut self) {
        if let MemTableData::Log { records, index } = &mut self.data {
            let log = std::mem::take(records);
            std::mem::take(index);
            let mut map = BTreeMap::new();
            for rec in log {
                let key = (rec.key.clone(), rec.ts, rec.seq);
                map.insert(key, rec);
            }
            self.data = MemTableData::Map(map);
        }
    }
}

fn increment_prefix(key: &[u8]) -> Vec<u8> {
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

pub(crate) struct MemTables {
    active: RwLock<MemTable>,
    frozen: RwLock<Vec<MemTable>>,
    #[allow(dead_code)]
    max_frozen: usize,
    memtable_size_limit: usize,
}

impl MemTables {
    pub fn new(max_frozen: usize, memtable_size_limit: usize) -> Self {
        Self {
            active: RwLock::new(MemTable::new()),
            frozen: RwLock::new(Vec::new()),
            max_frozen,
            memtable_size_limit,
        }
    }

    pub fn insert(&self, rec: InternalRecord) {
        let mut active = self.active.write();
        active.insert(rec);
    }

    pub fn active_for_batch(&self) -> parking_lot::RwLockWriteGuard<'_, MemTable> {
        self.active.write()
    }

    pub fn should_flush(&self) -> bool {
        let active = self.active.read();
        active.bytes() >= self.memtable_size_limit
    }

    #[allow(dead_code)]
    pub fn frozen_is_full(&self) -> bool {
        self.frozen.read().len() >= self.max_frozen
    }

    pub fn freeze(&self) -> bool {
        let mut active = self.active.write();
        if active.is_empty() {
            return false;
        }
        let mut old = std::mem::replace(&mut *active, MemTable::new());
        old.convert_to_map();
        let mut frozen = self.frozen.write();
        frozen.push(old);
        true
    }

    pub fn pop_frozen(&self) -> Option<MemTable> {
        let mut frozen = self.frozen.write();
        if !frozen.is_empty() {
            Some(frozen.remove(0))
        } else {
            None
        }
    }

    pub fn active_stats(&self) -> (usize, usize) {
        let active = self.active.read();
        (active.len(), active.bytes())
    }

    pub fn frozen_count(&self) -> usize {
        self.frozen.read().len()
    }

    pub fn query_prefix(&self, key: &[u8], now_us: i64) -> Vec<InternalRecord> {
        let mut results = Vec::new();
        {
            let active = self.active.read();
            results.extend(
                active
                    .query_prefix(key, now_us)
                    .iter()
                    .map(|r| (*r).clone()),
            );
        }
        {
            let frozen = self.frozen.read();
            for mt in frozen.iter() {
                results.extend(mt.query_prefix(key, now_us).iter().map(|r| (*r).clone()));
            }
        }
        results
    }

    pub fn query_key_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        now_us: i64,
    ) -> Vec<InternalRecord> {
        let mut results = Vec::new();
        {
            let active = self.active.read();
            results.extend(
                active
                    .query_key_range(start_key, end_key, now_us)
                    .iter()
                    .map(|r| (*r).clone()),
            );
        }
        {
            let frozen = self.frozen.read();
            for mt in frozen.iter() {
                results.extend(
                    mt.query_key_range(start_key, end_key, now_us)
                        .iter()
                        .map(|r| (*r).clone()),
                );
            }
        }
        results
    }

    pub fn query_time_range(&self, ts_start: i64, ts_end: i64, now_us: i64) -> Vec<InternalRecord> {
        let mut results = Vec::new();
        {
            let active = self.active.read();
            results.extend(
                active
                    .query_time_range(ts_start, ts_end, now_us)
                    .iter()
                    .map(|r| (*r).clone()),
            );
        }
        {
            let frozen = self.frozen.read();
            for mt in frozen.iter() {
                results.extend(
                    mt.query_time_range(ts_start, ts_end, now_us)
                        .iter()
                        .map(|r| (*r).clone()),
                );
            }
        }
        results
    }

    pub fn query_prefix_time_range(
        &self,
        key: &[u8],
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<InternalRecord> {
        let mut results = Vec::new();
        {
            let active = self.active.read();
            results.extend(
                active
                    .query_prefix_time_range(key, ts_start, ts_end, now_us)
                    .iter()
                    .map(|r| (*r).clone()),
            );
        }
        {
            let frozen = self.frozen.read();
            for mt in frozen.iter() {
                results.extend(
                    mt.query_prefix_time_range(key, ts_start, ts_end, now_us)
                        .iter()
                        .map(|r| (*r).clone()),
                );
            }
        }
        results
    }

    pub fn query_key_time_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<InternalRecord> {
        let mut results = Vec::new();
        {
            let active = self.active.read();
            results.extend(
                active
                    .query_key_time_range(start_key, end_key, ts_start, ts_end, now_us)
                    .iter()
                    .map(|r| (*r).clone()),
            );
        }
        {
            let frozen = self.frozen.read();
            for mt in frozen.iter() {
                results.extend(
                    mt.query_key_time_range(start_key, end_key, ts_start, ts_end, now_us)
                        .iter()
                        .map(|r| (*r).clone()),
                );
            }
        }
        results
    }

    pub fn get(&self, key: &[u8], ts: i64, now_us: i64) -> Option<InternalRecord> {
        {
            let active = self.active.read();
            if let Some(r) = active.get(key, ts) {
                if r.expire_at >= now_us {
                    return Some(r.clone());
                }
            }
        }
        let frozen = self.frozen.read();
        for mt in frozen.iter() {
            if let Some(r) = mt.get(key, ts) {
                if r.expire_at >= now_us {
                    return Some(r.clone());
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;

    fn make_rec(key: &str, ts: i64, seq: u64) -> InternalRecord {
        InternalRecord::from_record(
            &Record {
                key: key.as_bytes().to_vec(),
                ts,
                expire_at: i64::MAX,
                value: vec![1, 2, 3],
            },
            seq,
        )
    }

    #[test]
    fn test_memtable_insert_query() {
        let mut mt = MemTable::new();
        mt.insert(make_rec("a", 100, 1));
        mt.insert(make_rec("a", 200, 2));
        mt.insert(make_rec("b", 100, 3));

        let result = mt.query_prefix(b"a", i64::MAX);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_memtable_time_range() {
        let mut mt = MemTable::new();
        mt.insert(make_rec("a", 100, 1));
        mt.insert(make_rec("a", 200, 2));
        mt.insert(make_rec("b", 300, 3));

        let result = mt.query_time_range(150, 300, i64::MAX);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_memtable_key_range() {
        let mut mt = MemTable::new();
        mt.insert(make_rec("a", 100, 1));
        mt.insert(make_rec("b", 100, 2));
        mt.insert(make_rec("c", 100, 3));

        let result = mt.query_key_range(b"a", b"b", i64::MAX);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_memtable_expiry() {
        let mut mt = MemTable::new();
        let mut rec = make_rec("a", 100, 1);
        rec.expire_at = 50;
        mt.insert(rec);
        mt.insert(make_rec("b", 100, 2));

        let result = mt.query_prefix(b"a", 100);
        assert!(result.is_empty());
    }

    #[test]
    fn test_memtables_freeze() {
        let mts = MemTables::new(2, 1024);
        mts.insert(make_rec("a", 100, 1));
        assert!(mts.freeze());
        assert_eq!(mts.frozen_count(), 1);

        let results = mts.query_prefix(b"a", i64::MAX);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_memtables_drain_frozen() {
        let mts = MemTables::new(2, 1024);
        mts.insert(make_rec("a", 100, 1));
        mts.freeze();

        let frozen = mts.pop_frozen().unwrap();
        assert_eq!(frozen.len(), 1);
        assert_eq!(mts.frozen_count(), 0);
    }

    #[test]
    fn test_memtable_query_prefix_time_range() {
        let mut mt = MemTable::new();
        mt.insert(make_rec("a", 100, 1));
        mt.insert(make_rec("a", 200, 2));
        mt.insert(make_rec("a", 300, 3));
        mt.insert(make_rec("b", 200, 4));

        let result = mt.query_prefix_time_range(b"a", 150, 250, i64::MAX);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].ts, 200);
    }

    #[test]
    fn test_memtable_query_key_time_range() {
        let mut mt = MemTable::new();
        mt.insert(make_rec("a", 100, 1));
        mt.insert(make_rec("b", 200, 2));
        mt.insert(make_rec("c", 300, 3));
        mt.insert(make_rec("d", 400, 4));

        let result = mt.query_key_time_range(b"b", b"c", 150, 350, i64::MAX);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_memtable_len_is_empty_bytes() {
        let mut mt = MemTable::new();
        assert!(mt.is_empty());
        assert_eq!(mt.len(), 0);
        assert_eq!(mt.bytes(), 0);

        mt.insert(make_rec("a", 100, 1));
        assert!(!mt.is_empty());
        assert_eq!(mt.len(), 1);
        assert!(mt.bytes() > 0);

        mt.insert(make_rec("b", 200, 2));
        assert_eq!(mt.len(), 2);
    }

    #[test]
    fn test_memtable_iter_sorted() {
        let mut mt = MemTable::new();
        mt.insert(make_rec("c", 300, 3));
        mt.insert(make_rec("a", 100, 1));
        mt.insert(make_rec("b", 200, 2));
        mt.convert_to_map();

        let keys: Vec<Vec<u8>> = mt.iter_sorted().map(|r| r.key.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_memtable_get() {
        let mut mt = MemTable::new();
        mt.insert(make_rec("a", 100, 1));
        mt.insert(make_rec("b", 200, 2));

        let result = mt.get(b"a", 100);
        assert!(result.is_some());
        assert_eq!(result.unwrap().key, b"a".to_vec());

        let result = mt.get(b"c", 100);
        assert!(result.is_none());
    }

    #[test]
    fn test_memtable_query_prefix_time_range_expiry() {
        let mut mt = MemTable::new();
        let mut rec = make_rec("a", 100, 1);
        rec.expire_at = 50;
        mt.insert(rec);
        mt.insert(make_rec("a", 200, 2));

        let result = mt.query_prefix_time_range(b"a", 0, 300, 60);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].ts, 200);
    }

    #[test]
    fn test_memtable_query_key_time_range_expiry() {
        let mut mt = MemTable::new();
        let mut rec = make_rec("b", 200, 2);
        rec.expire_at = 50;
        mt.insert(rec);
        mt.insert(make_rec("a", 100, 1));
        mt.insert(make_rec("c", 300, 3));

        let result = mt.query_key_time_range(b"a", b"c", 0, 400, 60);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_memtables_query_prefix_time_range() {
        let mts = MemTables::new(2, 1024);
        mts.insert(make_rec("a", 100, 1));
        mts.insert(make_rec("a", 200, 2));
        mts.freeze();
        mts.insert(make_rec("a", 300, 3));

        let results = mts.query_prefix_time_range(b"a", 50, 250, i64::MAX);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_memtables_query_key_time_range() {
        let mts = MemTables::new(2, 1024);
        mts.insert(make_rec("b", 100, 1));
        mts.freeze();
        mts.insert(make_rec("c", 200, 2));

        let results = mts.query_key_time_range(b"b", b"c", 50, 250, i64::MAX);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_memtables_active_stats() {
        let mts = MemTables::new(2, 1024);
        let (count, bytes) = mts.active_stats();
        assert_eq!(count, 0);
        assert_eq!(bytes, 0);

        mts.insert(make_rec("a", 100, 1));
        let (count, bytes) = mts.active_stats();
        assert_eq!(count, 1);
        assert!(bytes > 0);
    }

    #[test]
    fn test_memtables_should_flush() {
        let mts = MemTables::new(2, 30);
        assert!(!mts.should_flush());
        mts.insert(make_rec("a", 100, 1));
        assert!(mts.should_flush());
    }

    #[test]
    fn test_memtables_frozen_is_full() {
        let mts = MemTables::new(2, 1024);
        assert!(!mts.frozen_is_full());
        mts.insert(make_rec("a", 100, 1));
        mts.freeze();
        mts.insert(make_rec("b", 200, 2));
        mts.freeze();
        assert!(mts.frozen_is_full());
    }
}
