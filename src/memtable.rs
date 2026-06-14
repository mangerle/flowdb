use crate::record::InternalRecord;
use parking_lot::RwLock;

/// A single memtable backed by a `Vec<InternalRecord>`.
///
/// The active memtable uses an **unsorted Vec** for O(1) writes (no key
/// clone, no BTreeMap traversal). On freeze, the Vec is sorted in-place
/// by `(key, ts, seq)` to support efficient range queries and SST flush.
///
/// Write path:  `insert` is O(1) — just a Vec push.
/// Read path:   `get` / `query_*` are O(n) linear scans on the active
///              table. This is acceptable because reads typically hit
///              SSTables (via bloom filter + block index), and the active
///              table is flushed quickly.
/// Freeze path: `sort()` is O(n log n) — called once per flush cycle.
pub(crate) struct MemTable {
    records: Vec<InternalRecord>,
    bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            bytes: 0,
        }
    }

    pub fn insert(&mut self, rec: InternalRecord) {
        self.bytes += rec.estimated_size();
        self.records.push(rec);
    }

    pub fn get(&self, key: &[u8], ts: i64) -> Option<&InternalRecord> {
        // Linear scan for the highest-seq record matching (key, ts).
        self.records
            .iter()
            .rev()
            .find(|r| r.key.as_slice() == key && r.ts == ts)
    }

    pub fn query_prefix(&self, key: &[u8], now_us: i64) -> Vec<&InternalRecord> {
        self.records
            .iter()
            .filter(|r| r.key.starts_with(key) && r.expire_at >= now_us)
            .collect()
    }

    pub fn query_key_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        now_us: i64,
    ) -> Vec<&InternalRecord> {
        self.records
            .iter()
            .filter(|r| {
                r.key.as_slice() >= start_key
                    && r.key.as_slice() <= end_key
                    && r.expire_at >= now_us
            })
            .collect()
    }

    pub fn query_time_range(
        &self,
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<&InternalRecord> {
        self.records
            .iter()
            .filter(|r| r.ts >= ts_start && r.ts <= ts_end && r.expire_at >= now_us)
            .collect()
    }

    pub fn query_prefix_time_range(
        &self,
        key: &[u8],
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<&InternalRecord> {
        self.records
            .iter()
            .filter(|r| {
                r.key.starts_with(key)
                    && r.ts >= ts_start
                    && r.ts <= ts_end
                    && r.expire_at >= now_us
            })
            .collect()
    }

    pub fn query_key_time_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<&InternalRecord> {
        self.records
            .iter()
            .filter(|r| {
                r.key.as_slice() >= start_key
                    && r.key.as_slice() <= end_key
                    && r.ts >= ts_start
                    && r.ts <= ts_end
                    && r.expire_at >= now_us
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Sort records in-place by (key, ts, seq). Called during freeze.
    pub fn sort(&mut self) {
        self.records
            .sort_by(|a, b| a.key.cmp(&b.key).then(a.ts.cmp(&b.ts)).then(a.seq.cmp(&b.seq)));
    }

    /// Iterate over records in sorted order (call `sort()` first).
    pub fn iter_sorted(&self) -> impl Iterator<Item = &InternalRecord> {
        self.records.iter()
    }
}

#[allow(dead_code)]
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

    /// Returns true when frozen tables have piled up past `max_frozen`,
    /// indicating that flush can't keep up with writes.  The write path
    /// should apply backpressure (block or error) in this case.
    pub fn frozen_backpressure(&self) -> bool {
        self.frozen.read().len() >= self.max_frozen
    }

    pub fn freeze(&self) -> bool {
        let mut active = self.active.write();
        if active.is_empty() {
            return false;
        }
        // Sort the active table in-place before freezing, so that
        // iter_sorted() and SST flush can consume it in order.
        active.sort();
        let old = std::mem::replace(&mut *active, MemTable::new());
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
        // Vec-backed memtable: call sort() before iter_sorted().
        let mut mt = MemTable::new();
        mt.insert(make_rec("c", 300, 3));
        mt.insert(make_rec("a", 100, 1));
        mt.insert(make_rec("b", 200, 2));
        mt.sort();

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

    // ------------------------------------------------------------------
    // Regression tests for MemTable::get multi-version resolution bug.
    //
    // BUG HISTORY: MemTable::get used `.next()` (lowest seq) instead of
    // `.next_back()` (highest seq).  This caused stale reads after delete
    // or patch operations on the same (key, ts).  The original test
    // `test_memtable_get` only tested with a single version per (key, ts)
    // — exactly the happy path that didn't exercise the bug.
    // ------------------------------------------------------------------

    #[test]
    fn test_memtable_get_returns_highest_seq_for_same_key_ts() {
        let mut mt = MemTable::new();
        // Two versions of ("a", 100) with different values.
        let mut rec1 = make_rec("a", 100, 1);
        rec1.value = b"old".to_vec();
        let mut rec2 = make_rec("a", 100, 2);
        rec2.value = b"new".to_vec();
        mt.insert(rec1);
        mt.insert(rec2);

        let result = mt.get(b"a", 100).expect("should find a record");
        assert_eq!(
            result.seq, 2,
            "get must return highest-seq version (seq=2), not lowest (seq=1)"
        );
        assert_eq!(result.value, b"new");
    }

    #[test]
    fn test_memtable_get_after_delete_returns_tombstone() {
        let mut mt = MemTable::new();
        // Put at seq=1, Delete at seq=2 for same (key, ts).
        mt.insert(make_rec("a", 100, 1));
        let delete = InternalRecord::delete(b"a".to_vec(), 100, 2);
        mt.insert(delete);

        let result = mt.get(b"a", 100).expect("should find the tombstone");
        assert_eq!(result.seq, 2, "must return the delete tombstone (highest seq)");
        assert!(
            result.op != crate::record::Op::Put,
            "delete tombstone must win over older Put"
        );
    }

    #[test]
    fn test_memtable_get_three_versions() {
        let mut mt = MemTable::new();
        let mut rec1 = make_rec("k", 50, 10);
        rec1.value = b"v1".to_vec();
        let mut rec2 = make_rec("k", 50, 20);
        rec2.value = b"v2".to_vec();
        let mut rec3 = make_rec("k", 50, 30);
        rec3.value = b"v3".to_vec();
        mt.insert(rec1);
        mt.insert(rec2);
        mt.insert(rec3);

        let result = mt.get(b"k", 50).unwrap();
        assert_eq!(result.seq, 30);
        assert_eq!(result.value, b"v3");
    }

    #[test]
    fn test_memtables_get_multi_version_across_active_and_frozen() {
        // Put in frozen table (seq=1), then a newer Put in active (seq=2).
        // MemTables::get should prefer active (higher seq).
        let mts = MemTables::new(2, 1024);
        let mut rec1 = make_rec("x", 100, 1);
        rec1.value = b"frozen_val".to_vec();
        mts.insert(rec1);
        mts.freeze();

        let mut rec2 = make_rec("x", 100, 2);
        rec2.value = b"active_val".to_vec();
        mts.insert(rec2);

        let result = mts.get(b"x", 100, i64::MAX).unwrap();
        assert_eq!(result.value, b"active_val");
        assert_eq!(result.seq, 2);
    }

    #[test]
    fn test_memtables_get_delete_in_active_overrides_put_in_frozen() {
        // Put in frozen table, Delete in active → must see deleted.
        let mts = MemTables::new(2, 1024);
        mts.insert(make_rec("d", 200, 1));
        mts.freeze();
        let delete = InternalRecord::delete(b"d".to_vec(), 200, 2);
        mts.insert(delete);

        let result = mts.get(b"d", 200, i64::MAX);
        assert!(
            result.is_none() || result.unwrap().op != crate::record::Op::Put,
            "delete in active must override put in frozen"
        );
    }
}
