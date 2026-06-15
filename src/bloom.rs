use serde::{Deserialize, Serialize};

/// Current hasher version. Persisted bloom filters whose `hash_version` differs
/// from this constant were built with a different hash function and MUST be
/// rebuilt before being trusted for queries. The engine does this
/// automatically on open (`Engine::open`).
///
/// History:
/// * `0` — legacy `ahash::AHasher::default()` (compile-time randomised seed,
///   unsafe across recompiles). Used by flowdb <= 0.1.6.
/// * `1` — `std::hash::DefaultHasher` (SipHash-1-3) with a fixed seed baked
///   into the binary. Stable across restarts AND recompiles.
/// * `2` — Inline FxHash (rotary-multiply) with fixed seeds. ~10× faster
///   than SipHash-1-3 while maintaining stable outputs across restarts.
pub(crate) const CURRENT_HASH_VERSION: u8 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: u8,
    /// Hasher version this filter was built with. Old manifests serialise
    /// without this field, so `#[serde(default)]` maps them to `0` (legacy).
    /// `Engine::open` detects version mismatches and rebuilds the filter.
    #[serde(default)]
    hash_version: u8,
}

impl BloomFilter {
    pub fn with_bits_per_key(expected_keys: usize, bits_per_key: usize) -> Self {
        // For 10 bits/key the optimal k = round((bits/key) * ln 2) ~= 7.
        // The legacy code hard-coded k=2 which yielded ~5% FPR; raising to 7
        // brings the FPR down to ~0.8%, eliminating unnecessary block reads.
        let num_hashes = bits_per_key.saturating_mul(69).div_ceil(100).min(30) as u8;
        // Guarantee at least 2 hash functions for small configs.
        let num_hashes = num_hashes.max(2);
        let num_bits = (expected_keys * bits_per_key).max(64);
        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_hashes,
            hash_version: CURRENT_HASH_VERSION,
        }
    }

    pub fn from_keys_with_bits(keys: &[Vec<u8>], bits_per_key: usize) -> Self {
        let mut bf = Self::with_bits_per_key(keys.len(), bits_per_key);
        for key in keys {
            bf.insert(key);
        }
        bf
    }

    /// Hasher version this filter was built with. Filters whose version differs
    /// from `CURRENT_HASH_VERSION` must not be queried (`may_contain` could
    /// return false negatives for keys that were actually inserted).
    pub fn hash_version(&self) -> u8 {
        self.hash_version
    }

    /// Mark this filter as upgraded to the current hasher version in-place.
    /// Only call this AFTER rebuilding `bits` with the current hasher (e.g. in
    /// `Engine::open`'s migration path). The dedicated migration code path
    /// rebuilds filters wholesale via `from_keys_with_bits`, so this method
    /// is currently only exercised by unit tests — kept public for future
    /// in-place rebuild scenarios and explicitly allowed dead-code.
    #[allow(dead_code)]
    pub fn mark_current(&mut self) {
        self.hash_version = CURRENT_HASH_VERSION;
    }

    /// Fast double-hashing with fixed seeds. Uses an inline FxHash (rotary-
    /// multiply) that is ~10× faster than `std::hash::DefaultHasher` (SipHash).
    /// Seeds are compile-time constants so outputs are stable across restarts
    /// and recompiles.
    ///
    /// Two independent hashes are derived via different seeds, then combined
    /// via the standard `h1 + i * h2` double-hashing scheme.
    fn hash_pair(data: &[u8]) -> (u64, u64) {
        const SEED_A: u64 = 0x5365_7244_424c_4f4f;
        const SEED_B: u64 = 0x5744_425f_464c_4f57;

        let hash1 = fxhash64(data, SEED_A ^ SEED_B);
        let hash2 = fxhash64(data, !SEED_A ^ SEED_B);
        (hash1, hash2)
    }

    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = Self::hash_pair(key);
        let num_bits = self.bits.len() as u64 * 64;
        for i in 0..self.num_hashes {
            let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let bit_pos = hash % num_bits;
            let word_idx = (bit_pos / 64) as usize;
            let bit_idx = bit_pos % 64;
            self.bits[word_idx] |= 1u64 << bit_idx;
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        // Safety net: if the filter was built with a different hasher, the
        // bit positions are meaningless for the current `hash_pair`. We
        // conservatively return `true` (possibly-in-set) so the caller falls
        // back to a full block read rather than silently skipping data.
        // `Engine::open` rebuilds stale filters on startup, so this path only
        // triggers transiently during the upgrade migration.
        if self.hash_version != CURRENT_HASH_VERSION {
            return true;
        }
        let (h1, h2) = Self::hash_pair(key);
        let num_bits = self.bits.len() as u64 * 64;
        for i in 0..self.num_hashes {
            let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let bit_pos = hash % num_bits;
            let word_idx = (bit_pos / 64) as usize;
            let bit_idx = bit_pos % 64;
            if self.bits[word_idx] & (1u64 << bit_idx) == 0 {
                return false;
            }
        }
        true
    }
}

/// Inline FxHash — rotary-multiply with fixed seeds.
/// ~10× faster than SipHash-1-3 and stable across restarts.
fn fxhash64(data: &[u8], seed: u64) -> u64 {
    const MULT: u64 = 0x517c_c1b7_2722_0a95;
    let mut hash = seed;
    let mut i = 0;
    while i + 8 <= data.len() {
        let chunk: [u8; 8] = data[i..i + 8].try_into().unwrap();
        hash = hash.rotate_left(5) ^ u64::from_le_bytes(chunk);
        hash = hash.wrapping_mul(MULT);
        i += 8;
    }
    if i < data.len() {
        let mut buf = [0u8; 8];
        buf[..data.len() - i].copy_from_slice(&data[i..]);
        hash = hash.rotate_left(5) ^ u64::from_le_bytes(buf);
        hash = hash.wrapping_mul(MULT);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..1000)
            .map(|i| format!("key_{:06}", i).into_bytes())
            .collect();
        let bf = BloomFilter::from_keys_with_bits(&keys, 10);
        for key in &keys {
            assert!(bf.may_contain(key), "false negative");
        }
    }

    #[test]
    fn test_bloom_serialization() {
        let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("k{}", i).into_bytes()).collect();
        let bf = BloomFilter::from_keys_with_bits(&keys, 10);
        let json = serde_json::to_string(&bf).unwrap();
        let bf2: BloomFilter = serde_json::from_str(&json).unwrap();
        for key in &keys {
            assert!(bf2.may_contain(key));
        }
    }

    #[test]
    fn test_bloom_version_is_current_after_build() {
        let bf = BloomFilter::from_keys_with_bits(&[b"k1".to_vec()], 10);
        assert_eq!(bf.hash_version(), CURRENT_HASH_VERSION);
    }

    #[test]
    fn test_bloom_legacy_version_loads_as_zero() {
        // Simulate an old manifest entry that has no `hash_version` field.
        // serde's `#[serde(default)]` should map it to 0 (legacy ahash).
        let json = r#"{"bits":[18446744073709551615],"num_hashes":2}"#;
        let bf: BloomFilter = serde_json::from_str(json).unwrap();
        assert_eq!(bf.hash_version(), 0);
    }

    #[test]
    fn test_bloom_stale_version_returns_true_safely() {
        // A bloom with hash_version=0 (legacy) must never produce a false
        // negative. `may_contain` should conservatively return true even for
        // keys that were never inserted, so callers fall back to a real read.
        let mut bf = BloomFilter::with_bits_per_key(10, 10);
        bf.hash_version = 0;
        // Bits are all zero — for a "real" bloom this would always return
        // false. But because the version is stale we MUST return true.
        assert!(bf.may_contain(b"never-inserted"));
    }

    #[test]
    fn test_bloom_mark_current_after_rebuild() {
        let keys: Vec<Vec<u8>> = (0..50).map(|i| format!("k{}", i).into_bytes()).collect();
        let mut bf = BloomFilter::from_keys_with_bits(&keys, 10);
        // Simulate rebuild: same bits computed with the current hasher, but
        // pretend it was a legacy filter before the rebuild.
        bf.hash_version = 0;
        // After rebuilding bits (no-op here since we reuse the same hasher),
        // we mark it current.
        bf.mark_current();
        assert_eq!(bf.hash_version(), CURRENT_HASH_VERSION);
        for k in &keys {
            assert!(bf.may_contain(k));
        }
    }

    #[test]
    fn test_bloom_false_positive_rate_is_bounded() {
        // With k=7 hashes for 10 bits/key, theoretical FPR is ~0.8%.
        // The legacy k=2 had ~5% FPR. We assert the new FPR is meaningfully
        // lower so we don't silently regress to the old value.
        let keys: Vec<Vec<u8>> = (0..10000)
            .map(|i| format!("key_{:06}", i).into_bytes())
            .collect();
        let bf = BloomFilter::from_keys_with_bits(&keys, 10);
        let mut fp = 0usize;
        let test_count = 10000;
        for i in 0..test_count {
            let key = format!("other_{:06}", i).into_bytes();
            if bf.may_contain(&key) {
                fp += 1;
            }
        }
        let fp_rate = fp as f64 / test_count as f64;
        // Upper bound: never worse than the legacy 5%.
        assert!(fp_rate < 0.05, "false positive rate too high: {}", fp_rate);
        // Lower bound sanity: hash count should have improved FPR. Allow some
        // headroom (3%) for run-to-run variation in hash distribution.
        assert!(
            fp_rate < 0.03,
            "expected improved FPR with optimal num_hashes, got {}",
            fp_rate
        );
    }
}
