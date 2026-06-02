use ahash::AHasher;
use serde::{Deserialize, Serialize};
use std::hash::Hasher;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: u8,
}

impl BloomFilter {
    pub fn with_bits_per_key(expected_keys: usize, bits_per_key: usize) -> Self {
        let num_hashes = 2u8;
        let num_bits = (expected_keys * bits_per_key).max(64);
        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_hashes,
        }
    }

    pub fn from_keys_with_bits(keys: &[Vec<u8>], bits_per_key: usize) -> Self {
        let mut bf = Self::with_bits_per_key(keys.len(), bits_per_key);
        for key in keys {
            bf.insert(key);
        }
        bf
    }

    fn hash_pair(data: &[u8]) -> (u64, u64) {
        let mut h1 = AHasher::default();
        h1.write(data);
        let hash1 = h1.finish();
        let mut h2 = AHasher::default();
        h2.write_u8(0xFF);
        h2.write(data);
        let hash2 = h2.finish();
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
    fn test_bloom_false_positive_rate() {
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
        assert!(fp_rate < 0.05, "false positive rate too high: {}", fp_rate);
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
}
