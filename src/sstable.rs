use crate::bloom::BloomFilter;
use crate::cache::{BlockCache, CacheKey};
use crate::error::{FlowError, Result};
use crate::manifest::BlockInfo;
use crate::record::{InternalRecord, Op};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

const BLOCK_MAGIC_LZ4: u32 = 0x54534E42;
const BLOCK_MAGIC_ZSTD: u32 = 0x5A534E42;
const HEADER_SIZE: usize = 48;

pub(crate) struct SstBlock {
    pub records: Vec<InternalRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct BlockHeader {
    pub num_records: u32,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_expire: i64,
    pub max_expire: i64,
    pub data_len: u32,
    pub compressed_len: u32,
    pub is_zstd: bool,
}

impl BlockHeader {
    /// Serialises the header into a fixed-size big-endian byte array (48 bytes).
    /// Layout: magic(4) | num_records(4) | min_ts(8) | max_ts(8) |
    ///         min_expire(8) | max_expire(8) | data_len(4) | compressed_len(4)
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let magic = if self.is_zstd {
            BLOCK_MAGIC_ZSTD
        } else {
            BLOCK_MAGIC_LZ4
        };
        let mut buf = [0u8; HEADER_SIZE];
        let mut pos = 0;
        buf[pos..pos + 4].copy_from_slice(&magic.to_be_bytes());
        pos += 4;
        buf[pos..pos + 4].copy_from_slice(&self.num_records.to_be_bytes());
        pos += 4;
        buf[pos..pos + 8].copy_from_slice(&self.min_ts.to_be_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.max_ts.to_be_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.min_expire.to_be_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.max_expire.to_be_bytes());
        pos += 8;
        buf[pos..pos + 4].copy_from_slice(&self.data_len.to_be_bytes());
        pos += 4;
        buf[pos..pos + 4].copy_from_slice(&self.compressed_len.to_be_bytes());
        buf
    }

    /// Parses a `BlockHeader` from a big-endian 48-byte slice.
    /// Each field is read via `data[i..j].try_into().unwrap()` — the length
    /// check against `HEADER_SIZE` guarantees the conversion never panics.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(FlowError::Corruption {
                file: "sst".into(),
                msg: "block header too short".into(),
            });
        }
        let magic = u32::from_be_bytes(data[..4].try_into().unwrap());
        let is_zstd = match magic {
            BLOCK_MAGIC_LZ4 => false,
            BLOCK_MAGIC_ZSTD => true,
            _ => {
                return Err(FlowError::InvalidMagic {
                    expected: BLOCK_MAGIC_LZ4,
                    actual: magic,
                });
            }
        };
        Ok(Self {
            num_records: u32::from_be_bytes(data[4..8].try_into().unwrap()),
            min_ts: i64::from_be_bytes(data[8..16].try_into().unwrap()),
            max_ts: i64::from_be_bytes(data[16..24].try_into().unwrap()),
            min_expire: i64::from_be_bytes(data[24..32].try_into().unwrap()),
            max_expire: i64::from_be_bytes(data[32..40].try_into().unwrap()),
            data_len: u32::from_be_bytes(data[40..44].try_into().unwrap()),
            compressed_len: u32::from_be_bytes(data[44..48].try_into().unwrap()),
            is_zstd,
        })
    }
}

fn decompress_block(data: &[u8], header: &BlockHeader) -> Result<Vec<u8>> {
    if header.is_zstd {
        zstd::bulk::decompress(data, header.data_len as usize)
            .map_err(|e| FlowError::Other(format!("zstd decompress: {}", e)))
    } else {
        lz4_flex::block::decompress(data, header.data_len as usize)
            .map_err(|e| FlowError::Other(format!("lz4 decompress: {}", e)))
    }
}

/// Encodes records into a compact binary buffer (big-endian, no compression).
/// Per-record layout: key_len(2) | key | ts(8) | expire_at(8) | op(1) |
///                    range_end_len(2) | range_end | val_len(4) | value
fn encode_records(records: &[InternalRecord]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(records.len() * 64);
    for rec in records {
        buf.extend_from_slice(&(rec.key.len() as u16).to_be_bytes());
        buf.extend_from_slice(&rec.key);
        buf.extend_from_slice(&rec.ts.to_be_bytes());
        buf.extend_from_slice(&rec.expire_at.to_be_bytes());
        buf.push(rec.op.to_u8());
        match &rec.range_end {
            Some(re) => {
                buf.extend_from_slice(&(re.len() as u16).to_be_bytes());
                buf.extend_from_slice(re);
            }
            None => {
                buf.extend_from_slice(&0u16.to_be_bytes());
            }
        }
        buf.extend_from_slice(&(rec.value.len() as u32).to_be_bytes());
        buf.extend_from_slice(&rec.value);
    }
    buf
}

/// Decodes `count` records from a big-endian byte slice.
/// Integer fields are read via `data[pos..pos+N].try_into().unwrap()` after
/// an explicit bounds check, so the unwrap is guaranteed safe.
fn decode_records(data: &[u8], count: u32) -> Result<Vec<InternalRecord>> {
    let mut records = Vec::with_capacity(count as usize);
    let mut pos = 0;
    for _ in 0..count {
        if pos + 2 > data.len() {
            break;
        }
        let key_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + key_len > data.len() {
            break;
        }
        let key = data[pos..pos + key_len].to_vec();
        pos += key_len;

        if pos + 17 > data.len() {
            break;
        }
        let ts = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let expire_at = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let op = Op::from_u8(data[pos]);
        pos += 1;

        if pos + 2 > data.len() {
            break;
        }
        let re_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let range_end = if re_len > 0 {
            if pos + re_len > data.len() {
                break;
            }
            let s = data[pos..pos + re_len].to_vec();
            pos += re_len;
            Some(s)
        } else {
            None
        };

        if pos + 4 > data.len() {
            break;
        }
        let val_len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + val_len > data.len() {
            break;
        }
        let value = data[pos..pos + val_len].to_vec();
        pos += val_len;

        records.push(InternalRecord {
            seq: 0,
            op,
            key,
            ts,
            expire_at,
            value,
            range_end,
        });
    }
    Ok(records)
}

pub(crate) struct SstWriter;

impl SstWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn write(
        path: &Path,
        records: &[InternalRecord],
        block_size: usize,
        zstd_level: i32,
        bloom_bits_per_key: usize,
        use_zstd: bool,
    ) -> Result<(u64, Vec<BlockInfo>, BloomFilter)> {
        let mut file = std::fs::File::create(path)?;
        let mut block_infos = Vec::new();
        let mut total_bytes: u64 = 0;

        let mut unique_keys: Vec<Vec<u8>> = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        for rec in records {
            if last_key.as_deref() != Some(&rec.key) {
                unique_keys.push(rec.key.clone());
                last_key = Some(rec.key.clone());
            }
        }
        let bloom = BloomFilter::from_keys_with_bits(&unique_keys, bloom_bits_per_key);

        for chunk in records.chunks(block_size.max(1)) {
            let raw_data = encode_records(chunk);
            let data_len = raw_data.len() as u32;
            let compressed = if use_zstd {
                zstd::bulk::compress(&raw_data, zstd_level)?
            } else {
                lz4_flex::block::compress(&raw_data)
            };
            let compressed_len = compressed.len() as u32;

            let min_ts = chunk.iter().map(|r| r.ts).min().unwrap_or(0);
            let max_ts = chunk.iter().map(|r| r.ts).max().unwrap_or(0);
            let min_expire = chunk.iter().map(|r| r.expire_at).min().unwrap_or(0);
            let max_expire = chunk.iter().map(|r| r.expire_at).max().unwrap_or(0);

            let first_key = chunk.first().map(|r| r.key.clone()).unwrap_or_default();
            let last_key = chunk.last().map(|r| r.key.clone()).unwrap_or_default();

            let header = BlockHeader {
                num_records: chunk.len() as u32,
                min_ts,
                max_ts,
                min_expire,
                max_expire,
                data_len,
                compressed_len,
                is_zstd: use_zstd,
            };

            let header_bytes = header.to_bytes();
            file.write_all(&header_bytes)?;
            file.write_all(&compressed)?;
            total_bytes += HEADER_SIZE as u64 + compressed.len() as u64;

            block_infos.push(BlockInfo {
                block_idx: block_infos.len() as u32,
                min_key: first_key,
                max_key: last_key,
                min_ts,
                max_ts,
                min_expire,
                max_expire,
            });
        }

        file.flush()?;
        file.sync_all()?;
        Ok((total_bytes, block_infos, bloom))
    }
}

pub(crate) struct SstReader {
    _file: std::fs::File,
    mmap: memmap2::Mmap,
    sst_id: u32,
    block_offsets: Vec<u64>,
}

impl SstReader {
    pub fn open(path: &Path, sst_id: u32, block_count: usize) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let total_size = file.metadata()?.len() as usize;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        let mut offsets = Vec::with_capacity(block_count);
        let mut pos: usize = 0;
        while pos + HEADER_SIZE <= total_size {
            offsets.push(pos as u64);
            let header = BlockHeader::from_bytes(&mmap[pos..pos + HEADER_SIZE])?;
            pos += HEADER_SIZE + header.compressed_len as usize;
        }

        Ok(Self {
            _file: file,
            mmap,
            sst_id,
            block_offsets: offsets,
        })
    }

    pub fn read_block(&self, block_idx: u32, cache: Option<&BlockCache>) -> Result<SstBlock> {
        let cache_key = CacheKey {
            sst_id: self.sst_id,
            block_idx,
        };

        if let Some(cache) = cache {
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(SstBlock {
                    records: (*cached).clone(),
                });
            }
        }

        let raw_records = self.read_block_inner(block_idx)?;

        if let Some(cache) = cache {
            cache.insert(cache_key, raw_records.clone());
        }

        Ok(SstBlock {
            records: raw_records,
        })
    }

    pub fn read_block_arc(
        &self,
        block_idx: u32,
        cache: &BlockCache,
    ) -> Result<Arc<Vec<InternalRecord>>> {
        let cache_key = CacheKey {
            sst_id: self.sst_id,
            block_idx,
        };

        if let Some(cached) = cache.get(&cache_key) {
            return Ok(cached);
        }

        let raw_records = self.read_block_inner(block_idx)?;
        cache.insert(cache_key, raw_records.clone());
        Ok(Arc::new(raw_records))
    }

    fn read_block_inner(&self, block_idx: u32) -> Result<Vec<InternalRecord>> {
        let offset =
            self.block_offsets
                .get(block_idx as usize)
                .ok_or(FlowError::BlockNotFound {
                    sst_id: self.sst_id,
                    block_idx,
                })?;

        let data = &self.mmap;
        let pos = *offset as usize;
        if pos + HEADER_SIZE > data.len() {
            return Err(FlowError::Corruption {
                file: format!("sst_{}", self.sst_id),
                msg: format!("block {} out of bounds", block_idx),
            });
        }

        let header = BlockHeader::from_bytes(&data[pos..pos + HEADER_SIZE])?;
        let compressed_start = pos + HEADER_SIZE;
        let compressed_end = compressed_start + header.compressed_len as usize;
        if compressed_end > data.len() {
            return Err(FlowError::Corruption {
                file: format!("sst_{}", self.sst_id),
                msg: format!("block {} compressed data truncated", block_idx),
            });
        }

        let raw = decompress_block(&data[compressed_start..compressed_end], &header)?;
        decode_records(&raw, header.num_records)
    }

    pub fn read_block_cached(
        &self,
        block_idx: u32,
        cache: &BlockCache,
    ) -> Option<Arc<Vec<InternalRecord>>> {
        let cache_key = CacheKey {
            sst_id: self.sst_id,
            block_idx,
        };
        cache.get(&cache_key)
    }

    pub fn read_block_decompress(
        &self,
        block_idx: u32,
    ) -> Result<(BlockHeader, Vec<InternalRecord>)> {
        let offset =
            self.block_offsets
                .get(block_idx as usize)
                .ok_or(FlowError::BlockNotFound {
                    sst_id: self.sst_id,
                    block_idx,
                })?;

        let data = &self.mmap;
        let pos = *offset as usize;
        if pos + HEADER_SIZE > data.len() {
            return Err(FlowError::Corruption {
                file: format!("sst_{}", self.sst_id),
                msg: format!("block {} out of bounds", block_idx),
            });
        }

        let header = BlockHeader::from_bytes(&data[pos..pos + HEADER_SIZE])?;
        let compressed_start = pos + HEADER_SIZE;
        let compressed_end = compressed_start + header.compressed_len as usize;
        if compressed_end > data.len() {
            return Err(FlowError::Corruption {
                file: format!("sst_{}", self.sst_id),
                msg: format!("block {} compressed data truncated", block_idx),
            });
        }

        let raw = decompress_block(&data[compressed_start..compressed_end], &header)?;
        let records = decode_records(&raw, header.num_records)?;
        Ok((header, records))
    }

    pub fn block_count(&self) -> u32 {
        self.block_offsets.len() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use tempfile::TempDir;

    fn make_records(n: usize) -> Vec<InternalRecord> {
        (0..n)
            .map(|i| {
                InternalRecord::from_record(
                    &Record {
                        key: format!("key_{:04}", i).into_bytes(),
                        ts: (i * 100) as i64,
                        expire_at: i64::MAX,
                        value: vec![1, 2, 3, 4],
                    },
                    i as u64,
                )
            })
            .collect()
    }

    #[test]
    fn test_sst_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sst");
        let records = make_records(100);

        let (bytes, block_infos, _) = SstWriter::write(&path, &records, 10, 3, 10, false).unwrap();
        assert!(bytes > 0);
        assert_eq!(block_infos.len(), 10);

        let reader = SstReader::open(&path, 1, block_infos.len()).unwrap();
        assert_eq!(reader.block_count(), 10);

        let block = reader.read_block(0, None).unwrap();
        assert_eq!(block.records.len(), 10);
        assert_eq!(block.records[0].key.as_slice(), b"key_0000");
    }

    #[test]
    fn test_sst_all_blocks_readable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sst");
        let records = make_records(50);

        let (_, block_infos, _) = SstWriter::write(&path, &records, 10, 3, 10, false).unwrap();
        let reader = SstReader::open(&path, 1, block_infos.len()).unwrap();

        let mut all_records = Vec::new();
        for i in 0..reader.block_count() {
            let block = reader.read_block(i, None).unwrap();
            all_records.extend(block.records);
        }

        assert_eq!(all_records.len(), 50);
        for (i, rec) in all_records.iter().enumerate() {
            assert_eq!(rec.key, format!("key_{:04}", i).into_bytes());
        }
    }

    #[test]
    fn test_sst_block_metadata() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sst");
        let records = make_records(20);

        let (_, block_infos, _) = SstWriter::write(&path, &records, 10, 3, 10, false).unwrap();
        assert_eq!(block_infos.len(), 2);

        assert_eq!(block_infos[0].min_key, b"key_0000");
        assert_eq!(block_infos[0].max_key, b"key_0009");
        assert_eq!(block_infos[0].min_ts, 0);
        assert_eq!(block_infos[0].max_ts, 900);

        assert_eq!(block_infos[1].min_key, b"key_0010");
        assert_eq!(block_infos[1].max_key, b"key_0019");
    }

    #[test]
    fn test_sst_compression() {
        let dir = TempDir::new().unwrap();
        let records: Vec<InternalRecord> = (0..100)
            .map(|i| {
                InternalRecord::from_record(
                    &Record {
                        key: b"same_key".to_vec(),
                        ts: i,
                        expire_at: i64::MAX,
                        value: vec![0u8; 100],
                    },
                    i as u64,
                )
            })
            .collect();

        let path = dir.path().join("compressed.sst");
        let (bytes, _, _) = SstWriter::write(&path, &records, 100, 3, 10, false).unwrap();

        let raw_size: usize = records.iter().map(|r| r.estimated_size()).sum();
        assert!(bytes < raw_size as u64);
    }
}
