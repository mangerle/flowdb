use crate::error::{FlowError, Result};
use crate::record::{InternalRecord, Op};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

struct WalSegment {
    writer: std::io::BufWriter<std::fs::File>,
    path: PathBuf,
    written_bytes: u64,
}

pub(crate) struct Wal {
    dir: PathBuf,
    segments: Vec<WalSegment>,
    max_segment_bytes: u64,
    next_seq: AtomicU64,
    next_segment_id: u64,
}

impl Wal {
    pub fn open(dir: &Path, segment_size_mb: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut wal = Self {
            dir: dir.to_path_buf(),
            segments: Vec::new(),
            max_segment_bytes: segment_size_mb * 1024 * 1024,
            next_seq: AtomicU64::new(1),
            next_segment_id: 1,
        };
        wal.load_existing()?;
        Ok(wal)
    }

    fn load_existing(&mut self) -> Result<()> {
        let mut entries: Vec<(u64, PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "wal") {
                if let Some(name) = path.file_stem().and_then(|n| n.to_str()) {
                    if let Ok(seq) = name.parse::<u64>() {
                        entries.push((seq, path));
                    }
                }
            }
        }
        entries.sort_by_key(|(seq, _)| *seq);

        let mut max_seq: u64 = 0;
        let mut max_seg_id: u64 = 0;
        for (seq, path) in &entries {
            max_seg_id = max_seg_id.max(*seq);
            let file = std::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .open(path)?;
            self.segments.push(WalSegment {
                writer: std::io::BufWriter::with_capacity(256 * 1024, file),
                path: path.clone(),
                written_bytes: 0,
            });
            max_seq =
                max_seq.max(self.find_max_seq_in_segment(&self.segments.last().unwrap().path)?);
        }

        if max_seq > 0 {
            self.next_seq.store(max_seq + 1, Ordering::SeqCst);
        }
        self.next_segment_id = max_seg_id + 1;

        if self.segments.is_empty() {
            self.create_new_segment(self.next_segment_id)?;
            self.next_segment_id += 1;
        }

        Ok(())
    }

    fn find_max_seq_in_segment(&self, path: &Path) -> Result<u64> {
        let data = std::fs::read(path)?;
        let mut max_seq = 0u64;
        let mut pos: usize = 0;
        while pos < data.len() {
            if pos + 11 > data.len() {
                break;
            }
            let seq = read_u64(&data[pos..pos + 8]);
            if let Some(len) = self.skip_record(&data, pos)? {
                max_seq = max_seq.max(seq);
                pos += len;
            } else {
                break;
            }
        }
        Ok(max_seq)
    }

    fn skip_record(&self, data: &[u8], start: usize) -> Result<Option<usize>> {
        let mut pos = start;
        pos += 8;
        pos += 1;

        if pos + 2 > data.len() {
            return Ok(None);
        }
        let key_len = read_u16(&data[pos..pos + 2]) as usize;
        pos += 2 + key_len;

        if pos + 16 > data.len() {
            return Ok(None);
        }
        pos += 16;

        if pos + 4 > data.len() {
            return Ok(None);
        }
        let range_end_len = read_u32(&data[pos..pos + 4]) as usize;
        pos += 4 + range_end_len;

        if pos + 4 > data.len() {
            return Ok(None);
        }
        let val_len = read_u32(&data[pos..pos + 4]) as usize;
        pos += 4 + val_len;

        Ok(Some(pos - start))
    }

    fn create_new_segment(&mut self, seq: u64) -> Result<()> {
        let name = format!("{:09}.wal", seq);
        let path = self.dir.join(&name);
        if path.exists() {
            let file = std::fs::OpenOptions::new().append(true).open(&path)?;
            self.segments.push(WalSegment {
                writer: std::io::BufWriter::with_capacity(256 * 1024, file),
                path,
                written_bytes: 0,
            });
            return Ok(());
        }
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        self.segments.push(WalSegment {
            writer: std::io::BufWriter::with_capacity(256 * 1024, file),
            path,
            written_bytes: 0,
        });
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        for seg in &mut self.segments {
            seg.writer.flush()?;
        }
        Ok(())
    }

    pub fn write_encoded(&mut self, buf: &[u8]) -> Result<()> {
        if self.segments.is_empty() {
            self.create_new_segment(self.next_segment_id)?;
        }

        let seg = self
            .segments
            .last_mut()
            .ok_or(FlowError::Other("no WAL segment".into()))?;

        seg.writer.write_all(buf)?;
        seg.written_bytes += buf.len() as u64;

        if seg.written_bytes >= self.max_segment_bytes {
            seg.writer.flush()?;
            let id = self.next_segment_id;
            self.next_segment_id += 1;
            self.create_new_segment(id)?;
        }

        Ok(())
    }

    pub fn replay_from(&mut self, after_seq: u64) -> Result<Vec<InternalRecord>> {
        let mut records = Vec::new();
        for segment in &mut self.segments {
            segment.writer.flush()?;
            let data = std::fs::read(&segment.path)?;
            let mut pos: usize = 0;
            while pos < data.len() {
                match decode_record(&data[pos..]) {
                    Ok(Some((rec, advance))) => {
                        if rec.seq > after_seq {
                            records.push(rec);
                        }
                        pos += advance;
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
        records.sort_by_key(|r| r.seq);
        Ok(records)
    }

    pub fn truncate_before(&mut self, seq: u64) -> Result<()> {
        let to_delete: Vec<PathBuf> = self
            .segments
            .iter()
            .filter(|s| {
                let s_seq = s
                    .path
                    .file_stem()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0);
                s_seq > 0 && s_seq < seq
            })
            .map(|s| s.path.clone())
            .collect();

        self.segments.retain(|s| {
            let s_seq = s
                .path
                .file_stem()
                .and_then(|n| n.to_str())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            s_seq == 0 || s_seq >= seq
        });

        if self.segments.is_empty() {
            let id = self.next_segment_id;
            self.next_segment_id += 1;
            self.create_new_segment(id)?;
        }

        for path in to_delete {
            let _ = std::fs::remove_file(&path);
        }
        Ok(())
    }
}

/// Encodes multiple records into a single binary buffer (big-endian).
/// Each record is appended sequentially via `encode_record`. Returns the
/// buffer and the total estimated memory footprint.
pub(crate) fn encode_batch(records: &[InternalRecord]) -> (Vec<u8>, u64) {
    let mut buf = Vec::with_capacity(records.len() * 80);
    let mut total_mem_bytes: u64 = 0;
    for rec in records {
        encode_record(rec, &mut buf);
        total_mem_bytes += rec.estimated_size() as u64;
    }
    (buf, total_mem_bytes)
}

/// Returns the exact encoded byte size of a single `InternalRecord`.
pub(crate) fn encoded_size(rec: &InternalRecord) -> usize {
    8 + 1
        + 2
        + rec.key.len()
        + 8
        + 8
        + 4
        + rec.range_end.as_ref().map_or(0, |re| re.len())
        + 4
        + rec.value.len()
}

/// Encodes a single record into `buf` in big-endian format.
/// Layout: seq(8) | op(1) | key_len(2) | key | ts(8) | expire_at(8) |
///         range_end_len(4) | range_end | val_len(4) | value
pub(crate) fn encode_record(rec: &InternalRecord, buf: &mut Vec<u8>) {
    buf.reserve(encoded_size(rec));
    buf.extend_from_slice(&rec.seq.to_be_bytes());
    buf.push(rec.op.to_u8());
    buf.extend_from_slice(&(rec.key.len() as u16).to_be_bytes());
    buf.extend_from_slice(&rec.key);
    buf.extend_from_slice(&rec.ts.to_be_bytes());
    buf.extend_from_slice(&rec.expire_at.to_be_bytes());
    match &rec.range_end {
        Some(re) => {
            buf.extend_from_slice(&(re.len() as u32).to_be_bytes());
            buf.extend_from_slice(re);
        }
        None => {
            buf.extend_from_slice(&0u32.to_be_bytes());
        }
    }
    buf.extend_from_slice(&(rec.value.len() as u32).to_be_bytes());
    buf.extend_from_slice(&rec.value);
}

/// Decodes one `InternalRecord` from the front of `data`.
/// Uses the `read_*` helpers which convert via `data[..N].try_into().unwrap()`.
/// Returns `(record, bytes_consumed)` or `None` if data is truncated.
fn decode_record(data: &[u8]) -> Result<Option<(InternalRecord, usize)>> {
    let mut pos = 0;
    if data.len() < 8 + 1 + 2 {
        return Ok(None);
    }
    let seq = read_u64(&data[pos..pos + 8]);
    pos += 8;
    let op = Op::from_u8(data[pos]);
    pos += 1;

    let key_len = read_u16(&data[pos..pos + 2]) as usize;
    pos += 2;
    if pos + key_len > data.len() {
        return Ok(None);
    }
    let key = data[pos..pos + key_len].to_vec();
    pos += key_len;

    if pos + 8 + 8 > data.len() {
        return Ok(None);
    }
    let ts = read_i64(&data[pos..pos + 8]);
    pos += 8;
    let expire_at = read_i64(&data[pos..pos + 8]);
    pos += 8;

    if pos + 4 > data.len() {
        return Ok(None);
    }
    let range_end_len = read_u32(&data[pos..pos + 4]) as usize;
    pos += 4;
    let range_end = if range_end_len > 0 {
        if pos + range_end_len > data.len() {
            return Ok(None);
        }
        let re = data[pos..pos + range_end_len].to_vec();
        pos += range_end_len;
        Some(re)
    } else {
        None
    };

    if pos + 4 > data.len() {
        return Ok(None);
    }
    let val_len = read_u32(&data[pos..pos + 4]) as usize;
    pos += 4;
    if pos + val_len > data.len() {
        return Ok(None);
    }
    let value = data[pos..pos + val_len].to_vec();
    pos += val_len;

    Ok(Some((
        InternalRecord {
            seq,
            op,
            key,
            ts,
            expire_at,
            value,
            range_end,
        },
        pos,
    )))
}

/// Reads 8 bytes as big-endian u64 via `try_into().unwrap()`.
/// Callers guarantee the slice is at least 8 bytes long.
fn read_u64(data: &[u8]) -> u64 {
    u64::from_be_bytes(data[..8].try_into().unwrap())
}

/// Reads 8 bytes as big-endian i64 via `try_into().unwrap()`.
fn read_i64(data: &[u8]) -> i64 {
    i64::from_be_bytes(data[..8].try_into().unwrap())
}

/// Reads 4 bytes as big-endian u32 via `try_into().unwrap()`.
fn read_u32(data: &[u8]) -> u32 {
    u32::from_be_bytes(data[..4].try_into().unwrap())
}

/// Reads 2 bytes as big-endian u16 via `try_into().unwrap()`.
fn read_u16(data: &[u8]) -> u16 {
    u16::from_be_bytes(data[..2].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use tempfile::TempDir;

    #[test]
    fn test_read_u64() {
        let n: u64 = 0x0102030405060708;
        let bytes = n.to_be_bytes();
        assert_eq!(read_u64(&bytes), n);
    }

    #[test]
    fn test_read_i64() {
        let n: i64 = -0x0102030405060708;
        let bytes = n.to_be_bytes();
        assert_eq!(read_i64(&bytes), n);
    }

    #[test]
    fn test_read_u32() {
        let n: u32 = 0x01020304;
        let bytes = n.to_be_bytes();
        assert_eq!(read_u32(&bytes), n);
    }

    #[test]
    fn test_read_u16() {
        let n: u16 = 0x0102;
        let bytes = n.to_be_bytes();
        assert_eq!(read_u16(&bytes), n);
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_read_u64_panics_on_short_slice() {
        read_u64(&[1, 2, 3]);
    }

    fn make_record(key: &str, ts: i64, seq: u64) -> InternalRecord {
        InternalRecord::from_record(
            &Record {
                key: key.to_string(),
                ts,
                expire_at: i64::MAX,
                value: vec![1, 2, 3],
            },
            seq,
        )
    }

    #[test]
    fn test_wal_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::open(dir.path(), 64).unwrap();

        let recs = vec![make_record("key1", 100, 1), make_record("key2", 200, 2)];
        let (buf, _) = encode_batch(&recs);
        wal.write_encoded(&buf).unwrap();

        let result = wal.replay_from(0).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].key, b"key1".as_slice());
        assert_eq!(result[1].key, b"key2".as_slice());
    }

    #[test]
    fn test_wal_replay_from_seq() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::open(dir.path(), 64).unwrap();

        let recs = vec![make_record("key1", 100, 1), make_record("key2", 200, 2)];
        let (buf, _) = encode_batch(&recs);
        wal.write_encoded(&buf).unwrap();

        let result = wal.replay_from(1).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"key2".as_slice());
    }

    #[test]
    fn test_wal_truncation_detection() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::open(dir.path(), 64).unwrap();

        let recs = vec![make_record("key1", 100, 1)];
        let (buf, _) = encode_batch(&recs);
        wal.write_encoded(&buf).unwrap();

        let seg = wal.segments.first().unwrap();
        let path = seg.path.clone();
        drop(wal);

        let data = std::fs::read(&path).unwrap();
        let truncated = &data[..data.len() / 2];
        std::fs::write(&path, truncated).unwrap();

        let mut wal2 = Wal::open(dir.path(), 64).unwrap();
        let result = wal2.replay_from(0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_wal_recovery_after_restart() {
        let dir = TempDir::new().unwrap();

        {
            let mut wal = Wal::open(dir.path(), 64).unwrap();
            let recs = vec![
                make_record("a", 1, 1),
                make_record("b", 2, 2),
                make_record("c", 3, 3),
            ];
            let (buf, _) = encode_batch(&recs);
            wal.write_encoded(&buf).unwrap();
        }

        {
            let mut wal = Wal::open(dir.path(), 64).unwrap();
            let result = wal.replay_from(0).unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result[0].key, b"a".as_slice());
            assert_eq!(result[2].key, b"c".as_slice());
        }
    }

    #[test]
    fn test_encode_batch_consistency() {
        let recs = vec![make_record("alpha", 100, 1), make_record("beta", 200, 2)];
        let (buf, _) = encode_batch(&recs);
        assert!(!buf.is_empty());

        let (rec1, adv1) = decode_record(&buf).unwrap().unwrap();
        assert_eq!(rec1.key, b"alpha");
        let (rec2, _) = decode_record(&buf[adv1..]).unwrap().unwrap();
        assert_eq!(rec2.key, b"beta");
    }

    #[test]
    fn test_write_encoded() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::open(dir.path(), 64).unwrap();

        let recs = vec![make_record("k1", 10, 1)];
        let (buf, _) = encode_batch(&recs);
        let len = buf.len();
        wal.write_encoded(&buf).unwrap();

        let result = wal.replay_from(0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"k1");
        assert_eq!(len, 8 + 1 + 2 + 2 + 8 + 8 + 4 + 4 + 3);
    }

    #[test]
    fn test_encoded_size() {
        let rec = make_record("hello", 100, 1);
        let size = encoded_size(&rec);
        assert_eq!(size, 8 + 1 + 2 + 5 + 8 + 8 + 4 + 4 + 3);
    }

    #[test]
    fn test_wal_segment_rollover() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::open(dir.path(), 1).unwrap();

        let big_val = vec![0u8; 100_000];
        for i in 0..20 {
            let rec = InternalRecord::from_record(
                &Record {
                    key: format!("key_{:04}", i),
                    ts: i as i64,
                    expire_at: i64::MAX,
                    value: big_val.clone(),
                },
                (i + 1) as u64,
            );
            let (buf, _) = encode_batch(&[rec]);
            wal.write_encoded(&buf).unwrap();
        }

        let result = wal.replay_from(0).unwrap();
        assert_eq!(result.len(), 20);
        assert_eq!(wal.segments.len(), 2);
    }
}
