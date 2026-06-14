use crate::engine::Engine;
use crate::error::{FlowError, Result};
use crate::record::Record;
use crate::stats::StatsCounters;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

const MAGIC: u8 = 0x54;
const VERSION: u8 = 0x01;
const TTL_NONE: u32 = 0;

/// Hard cap on key/value lengths to prevent pathological allocations.
const MAX_KEY_BYTES: usize = 4096;
const MAX_VAL_BYTES: usize = 64 * 1024;

/// Reads 2 bytes at `pos` as big-endian u16. Returns `None` if out of bounds.
/// Conversion uses `data[pos..pos+2].try_into().unwrap()` — the bounds check
/// guarantees the slice length matches the array size so the unwrap is safe.
fn read_u16(data: &[u8], pos: usize) -> Option<(u16, usize)> {
    if pos + 2 > data.len() {
        return None;
    }
    Some((
        u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()),
        pos + 2,
    ))
}

/// Reads 4 bytes at `pos` as big-endian u32 with bounds checking.
fn read_u32(data: &[u8], pos: usize) -> Option<(u32, usize)> {
    if pos + 4 > data.len() {
        return None;
    }
    Some((
        u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()),
        pos + 4,
    ))
}

/// Reads 8 bytes at `pos` as big-endian i64 with bounds checking.
fn read_i64(data: &[u8], pos: usize) -> Option<(i64, usize)> {
    if pos + 8 > data.len() {
        return None;
    }
    Some((
        i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap()),
        pos + 8,
    ))
}

/// Decodes a single `Record` from `data` at `pos`. Uses the bounded `read_*`
/// helpers which internally apply `data[pos..pos+N].try_into().unwrap()` after
/// verifying the slice is long enough.
fn read_record(data: &[u8], mut pos: usize) -> Option<(Record, usize)> {
    let (key_len, p) = read_u16(data, pos)?;
    if key_len as usize > MAX_KEY_BYTES {
        return None;
    }
    pos = p;
    if pos + key_len as usize > data.len() {
        return None;
    }
    let key = data[pos..pos + key_len as usize].to_vec();
    pos += key_len as usize;

    let (ts, p) = read_i64(data, pos)?;
    pos = p;

    let (ttl, p) = read_u32(data, pos)?;
    pos = p;

    let (val_len, p) = read_u16(data, pos)?;
    if val_len as usize > MAX_VAL_BYTES {
        return None;
    }
    pos = p;
    if pos + val_len as usize > data.len() {
        return None;
    }
    let value = data[pos..pos + val_len as usize].to_vec();
    pos += val_len as usize;

    let expire_at = if ttl == TTL_NONE {
        i64::MAX
    } else {
        ts + (ttl as i64 * 1_000_000)
    };

    Some((
        Record {
            key,
            ts,
            expire_at,
            value,
        },
        pos,
    ))
}

/// Decodes a UDP frame containing one or more records (big-endian binary format).
/// Frame layout: magic(1) | version(1) | count(2) | records...
pub fn decode_frame(data: &[u8]) -> Result<Vec<Record>> {
    if data.len() < 4 {
        return Err(FlowError::Other("frame too short".into()));
    }
    if data[0] != MAGIC {
        return Err(FlowError::Other(format!("invalid magic: {:#x}", data[0])));
    }
    if data[1] != VERSION {
        return Err(FlowError::Other(format!(
            "unsupported version: {}",
            data[1]
        )));
    }
    let (raw_count, mut pos) = read_u16(data, 2).unwrap();
    // Cap declared record count to prevent amplification attacks.
    // The existing bounds checks in read_record handle actual data truncation.
    let count = (raw_count as usize).min(1024);

    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let (rec, p) =
            read_record(data, pos).ok_or_else(|| FlowError::Other("truncated record".into()))?;
        pos = p;
        records.push(rec);
    }

    Ok(records)
}

/// Encodes records into a UDP frame (big-endian binary format).
/// Each record: key_len(2) | key | ts(8) | ttl(4) | val_len(2) | value
pub fn encode_frame(records: &[Record]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 * records.len());
    buf.push(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&(records.len() as u16).to_be_bytes());

    for rec in records {
        let key_bytes = &rec.key;
        buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(key_bytes);
        buf.extend_from_slice(&rec.ts.to_be_bytes());

        let ttl = if rec.expire_at == i64::MAX {
            TTL_NONE
        } else {
            ((rec.expire_at - rec.ts) / 1_000_000) as u32
        };
        buf.extend_from_slice(&ttl.to_be_bytes());

        buf.extend_from_slice(&(rec.value.len() as u16).to_be_bytes());
        buf.extend_from_slice(&rec.value);
    }

    buf
}

pub async fn start_udp_listener(
    engine: Arc<Engine>,
    stats: Arc<StatsCounters>,
    addr: SocketAddr,
    max_packet_size: usize,
) -> Result<()> {
    let socket = UdpSocket::bind(addr).await?;
    let mut buf = vec![0u8; max_packet_size];

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((len, _src)) => {
                stats
                    .udp_packets_received
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                match decode_frame(&buf[..len]) {
                    Ok(records) => {
                        if let Err(e) = engine.write_batch(&records).await {
                            tracing::warn!("UDP write error: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::debug!("UDP decode error: {}", e);
                        stats
                            .udp_packets_dropped
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("UDP recv error: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_single() {
        let rec = Record {
            key: "test-key".into(),
            ts: 1234567890,
            expire_at: 1234567890 + 3600 * 1_000_000,
            value: b"hello".to_vec(),
        };
        let encoded = encode_frame(std::slice::from_ref(&rec));
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].key, b"test-key");
        assert_eq!(decoded[0].ts, 1234567890);
        assert_eq!(decoded[0].value, b"hello");
        assert!(decoded[0].expire_at < i64::MAX);
    }

    #[test]
    fn test_encode_decode_no_ttl() {
        let rec = Record {
            key: "key".into(),
            ts: 100,
            expire_at: i64::MAX,
            value: b"val".to_vec(),
        };
        let encoded = encode_frame(&[rec]);
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded[0].expire_at, i64::MAX);
    }

    #[test]
    fn test_encode_decode_batch() {
        let recs = vec![
            Record {
                key: "a".into(),
                ts: 100,
                expire_at: i64::MAX,
                value: b"v1".to_vec(),
            },
            Record {
                key: "b".into(),
                ts: 200,
                expire_at: i64::MAX,
                value: b"v2".to_vec(),
            },
        ];
        let encoded = encode_frame(&recs);
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].key, b"a");
        assert_eq!(decoded[1].key, b"b");
    }

    #[test]
    fn test_decode_corrupt_magic() {
        let rec = Record {
            key: "key".into(),
            ts: 100,
            expire_at: i64::MAX,
            value: b"val".to_vec(),
        };
        let mut encoded = encode_frame(&[rec]);
        encoded[0] = 0x00;
        assert!(decode_frame(&encoded).is_err());
    }

    #[test]
    fn test_decode_truncated() {
        assert!(decode_frame(&[MAGIC, VERSION]).is_err());
        assert!(decode_frame(&[MAGIC, VERSION, 0x00, 0x01]).is_err());
    }

    #[test]
    fn test_read_u16_with_position() {
        let data = [0x01, 0x02, 0x03, 0x04];
        let (v, pos) = read_u16(&data, 0).unwrap();
        assert_eq!(v, 0x0102);
        assert_eq!(pos, 2);

        let (v, pos) = read_u16(&data, 2).unwrap();
        assert_eq!(v, 0x0304);
        assert_eq!(pos, 4);
    }

    #[test]
    fn test_read_u16_oob() {
        assert!(read_u16(&[0x01], 0).is_none());
        assert!(read_u16(&[0x01, 0x02], 1).is_none());
    }

    #[test]
    fn test_read_u32_with_position() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (v, pos) = read_u32(&data, 0).unwrap();
        assert_eq!(v, 0x01020304);
        assert_eq!(pos, 4);

        let (v, pos) = read_u32(&data, 4).unwrap();
        assert_eq!(v, 0x05060708);
        assert_eq!(pos, 8);
    }

    #[test]
    fn test_read_u32_oob() {
        assert!(read_u32(&[0; 3], 0).is_none());
        assert!(read_u32(&[0; 4], 1).is_none());
    }

    #[test]
    fn test_read_i64_with_position() {
        let n: i64 = -0x0102030405060708;
        let bytes = n.to_be_bytes();
        let (v, pos) = read_i64(&bytes, 0).unwrap();
        assert_eq!(v, n);
        assert_eq!(pos, 8);
    }

    #[test]
    fn test_read_i64_oob() {
        assert!(read_i64(&[0; 7], 0).is_none());
        assert!(read_i64(&[0; 8], 1).is_none());
    }
}
