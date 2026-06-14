use crate::engine::Engine;
use crate::error::{FlowError, Result};
use crate::record::Record;
use crate::stats::StatsCounters;
use std::collections::HashMap;
use std::hash::Hasher;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;

const MAGIC: u8 = 0x54;
const VERSION_V1: u8 = 0x01;
const VERSION_V2: u8 = 0x02;
const TTL_NONE: u32 = 0;

/// Hard cap on key/value lengths to prevent pathological allocations.
const MAX_KEY_BYTES: usize = 4096;
const MAX_VAL_BYTES: usize = 64 * 1024;

/// 8-byte SipHash-based authentication tag for V2 frames.
const AUTH_HASH_BYTES: usize = 8;

/// Compute an 8-byte authentication tag over `count` (2 bytes big-endian)
/// and the record payload. Uses SipHash-1-3 with a fixed seed distinct from
/// the bloom-filter seed.
fn compute_auth_tag(api_key: &str, count: u16, records_payload: &[u8]) -> [u8; AUTH_HASH_BYTES] {
    const SEED_K0: u64 = 0x5550_445f_4155_5448;
    const SEED_K1: u64 = 0x464c_4f57_4442_4b45;

    let mut h = std::hash::DefaultHasher::new();
    h.write_u64(SEED_K0);
    h.write_u64(SEED_K1);
    h.write(api_key.as_bytes());
    h.write(&count.to_be_bytes());
    h.write(records_payload);
    let hash = h.finish();
    hash.to_be_bytes()
}

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
///
/// Frame layout:
///   V1: magic(1) | version=0x01(1) | count(2) | records...
///   V2: magic(1) | version=0x02(1) | count(2) | auth_tag(8) | records...
///
/// When `api_key` is `Some`, V1 frames are rejected and V2 frames must
/// carry a valid auth tag. When `api_key` is `None`, only V1 frames are
/// accepted (backward-compatible with pre-auth clients).
pub fn decode_frame(data: &[u8], api_key: Option<&str>) -> Result<Vec<Record>> {
    if data.len() < 4 {
        return Err(FlowError::Other("frame too short".into()));
    }
    if data[0] != MAGIC {
        return Err(FlowError::Other(format!("invalid magic: {:#x}", data[0])));
    }
    let version = data[1];
    let (raw_count, mut pos) = read_u16(data, 2).unwrap();
    let count = (raw_count as usize).min(1024);

    match (version, api_key) {
        (VERSION_V1, None) => {
            // Unauthenticated path — no key required, no auth tag in frame.
        }
        (VERSION_V1, Some(_)) => {
            return Err(FlowError::Other(
                "authentication required; upgrade client to V2 protocol".into(),
            ));
        }
        (VERSION_V2, Some(key)) => {
            if data.len() < pos + AUTH_HASH_BYTES {
                return Err(FlowError::Other("frame too short for v2 auth tag".into()));
            }
            let received_tag: [u8; AUTH_HASH_BYTES] =
                data[pos..pos + AUTH_HASH_BYTES].try_into().unwrap();
            pos += AUTH_HASH_BYTES;

            let expected_tag = compute_auth_tag(key, raw_count, &data[pos..]);
            if received_tag != expected_tag {
                return Err(FlowError::Other("authentication failed: invalid key hash".into()));
            }
        }
        (VERSION_V2, None) => {
            return Err(FlowError::Other(
                "v2 frame received but server has no api_key configured".into(),
            ));
        }
        _ => {
            return Err(FlowError::Other(format!(
                "unsupported version: {}",
                data[1]
            )));
        }
    }

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
///
/// When `api_key` is `Some`, produces a V2 frame with an 8-byte
/// authentication tag. When `None`, produces a legacy V1 frame.
pub fn encode_frame(records: &[Record], api_key: Option<&str>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 * records.len());
    buf.push(MAGIC);

    let version = if api_key.is_some() {
        VERSION_V2
    } else {
        VERSION_V1
    };
    buf.push(version);

    let count = records.len() as u16;
    buf.extend_from_slice(&count.to_be_bytes());

    // For V2: reserve space for the auth tag and fill the records section.
    // We will compute the tag afterward.
    let auth_pos = if api_key.is_some() { Some(buf.len()) } else { None };
    if let Some(_) = auth_pos {
        buf.extend_from_slice(&[0u8; AUTH_HASH_BYTES]);
    }

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

    // Compute and write the auth tag for V2 frames.
    if let (Some(pos), Some(key)) = (auth_pos, api_key) {
        let tag = compute_auth_tag(key, count, &buf[pos + AUTH_HASH_BYTES..]);
        buf[pos..pos + AUTH_HASH_BYTES].copy_from_slice(&tag);
    }

    buf
}

/// Simple per-IP token bucket for UDP rate limiting. Tokens refill at
/// `rate_per_sec` (max burst = rate_per_sec). `try_consume` returns true
/// if a token was available.
struct TokenBucket {
    tokens: f64,
    last_refill: std::time::Instant,
    rate: f64,
}

impl TokenBucket {
    fn new(rate_per_sec: u32) -> Self {
        Self {
            tokens: rate_per_sec as f64,
            last_refill: std::time::Instant::now(),
            rate: rate_per_sec as f64,
        }
    }

    fn try_consume(&mut self, now: std::time::Instant) -> bool {
        let elapsed = (now - self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.rate);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub async fn start_udp_listener(
    engine: Arc<Engine>,
    stats: Arc<StatsCounters>,
    addr: SocketAddr,
    max_packet_size: usize,
    api_key: Option<String>,
    rate_limit_per_ip: u32,
) -> Result<()> {
    let socket = UdpSocket::bind(addr).await?;
    let mut buf = vec![0u8; max_packet_size];
    let mut rate_limits: HashMap<IpAddr, TokenBucket> = HashMap::new();

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((len, src)) => {
                let now = std::time::Instant::now();

                // Per-IP rate limiting
                if rate_limit_per_ip > 0 {
                    let ip = src.ip();
                    let bucket = rate_limits
                        .entry(ip)
                        .or_insert_with(|| TokenBucket::new(rate_limit_per_ip));
                    if !bucket.try_consume(now) {
                        stats
                            .udp_packets_dropped
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                }

                stats
                    .udp_packets_received
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                match decode_frame(&buf[..len], api_key.as_deref()) {
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
        let encoded = encode_frame(std::slice::from_ref(&rec), None);
        let decoded = decode_frame(&encoded, None).unwrap();
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
        let encoded = encode_frame(&[rec], None);
        let decoded = decode_frame(&encoded, None).unwrap();
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
        let encoded = encode_frame(&recs, None);
        let decoded = decode_frame(&encoded, None).unwrap();
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
        let mut encoded = encode_frame(&[rec], None);
        encoded[0] = 0x00;
        assert!(decode_frame(&encoded, None).is_err());
    }

    #[test]
    fn test_decode_truncated() {
        assert!(decode_frame(&[MAGIC, VERSION_V1], None).is_err());
        assert!(decode_frame(&[MAGIC, VERSION_V1, 0x00, 0x01], None).is_err());
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
