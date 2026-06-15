//! Key encoding, value encoding, and JSON field extraction for JsonDB.
//!
//! # Key scheme
//!
//! All JsonDB keys share a single FlowDB keyspace.  A one-byte **prefix**
//! distinguishes the key type, and `\x00` separates components:
//!
//! | Type    | Layout |
//! |---------|--------|
//! | Document | `0x01  store  0x00  primary_key` |
//! | Index    | `0x02  store  0x00  index  0x00  encoded_value  0x00  primary_key` |
//! | Schema   | `0x03  store` |
//! | Counter  | `0x04  store` |
//!
//! Store and index names are validated to be ASCII identifiers so they never
//! contain `\x00`.  Encoded index values may contain `\x00` (e.g. in big-endian
//! numeric encoding) — this is safe because prefix scans operate on raw bytes
//! without interpreting separators.

use crate::error::{FlowError, Result};
use crate::record::{ScanRange, increment_prefix_bytes};
use serde_json::Value;
use std::ops::Bound;

pub(crate) const DOC_PREFIX: u8 = 0x01;
pub(crate) const IDX_PREFIX: u8 = 0x02;
pub(crate) const SCH_PREFIX: u8 = 0x03;
pub(crate) const CTR_PREFIX: u8 = 0x04;
pub(crate) const SEP: u8 = 0x00;

// ── key construction ──────────────────────────────────────────────

pub(crate) fn doc_key(store: &str, key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + store.len() + key.len());
    buf.push(DOC_PREFIX);
    buf.extend_from_slice(store.as_bytes());
    buf.push(SEP);
    buf.extend_from_slice(key);
    buf
}

pub(crate) fn doc_prefix(store: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + store.len());
    buf.push(DOC_PREFIX);
    buf.extend_from_slice(store.as_bytes());
    buf.push(SEP);
    buf
}

pub(crate) fn idx_key(store: &str, index: &str, value: &[u8], key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(3 + store.len() + index.len() + value.len() + key.len());
    buf.push(IDX_PREFIX);
    buf.extend_from_slice(store.as_bytes());
    buf.push(SEP);
    buf.extend_from_slice(index.as_bytes());
    buf.push(SEP);
    buf.extend_from_slice(value);
    buf.push(SEP);
    buf.extend_from_slice(key);
    buf
}

pub(crate) fn idx_prefix(store: &str, index: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(3 + store.len() + index.len());
    buf.push(IDX_PREFIX);
    buf.extend_from_slice(store.as_bytes());
    buf.push(SEP);
    buf.extend_from_slice(index.as_bytes());
    buf.push(SEP);
    buf
}

pub(crate) fn idx_value_prefix(store: &str, index: &str, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + store.len() + index.len() + value.len());
    buf.push(IDX_PREFIX);
    buf.extend_from_slice(store.as_bytes());
    buf.push(SEP);
    buf.extend_from_slice(index.as_bytes());
    buf.push(SEP);
    buf.extend_from_slice(value);
    buf.push(SEP);
    buf
}

pub(crate) fn schema_key(store: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + store.len());
    buf.push(SCH_PREFIX);
    buf.extend_from_slice(store.as_bytes());
    buf
}

pub(crate) fn schema_prefix() -> [u8; 1] {
    [SCH_PREFIX]
}

pub(crate) fn counter_key(store: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + store.len());
    buf.push(CTR_PREFIX);
    buf.extend_from_slice(store.as_bytes());
    buf
}

// ── prefix scan range helper ──────────────────────────────────────

pub(crate) fn prefix_range(prefix: &[u8]) -> ScanRange {
    let end = increment_prefix_bytes(prefix);
    ScanRange {
        key_start: Bound::Included(prefix.to_vec()),
        key_end: Bound::Excluded(end),
        ts_start: Bound::Unbounded,
        ts_end: Bound::Unbounded,
    }
}

// ── index value encoding (sortable) ───────────────────────────────
//
// Each value is prefixed with a type tag so cross-type ordering is
// well-defined: null < false < true < number < string < other.
//
// Numbers use a fixed 9-byte payload (1 sub-type + 8 data bytes) with
// sign-magnitude adjustment so byte-wise ordering matches numeric ordering.

const TAG_NULL: u8 = 0x01;
const TAG_FALSE: u8 = 0x02;
const TAG_TRUE: u8 = 0x03;
const TAG_NUM_I64: u8 = 0x04;
const TAG_NUM_F64: u8 = 0x05;
const TAG_NUM_U64: u8 = 0x06;
const TAG_STRING: u8 = 0x07;
const TAG_OTHER: u8 = 0x08;

pub(crate) fn encode_index_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![TAG_NULL],
        Value::Bool(false) => vec![TAG_FALSE],
        Value::Bool(true) => vec![TAG_TRUE],
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                let mut buf = Vec::with_capacity(9);
                buf.push(TAG_NUM_I64);
                buf.extend_from_slice(&(i as u64 ^ (1u64 << 63)).to_be_bytes());
                buf
            } else if let Some(u) = n.as_u64() {
                let mut buf = Vec::with_capacity(9);
                buf.push(TAG_NUM_U64);
                buf.extend_from_slice(&(u ^ (1u64 << 63)).to_be_bytes());
                buf
            } else if let Some(f) = n.as_f64() {
                let bits = f.to_bits();
                let adjusted = if bits & (1u64 << 63) != 0 {
                    !bits
                } else {
                    bits ^ (1u64 << 63)
                };
                let mut buf = Vec::with_capacity(9);
                buf.push(TAG_NUM_F64);
                buf.extend_from_slice(&adjusted.to_be_bytes());
                buf
            } else {
                vec![TAG_OTHER]
            }
        }
        Value::String(s) => {
            let bytes = s.as_bytes();
            let mut buf = Vec::with_capacity(1 + bytes.len());
            buf.push(TAG_STRING);
            buf.extend_from_slice(bytes);
            buf
        }
        _ => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_OTHER);
            if let Ok(json) = serde_json::to_vec(value) {
                buf.extend_from_slice(&json);
            }
            buf
        }
    }
}

// ── primary key encoding ──────────────────────────────────────────
//
// Primary keys are unique identifiers within a store.  We encode them to
// bytes for use as part of composite keys.  Unlike index values, primary
// keys don't need cross-type sortable ordering — uniqueness is what matters.

pub(crate) fn encode_primary_key(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::String(s) => Ok(s.clone().into_bytes()),
        Value::Number(n) => Ok(n.to_string().into_bytes()),
        Value::Bool(b) => Ok(b.to_string().into_bytes()),
        Value::Null => Ok(vec![0x00]),
        _ => serde_json::to_vec(value)
            .map_err(FlowError::from)
            .map(|mut v| {
                v.insert(0, 0xFF);
                v
            }),
    }
}

// ── JSON helpers ──────────────────────────────────────────────────

pub(crate) fn encode_doc(doc: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(doc).map_err(FlowError::from)
}

pub(crate) fn decode_doc(bytes: &[u8]) -> Result<Value> {
    serde_json::from_slice(bytes).map_err(FlowError::from)
}

/// Extract a field from a JSON document by dotted path.
/// `"email"` → `doc["email"]`
/// `"user.name"` → `doc["user"]["name"]`
/// Returns `None` if any segment is missing.
pub(crate) fn extract_field(doc: &Value, path: &str) -> Option<Value> {
    if path.is_empty() {
        return Some(doc.clone());
    }
    let mut current = doc;
    for segment in path.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(segment)?;
            }
            Value::Array(arr) => {
                let idx: usize = segment.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

/// Validate that a name is a valid store/index identifier.
/// Must be non-empty, ASCII alphanumeric + underscore/dash only.
pub(crate) fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(FlowError::JsonDb("name cannot be empty".into()));
    }
    if name.len() > 255 {
        return Err(FlowError::JsonDb("name too long (max 255 bytes)".into()));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(FlowError::JsonDb(format!(
            "invalid name '{}': only alphanumeric, underscore and dash are allowed",
            name
        )));
    }
    Ok(())
}

// ── composite value encoding (multi-field index keys) ─────────────
//
// Fields are joined with SEP (0x00) so the encoded bytes sort correctly:
// encode(field1) < SEP < encode(field2) ...  preserves field-by-field ordering.

/// Encode a slice of JSON values into a single sortable byte sequence
/// for use in composite (multi-field) index keys.
pub(crate) fn encode_composite_value(values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 16);
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            buf.push(SEP);
        }
        buf.extend_from_slice(&encode_index_value(v));
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_doc_key() {
        let key = doc_key("users", b"u1");
        assert_eq!(&key[..1], &[DOC_PREFIX]);
        assert_eq!(&key[1..6], b"users");
        assert_eq!(key[6], SEP);
        assert_eq!(&key[7..], b"u1");
    }

    #[test]
    fn test_idx_key() {
        let key = idx_key("users", "by_email", b"a@b.com", b"u1");
        assert_eq!(&key[..1], &[IDX_PREFIX]);
    }

    #[test]
    fn test_encode_index_value_ordering() {
        let values = vec![
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            json!(-100),
            json!(-1),
            json!(0),
            json!(1),
            json!(42),
            json!(100),
            json!("a"),
            json!("b"),
            json!("hello"),
        ];
        let encoded: Vec<Vec<u8>> = values.iter().map(encode_index_value).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] <= encoded[i + 1],
                "ordering violation at index {}: {:?} > {:?}",
                i,
                encoded[i],
                encoded[i + 1]
            );
        }
    }

    #[test]
    fn test_encode_index_value_float_ordering() {
        let values = [json!(-3.14),
            json!(-1.0),
            json!(0.0),
            json!(0.5),
            json!(1.0),
            json!(3.14)];
        let encoded: Vec<Vec<u8>> = values.iter().map(encode_index_value).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "float ordering violation at index {}",
                i
            );
        }
    }

    #[test]
    fn test_encode_primary_key() {
        assert_eq!(encode_primary_key(&json!("hello")).unwrap(), b"hello");
        assert_eq!(encode_primary_key(&json!(42)).unwrap(), b"42");
        assert_eq!(encode_primary_key(&json!(true)).unwrap(), b"true");
    }

    #[test]
    fn test_extract_field() {
        let doc = json!({"user": {"name": "Alice", "tags": ["a", "b"]}});
        assert_eq!(extract_field(&doc, "user.name"), Some(json!("Alice")));
        assert_eq!(extract_field(&doc, "user.tags.0"), Some(json!("a")));
        assert_eq!(extract_field(&doc, "user.tags.1"), Some(json!("b")));
        assert_eq!(extract_field(&doc, "missing"), None);
        assert_eq!(extract_field(&doc, "user.missing"), None);
        assert_eq!(extract_field(&doc, ""), Some(doc.clone()));
    }

    #[test]
    fn test_validate_name() {
        assert!(validate_name("users").is_ok());
        assert!(validate_name("by_email").is_ok());
        assert!(validate_name("store-1").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("has space").is_err());
        assert!(validate_name("has\x00null").is_err());
    }

    #[test]
    fn test_encode_decode_doc() {
        let doc = json!({"id": 1, "name": "Alice"});
        let bytes = encode_doc(&doc).unwrap();
        let back = decode_doc(&bytes).unwrap();
        assert_eq!(doc, back);
    }
}
