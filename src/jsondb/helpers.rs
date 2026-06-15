use crate::engine::Engine;
use crate::error::{FlowError, Result};
use crate::jsondb::encoding::*;
use crate::jsondb::schema::*;
use crate::record::{InternalRecord, Record};
use serde_json::Value;

// ── internal helpers ──────────────────────────────────────────────

/// Build a batch of `InternalRecord`s for a document put.
pub(crate) fn build_put_batch(
    def: &StoreDef,
    store: &str,
    key_bytes: &[u8],
    doc_bytes: &[u8],
    doc: &Value,
    engine: &Engine,
) -> Result<Vec<InternalRecord>> {
    let mut records = Vec::new();

    // Read old document for index maintenance.
    let old_doc = match engine.get_bytes(&doc_key(store, key_bytes), 0) {
        Some(r) => match decode_doc(&r.value) {
            Ok(doc) => Some(doc),
            Err(e) => {
                return Err(FlowError::JsonDb(format!(
                    "corrupted document at key {:?}: {}",
                    String::from_utf8_lossy(key_bytes),
                    e
                )));
            }
        },
        None => None,
    };

    // Delete old index entries.
    if let Some(ref old_doc_val) = old_doc {
        for idx in &def.indexes {
            let old_values = extract_index_values(old_doc_val, idx);
            for vals in old_values {
                let encoded = encode_composite_value(&vals);
                records.push(InternalRecord::delete(
                    idx_key(store, &idx.name, &encoded, key_bytes),
                    0,
                    0,
                ));
            }
        }
    }

    // Write new document.
    records.push(InternalRecord::from_record(
        &Record::new(doc_key(store, key_bytes), 0, doc_bytes.to_vec()),
        0,
    ));

    // Write new index entries.
    for idx in &def.indexes {
        let new_values = extract_index_values(doc, idx);

        // Unique validation.
        if idx.unique {
            for vals in &new_values {
                let encoded = encode_composite_value(vals);
                let val_pfx = idx_value_prefix(store, &idx.name, &encoded);
                let iter = engine.scan(prefix_range(&val_pfx))?;
                for r in iter {
                    let rec = r?;
                    if rec.value.as_slice() != key_bytes {
                        return Err(FlowError::JsonDb(format!(
                            "unique constraint violation: index '{}' value '{:?}' already exists",
                            idx.name, vals
                        )));
                    }
                }
            }
        }

        for vals in new_values {
            let encoded = encode_composite_value(&vals);
            records.push(InternalRecord::from_record(
                &Record::new(
                    idx_key(store, &idx.name, &encoded, key_bytes),
                    0,
                    key_bytes.to_vec(),
                ),
                0,
            ));
        }
    }

    Ok(records)
}

/// Build a batch of `InternalRecord`s for a document delete.
pub(crate) fn build_delete_batch(
    def: &StoreDef,
    store: &str,
    key_bytes: &[u8],
    engine: &Engine,
) -> Result<Vec<InternalRecord>> {
    let mut records = Vec::new();

    // Read old document for index maintenance.
    let old_doc = match engine.get_bytes(&doc_key(store, key_bytes), 0) {
        Some(r) => match decode_doc(&r.value) {
            Ok(doc) => Some(doc),
            Err(e) => {
                return Err(FlowError::JsonDb(format!(
                    "corrupted document at key {:?}: {}",
                    String::from_utf8_lossy(key_bytes),
                    e
                )));
            }
        },
        None => None,
    };

    // Delete index entries.
    if let Some(ref old_doc_val) = old_doc {
        for idx in &def.indexes {
            let old_values = extract_index_values(old_doc_val, idx);
            for vals in old_values {
                let encoded = encode_composite_value(&vals);
                records.push(InternalRecord::delete(
                    idx_key(store, &idx.name, &encoded, key_bytes),
                    0,
                    0,
                ));
            }
        }
    }

    // Delete document.
    records.push(InternalRecord::delete(doc_key(store, key_bytes), 0, 0));

    Ok(records)
}

/// Extract index values from a document. Returns one entry per "row" in the
/// index (for composite indexes this is one row with all field values; for
/// multi-entry indexes it can be one row per array element).
pub(crate) fn extract_index_values(doc: &Value, idx: &IndexDef) -> Vec<Vec<Value>> {
    // Collect values for each key_path
    let mut raw: Vec<Value> = Vec::with_capacity(idx.key_paths.len());
    for path in &idx.key_paths {
        match extract_field(doc, path) {
            None => return vec![],
            Some(val) => raw.push(val),
        }
    }

    // Multi-entry on single-field index: expand array elements
    if idx.multi_entry
        && idx.key_paths.len() == 1
        && let Value::Array(arr) = &raw[0]
    {
        return arr.iter().map(|v| vec![v.clone()]).collect();
    }

    vec![raw]
}

/// Read the current auto-increment counter and produce the next value
/// together with an `InternalRecord` that must be included in the main
/// write batch so the counter increment is atomic with the document
/// write.
pub(crate) fn prepare_counter(engine: &Engine, store: &str) -> Result<(u64, InternalRecord)> {
    let key = counter_key(store);
    let current = match engine.get_bytes(&key, 0) {
        Some(r) => {
            let arr: [u8; 8] = r.value.as_slice().try_into().map_err(|_| {
                FlowError::JsonDb(format!(
                    "corrupted auto-increment counter for store '{}'",
                    store
                ))
            })?;
            u64::from_be_bytes(arr)
        }
        None => 0,
    };

    let next = current + 1;
    let rec = InternalRecord::from_record(&Record::new(key, 0, next.to_be_bytes().to_vec()), 0);
    Ok((next, rec))
}
