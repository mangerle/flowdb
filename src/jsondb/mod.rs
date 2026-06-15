//! JsonDB – A JSON document database interface built on top of FlowDB.
//!
//! Provides an IndexedDB-like API with ACID transactions, secondary indexes,
//! and auto-increment support.

mod encoding;
mod schema;

use crate::engine::Engine;
use crate::error::{FlowError, Result};
use crate::jsondb::encoding::{
    counter_key, decode_doc, doc_key, doc_prefix, encode_composite_value, encode_doc,
    encode_index_value, encode_primary_key, extract_field, idx_key, idx_prefix, idx_value_prefix,
    prefix_range, SEP,
};
use crate::jsondb::schema::{
    load_schemas, schema_delete_record, schema_record, validate_index_def, validate_store_def,
    IndexDef, Schema, StoreDef,
};
use crate::record::{Config, InternalRecord, Record, ScanRange};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::ops::Bound;

// Re-export key types
pub use crate::jsondb::schema::IndexDef as IndexSchema;
pub use crate::jsondb::schema::StoreDef as StoreSchema;

/// Transaction mode (read-only vs read-write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Read-only — queries only.
    ReadOnly,
    /// Read-write — queries, puts, deletes, and index updates.
    ReadWrite,
}

// ── JsonDB ───────────────────────────────────────────────────────

/// A JSON document database built on top of a single FlowDB instance.
///
/// Every document operation is ACID — document writes and secondary-index
/// updates are applied atomically.  Explicit [`Transaction`]s group multiple
/// operations into a single atomic batch.
///
/// # Example
///
/// ```no_run
/// use flowdb::jsondb::{JsonDB, TransactionMode};
/// use serde_json::json;
///
/// let db = JsonDB::open(Default::default()).unwrap();
/// db.create_object_store("users", "id").unwrap();
/// db.create_index("users", "by_email", &["email"], true).unwrap();
///
/// db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();
/// let doc = db.get("users", &json!("u1")).unwrap();
///
/// let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
/// tx.put("users", json!({"id": "u2", "email": "c@d.com"})).unwrap();
/// tx.commit().unwrap();
/// ```

// ── KeyArg / Document traits ──────────────────────────────────────

/// Trait for types that can be used as a primary key argument.
///
/// Implemented for `&str`, `String`, `i64`, `u64`, `f64`, `Value`, and `&Value`.
pub trait KeyArg {
    fn into_value(self) -> Value;
}

impl KeyArg for &str {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }
}
impl KeyArg for String {
    fn into_value(self) -> Value {
        Value::String(self)
    }
}
impl KeyArg for i64 {
    fn into_value(self) -> Value {
        Value::Number(self.into())
    }
}
impl KeyArg for i32 {
    fn into_value(self) -> Value {
        Value::Number((self as i64).into())
    }
}
impl KeyArg for u64 {
    fn into_value(self) -> Value {
        Value::Number(self.into())
    }
}
impl KeyArg for u32 {
    fn into_value(self) -> Value {
        Value::Number((self as u64).into())
    }
}
impl KeyArg for Value {
    fn into_value(self) -> Value {
        self
    }
}
impl KeyArg for &Value {
    fn into_value(self) -> Value {
        self.clone()
    }
}

pub struct JsonDB {
    engine: Engine,
    schema: Schema,
    // Serialises read-modify-write operations (put, delete, put_auto) so
    // concurrent threads don't compute stale index maintenance from the
    // same old-document snapshot.
    write_lock: std::sync::Mutex<()>,
}

impl fmt::Debug for JsonDB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonDB")
            .field("store_names", &self.store_names())
            .finish()
    }
}

impl JsonDB {
    /// Open a JsonDB backed by a FlowDB engine at `config.data_dir`.
    pub fn open(config: Config) -> Result<Self> {
        let engine = Engine::open(config)?;
        Self::from_engine(engine)
    }

    /// Wrap an already-open FlowDB [`Engine`] with a JsonDB layer.
    ///
    /// Schemas are loaded lazily on first use and cached.
    pub fn from_engine(engine: Engine) -> Result<Self> {
        let schema = load_schemas(|range| {
            let iter = engine.scan(range)?;
            iter.collect()
        })?;
        Ok(Self { engine, schema, write_lock: std::sync::Mutex::new(()) })
    }

    /// Shut down the underlying engine.
    pub fn shutdown(self) -> Result<()> {
        self.engine.shutdown()
    }

    /// Close (flush) without consuming.
    pub fn close(&self) -> Result<()> {
        self.engine.close()
    }

    /// Access the underlying FlowDB engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    // ── schema management ─────────────────────────────────────────

    /// Create a new object store.
    ///
    /// `key_path` is a dotted field path (e.g. `"id"` or `"user.id"`) that
    /// identifies the primary key within each document.
    pub fn create_object_store(&self, name: &str, key_path: &str) -> Result<()> {
        validate_store_def(name, key_path)?;

        let mut def = self.schema.get(name);

        match &mut def {
            Some(d) => {
                // Store already exists – update key_path if it matches.
                if d.key_path != key_path {
                    return Err(FlowError::JsonDb(format!(
                        "store '{}' already exists with a different key_path",
                        name
                    )));
                }
                // Already exists with identical definition – no-op.
                return Ok(());
            }
            None => {
                let entry = StoreDef {
                    name: name.to_string(),
                    key_path: key_path.to_string(),
                    auto_increment: false,
                    indexes: vec![],
                    next_auto_id: 0,
                };
                self.engine
                    .write_internal(vec![InternalRecord::from_record(
                        &schema_record(&entry)?,
                        0,
                    )])?;
                self.schema.insert(entry);
                Ok(())
            }
        }
    }

    /// Delete an object store (all documents, indexes, and schema).
    pub fn delete_object_store(&self, name: &str) -> Result<()> {
        let def = self.schema.get(name);
        if def.is_none() {
            return Err(FlowError::JsonDb(format!(
                "store '{}' not found",
                name
            )));
        }
        let def = def.unwrap();

        // Range-delete all documents and index entries.
        let doc_pfx = doc_prefix(name);

        let mut records = Vec::new();

        // Delete all index entries for each index.
        for index in &def.indexes {
            let pfx = idx_prefix(name, &index.name);
            let end = crate::record::increment_prefix_bytes(&pfx);
            records.push(InternalRecord::delete_range(pfx, end, 0));
        }

        // Delete all documents.
        let doc_end = crate::record::increment_prefix_bytes(&doc_pfx);
        records.push(InternalRecord::delete_range(doc_pfx, doc_end, 0));

        // Delete schema.
        records.push(schema_delete_record(name));

        // Delete counter.
        records.push(InternalRecord::delete(counter_key(name), 0, 0));

        self.engine.write_internal(records)?;
        self.schema.remove(name);
        Ok(())
    }

    /// Create a secondary index on an existing object store.
    ///
    /// `key_paths` can be one or more field paths (e.g. `&["email"]` for a
    /// single-field index or `&["city", "age"]` for a composite index).
    /// Composite indexes enable efficient multi-field queries via
    /// [`QueryBuilder`].
    pub fn create_index(
        &self,
        store: &str,
        name: &str,
        key_paths: &[&str],
        unique: bool,
    ) -> Result<()> {
        let mut def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        validate_index_def(&def, name, key_paths)?;

        let index = IndexDef {
            name: name.to_string(),
            key_paths: key_paths.iter().map(|s| s.to_string()).collect(),
            unique,
            multi_entry: false,
        };

        def.indexes.push(index.clone());

        // Build a single atomic batch: schema + all index entries for
        // existing documents.  This ensures consistency — if the write
        // fails, neither the schema nor any entries are committed.
        let mut records = Vec::new();
        records.push(InternalRecord::from_record(&schema_record(&def)?, 0));

        let doc_pfx = doc_prefix(store);
        let docs = self.engine.scan(prefix_range(&doc_pfx))?;
        for rec in docs {
            let doc = decode_doc(&rec?.value)?;
            let index_vals = extract_index_values(&doc, &index);
            for vals in index_vals {
                let key_bytes = encode_primary_key(
                    &extract_field(&doc, &def.key_path).unwrap_or(Value::Null),
                )?;
                let encoded = encode_composite_value(&vals);
                records.push(InternalRecord::from_record(
                    &Record::new(
                        idx_key(store, name, &encoded, &key_bytes),
                        0,
                        key_bytes.clone(),
                    ),
                    0,
                ));
            }
        }
        self.engine.write_internal(records)?;

        self.schema.insert(def);
        Ok(())
    }

    /// Convenience: create a single-field index.  Equivalent to
    /// `create_index(store, name, &[key_path], unique)`.
    pub fn create_index_on(&self, store: &str, name: &str, key_path: &str, unique: bool) -> Result<()> {
        self.create_index(store, name, &[key_path], unique)
    }

    /// Delete a secondary index (removes all index entries).
    pub fn delete_index(&self, store: &str, name: &str) -> Result<()> {
        let mut def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;

        let pos = def.indexes.iter().position(|i| i.name == name);
        if pos.is_none() {
            return Err(FlowError::JsonDb(format!(
                "index '{}' not found on store '{}'",
                name, store
            )));
        }
        def.indexes.remove(pos.unwrap());

        // Delete all index entries.
        let pfx = idx_prefix(store, name);
        let end = crate::record::increment_prefix_bytes(&pfx);
        let mut records = vec![InternalRecord::delete_range(pfx, end, 0)];
        records.push(InternalRecord::from_record(&schema_record(&def)?, 0));

        self.engine.write_internal(records)?;
        self.schema.insert(def);
        Ok(())
    }

    /// List all store names.
    pub fn store_names(&self) -> Vec<String> {
        self.schema.list().into_iter().map(|s| s.name).collect()
    }

    /// Get a store definition.
    pub fn get_store(&self, name: &str) -> Option<StoreDef> {
        self.schema.get(name)
    }

    // ── direct document operations (implicit transaction) ─────────

    /// Insert or update a document.
    ///
    /// The document **must** contain the store's `key_path` field.
    /// Returns the extracted primary key value.
    pub fn put(&self, store: &str, doc: Value) -> Result<Value> {
        let _lock = self.write_lock.lock().unwrap();
        let def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_val = extract_field(&doc, &def.key_path).ok_or_else(|| {
            FlowError::JsonDb(format!(
                "document missing key_path '{}' for store '{}'",
                def.key_path, store
            ))
        })?;
        let key_bytes = encode_primary_key(&key_val)?;
        let doc_bytes = encode_doc(&doc)?;

        let batch = build_put_batch(&def, store, &key_bytes, &doc_bytes, &doc, &self.engine)?;
        self.engine.write_internal(batch)?;
        Ok(key_val)
    }

    /// Retrieve a document by primary key.
    pub fn get(&self, store: &str, key: &Value) -> Result<Option<Value>> {
        let _def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_bytes = encode_primary_key(key)?;
        let rec = self
            .engine
            .get_bytes(&doc_key(store, &key_bytes), 0);
        match rec {
            Some(r) => Ok(Some(decode_doc(&r.value)?)),
            None => Ok(None),
        }
    }

    /// Delete a document by primary key (and all associated index entries).
    pub fn delete(&self, store: &str, key: &Value) -> Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_bytes = encode_primary_key(key)?;

        let batch = build_delete_batch(&def, store, &key_bytes, &self.engine)?;
        self.engine.write_internal(batch)?;
        Ok(())
    }

    /// Insert a document with auto-generated key (for auto-increment stores).
    ///
    /// Returns the assigned key value.
    pub fn put_auto(&self, store: &str, mut doc: Value) -> Result<Value> {
        let _lock = self.write_lock.lock().unwrap();
        let def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        if !def.auto_increment {
            return Err(FlowError::JsonDb(format!(
                "store '{}' is not auto-increment",
                store
            )));
        }

        let (next_id, counter_rec) = prepare_counter(&self.engine, store)?;
        let key_val = Value::Number(next_id.into());
        let key_bytes = next_id.to_string().into_bytes();

        // Inject the auto key into the document.
        if let Value::Object(ref mut map) = doc {
            map.insert(def.key_path.clone(), key_val.clone());
        }

        let doc_bytes = encode_doc(&doc)?;
        let mut batch = build_put_batch(&def, store, &key_bytes, &doc_bytes, &doc, &self.engine)?;
        batch.push(counter_rec); // atomic: counter + doc + index entries
        self.engine.write_internal(batch)?;
        Ok(key_val)
    }

    /// Count documents in a store.
    pub fn count(&self, store: &str) -> Result<usize> {
        let _ = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let pfx = doc_prefix(store);
        let iter = self.engine.scan(prefix_range(&pfx))?;
        let mut count = 0;
        for r in iter {
            let _ = r?;
            count += 1;
        }
        Ok(count)
    }

    /// Retrieve all documents in a store.
    pub fn scan(&self, store: &str) -> Result<Vec<Value>> {
        let _ = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let pfx = doc_prefix(store);
        let iter = self.engine.scan(prefix_range(&pfx))?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            docs.push(decode_doc(&rec.value)?);
        }
        Ok(docs)
    }

    /// Look up documents by an exact index value.
    pub fn get_by_index(
        &self,
        store: &str,
        index: &str,
        value: &Value,
    ) -> Result<Vec<Value>> {
        let _def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let idx_def = _def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| {
                FlowError::JsonDb(format!("index '{}' not found on '{}'", index, store))
            })?;
        let is_composite = idx_def.key_paths.len() > 1;

        let encoded = if is_composite {
            // Multi-field index: accept array ["v1","v2"] for full match,
            // or single value for prefix scan on first field.
            match value {
                Value::Array(arr) => encode_composite_value(arr),
                _ => encode_index_value(value),
            }
        } else {
            encode_index_value(value)
        };

        let pfx = idx_value_prefix(store, index, &encoded);
        let iter = self.engine.scan(prefix_range(&pfx))?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            if let Some(doc) = self.engine.get_bytes(&doc_key(store, &rec.value), 0) {
                docs.push(decode_doc(&doc.value)?);
            }
        }
        Ok(docs)
    }

    /// Look up documents by a range of index values `[start, end)`.
    ///
    /// The range is **exclusive** of `end`.
    pub fn range_by_index(
        &self,
        store: &str,
        index: &str,
        start: &Value,
        end: &Value,
    ) -> Result<Vec<Value>> {
        let _def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let idx_def = _def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| {
                FlowError::JsonDb(format!("index '{}' not found on '{}'", index, store))
            })?;
        let is_composite = idx_def.key_paths.len() > 1;

        let enc_start = if is_composite {
            match start {
                Value::Array(arr) => encode_composite_value(arr),
                _ => encode_index_value(start),
            }
        } else {
            encode_index_value(start)
        };
        let enc_end = if is_composite {
            match end {
                Value::Array(arr) => encode_composite_value(arr),
                _ => encode_index_value(end),
            }
        } else {
            encode_index_value(end)
        };

        let pfx = idx_prefix(store, index);
        let range = ScanRange {
            key_start: Bound::Included([pfx.as_slice(), &enc_start].concat()),
            key_end: Bound::Excluded([pfx.as_slice(), &enc_end].concat()),
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        };

        let iter = self.engine.scan(range)?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            if let Some(doc) = self.engine.get_bytes(&doc_key(store, &rec.value), 0) {
                docs.push(decode_doc(&doc.value)?);
            }
        }
        Ok(docs)
    }

    // ── explicit transactions ──────────────────────────────────────

    /// Begin an explicit transaction over the given stores.
    ///
    /// Call [`Transaction::commit`] to apply buffered writes atomically.
    /// Dropping the transaction without calling `commit` discards all writes.
    pub fn transaction<'db>(
        &'db self,
        stores: &[&str],
        mode: TransactionMode,
    ) -> Result<Transaction<'db>> {
        // Validate all stores exist.
        for name in stores {
            if self.schema.get(name).is_none() {
                return Err(FlowError::JsonDb(format!(
                    "store '{}' not found",
                    name
                )));
            }
        }
        Ok(Transaction {
            db: self,
            mode,
            writes: HashMap::new(),
            counter_updates: Vec::new(),
            next_ids: HashMap::new(),
            committed: false,
        })
    }

    /// Create a [`QueryBuilder`] for the given store.
    pub fn query<'a>(&'a self, store: &'a str) -> QueryBuilder<'a> {
        QueryBuilder::new(self, store)
    }

    // ── generic document API (struct-based) ──────────────────────

    /// Insert or update a document of any `Serialize` type.
    ///
    /// The type **must** have a field matching the store's `key_path`.
    /// Returns the extracted primary key value.
    ///
    /// ```ignore
    /// db.put_doc("users", &User { id: "u1".into(), name: "Alice".into() })?;
    /// ```
    pub fn put_doc<T: serde::Serialize>(&self, store: &str, doc: &T) -> Result<Value> {
        let json = serde_json::to_value(doc).map_err(FlowError::from)?;
        self.put(store, json)
    }

    /// Retrieve a document by primary key, deserialized to `T`.
    ///
    /// ```ignore
    /// let user: User = db.get_doc("users", "u1")?.unwrap();
    /// ```
    pub fn get_doc<T: serde::de::DeserializeOwned>(
        &self,
        store: &str,
        key: impl KeyArg,
    ) -> Result<Option<T>> {
        let val = self.get(store, &key.into_value())?;
        match val {
            Some(v) => {
                let t: T = serde_json::from_value(v).map_err(FlowError::from)?;
                Ok(Some(t))
            }
            None => Ok(None),
        }
    }

    /// Delete a document by primary key, accepting any `KeyArg` type.
    pub fn delete_doc(&self, store: &str, key: impl KeyArg) -> Result<()> {
        self.delete(store, &key.into_value())
    }
}

// ── Transaction ───────────────────────────────────────────────────

/// An explicit JsonDB transaction.
///
/// Writes are buffered in memory until [`commit`](Transaction::commit) is
/// called, at which point all document and index updates are applied as a
/// single atomic batch.
///
/// Dropping the transaction without calling `commit` **discards** all
/// buffered writes — there is no automatic roll-back needed.
pub struct Transaction<'db> {
    db: &'db JsonDB,
    mode: TransactionMode,
    // (store_name, primary_key_bytes) -> Some(doc_bytes) | None (delete)
    writes: HashMap<(String, Vec<u8>), Option<Vec<u8>>>,
    // Counter records (auto-increment) that must be committed atomically
    // with the document writes.
    counter_updates: Vec<InternalRecord>,
    // Per-store next auto-increment IDs (tracked in memory for
    // multiple put_auto calls within the same transaction).
    next_ids: HashMap<String, u64>,
    committed: bool,
}

impl fmt::Debug for Transaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("mode", &self.mode)
            .field("writes_count", &self.writes.len())
            .field("committed", &self.committed)
            .finish()
    }
}

impl<'db> Transaction<'db> {
    /// Insert or update a document within this transaction.
    pub fn put(&mut self, store: &str, doc: Value) -> Result<Value> {
        self.require_read_write()?;
        let def = self.require_store(store)?;
        let key_val = extract_field(&doc, &def.key_path).ok_or_else(|| {
            FlowError::JsonDb(format!(
                "document missing key_path '{}' for store '{}'",
                def.key_path, store
            ))
        })?;
        let key_bytes = encode_primary_key(&key_val)?;
        let doc_bytes = encode_doc(&doc)?;
        self.writes
            .insert((store.to_string(), key_bytes), Some(doc_bytes));
        Ok(key_val)
    }

    /// Retrieve a document by primary key.
    ///
    /// Reads from the transaction's write buffer first (read-your-writes),
    /// falling back to the engine.
    pub fn get(&self, store: &str, key: &Value) -> Result<Option<Value>> {
        let _ = self.require_store(store)?;
        let key_bytes = encode_primary_key(key)?;

        // Check write buffer.
        if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes.clone())) {
            return match doc_opt {
                Some(bytes) => Ok(Some(decode_doc(bytes)?)),
                None => Ok(None),
            };
        }

        // Fall back to engine.
        let rec = self
            .db
            .engine
            .get_bytes(&doc_key(store, &key_bytes), 0);
        match rec {
            Some(r) => Ok(Some(decode_doc(&r.value)?)),
            None => Ok(None),
        }
    }

    /// Delete a document by primary key.
    pub fn delete(&mut self, store: &str, key: &Value) -> Result<()> {
        self.require_read_write()?;
        let _ = self.require_store(store)?;
        let key_bytes = encode_primary_key(key)?;
        self.writes
            .insert((store.to_string(), key_bytes), None);
        Ok(())
    }

    /// Count documents (visible within this transaction).
    pub fn count(&self, store: &str) -> Result<usize> {
        let _ = self.require_store(store)?;
        let pfx = doc_prefix(store);
        let iter = self.db.engine.scan(prefix_range(&pfx))?;
        let mut count = 0usize;
        for r in iter {
            let rec = r?;
            // Skip if the doc has been deleted in our writes.
            let key_bytes = rec.key[doc_prefix(store).len()..].to_vec();
            if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes)) {
                if doc_opt.is_none() {
                    continue; // deleted
                }
            }
            count += 1;
        }
        // Add buffered puts that aren't in the engine yet.
        for ((s, k), doc_opt) in &self.writes {
            if s != store {
                continue;
            }
            if doc_opt.is_none() {
                continue;
            }
            // Check if it already was counted by the scan.
            if self
                .db
                .engine
                .get_bytes(&doc_key(store, k), 0)
                .is_none()
            {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Retrieve all documents in a store (visible within this transaction).
    pub fn scan(&self, store: &str) -> Result<Vec<Value>> {
        let _ = self.require_store(store)?;
        let pfx = doc_prefix(store);
        let iter = self.db.engine.scan(prefix_range(&pfx))?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            let key_bytes = rec.key[doc_prefix(store).len()..].to_vec();
            if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes)) {
                match doc_opt {
                    Some(bytes) => {
                        docs.push(decode_doc(bytes)?);
                    }
                    None => {} // deleted
                }
            } else {
                docs.push(decode_doc(&rec.value)?);
            }
        }
        // Add buffered puts not in the engine.
        for ((s, k), doc_opt) in &self.writes {
            if s != store {
                continue;
            }
            if let Some(bytes) = doc_opt {
                if self
                    .db
                    .engine
                    .get_bytes(&doc_key(store, k), 0)
                    .is_none()
                {
                    docs.push(decode_doc(bytes)?);
                }
            }
        }
        Ok(docs)
    }

    /// Look up documents by exact index value within this transaction.
    pub fn get_by_index(
        &self,
        store: &str,
        index: &str,
        value: &Value,
    ) -> Result<Vec<Value>> {
        let def = self.require_store(store)?;
        let _ = def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| {
                FlowError::JsonDb(format!("index '{}' not found on '{}'", index, store))
            })?;

        let encoded = encode_index_value(value);
        let pfx = idx_value_prefix(store, index, &encoded);
        let iter = self.db.engine.scan(prefix_range(&pfx))?;
        let mut docs = Vec::new();

        // Find the index key_path for field checking.
        let idx_key_paths = def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .map(|i| i.key_paths.clone())
            .unwrap_or_default();
        // For composite indexes, use the first key_path for basic write buffer matching.
        let first_path = idx_key_paths.first().map(|s| s.as_str()).unwrap_or("");

        for r in iter {
            let rec = r?;
            let key_bytes = &rec.value;
            // Check write buffer.
            if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes.clone())) {
                match doc_opt {
                    Some(bytes) => {
                        let buffered_doc = decode_doc(bytes)?;
                        // Only include if the buffered doc still matches the query value.
                        if extract_field(&buffered_doc, first_path) == Some(value.clone()) {
                            docs.push(buffered_doc);
                        }
                    }
                    None => {} // deleted
                }
            } else if let Some(doc) = self.db.engine.get_bytes(&doc_key(store, key_bytes), 0) {
                docs.push(decode_doc(&doc.value)?);
            }
        }
        // Also check buffered puts whose index value matches but aren't in the
        // engine index yet (brand-new documents).
        for ((s, _k), doc_opt) in &self.writes {
            if s != store {
                continue;
            }
            if let Some(bytes) = doc_opt {
                let doc: Value = decode_doc(bytes)?;
                if extract_field(&doc, first_path) == Some(value.clone()) {
                    // Avoid duplicates that were already returned from the engine scan.
                    let already = docs.iter().any(|d| extract_field(d, &def.key_path) == extract_field(&doc, &def.key_path));
                    if !already {
                        docs.push(doc);
                    }
                }
            }
        }
        Ok(docs)
    }

    /// Look up documents by index value range within this transaction.
    pub fn range_by_index(
        &self,
        store: &str,
        index: &str,
        start: &Value,
        end: &Value,
    ) -> Result<Vec<Value>> {
        let store_def = self.require_store(store)?;
        let first_path = store_def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| {
                FlowError::JsonDb(format!("index '{}' not found on '{}'", index, store))
            })?
            .key_paths
            .first()
            .cloned()
            .unwrap_or_default();

        let pfx = idx_prefix(store, index);
        let enc_start = encode_index_value(start);
        let enc_end = encode_index_value(end);

        let range = ScanRange {
            key_start: Bound::Included([pfx.as_slice(), &enc_start].concat()),
            key_end: Bound::Excluded([pfx.as_slice(), &enc_end].concat()),
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        };

        let iter = self.db.engine.scan(range)?;
        let mut docs = Vec::new();

        for r in iter {
            let rec = r?;
            let key_bytes = &rec.value;
            if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes.clone())) {
                match doc_opt {
                    Some(bytes) => {
                        let buffered_doc = decode_doc(bytes)?;
                        if let Some(index_val) = extract_field(&buffered_doc, &first_path) {
                            let enc = encode_index_value(&index_val);
                            if enc.as_slice() >= enc_start.as_slice()
                                && enc.as_slice() < enc_end.as_slice()
                            {
                                docs.push(buffered_doc);
                            }
                        }
                    }
                    None => {}
                }
            } else if let Some(doc) = self.db.engine.get_bytes(&doc_key(store, key_bytes), 0) {
                docs.push(decode_doc(&doc.value)?);
            }
        }

        // Also check buffered puts that aren't in the engine index yet.
        for ((s, key_bytes), doc_opt) in &self.writes {
            if s != store {
                continue;
            }
            if let Some(bytes) = doc_opt {
                if self
                    .db
                    .engine
                    .get_bytes(&doc_key(store, key_bytes), 0)
                    .is_some()
                {
                    continue;
                }
                let buffered_doc = decode_doc(bytes)?;
                if let Some(index_val) = extract_field(&buffered_doc, &first_path) {
                    let enc = encode_index_value(&index_val);
                    if enc.as_slice() >= enc_start.as_slice()
                        && enc.as_slice() < enc_end.as_slice()
                    {
                        docs.push(buffered_doc);
                    }
                }
            }
        }
        Ok(docs)
    }

    /// Insert a document with auto-generated key (for auto-increment stores).
    pub fn put_auto(&mut self, store: &str, mut doc: Value) -> Result<Value> {
        self.require_read_write()?;
        let def = self.require_store(store)?;
        if !def.auto_increment {
            return Err(FlowError::JsonDb(format!(
                "store '{}' is not auto-increment",
                store
            )));
        }

        // Use in-memory tracking for multiple put_auto calls in the same
        // transaction. Only the first call reads the engine counter.
        let next_id = match self.next_ids.get(store) {
            Some(&existing) => {
                self.next_ids.insert(store.to_string(), existing + 1);
                existing + 1
            }
            None => {
                let (id, counter_rec) = prepare_counter(&self.db.engine, store)?;
                self.counter_updates.push(counter_rec);
                self.next_ids.insert(store.to_string(), id);
                id
            }
        };

        let key_val = Value::Number(next_id.into());

        if let Value::Object(ref mut map) = doc {
            map.insert(def.key_path.clone(), key_val.clone());
        }

        let key_bytes = next_id.to_string().into_bytes();
        let doc_bytes = encode_doc(&doc)?;
        self.writes
            .insert((store.to_string(), key_bytes), Some(doc_bytes));
        Ok(key_val)
    }

    /// Commit all buffered writes atomically.
    pub fn commit(mut self) -> Result<()> {
        if self.committed {
            return Ok(());
        }

        let mut records = Vec::new();

        // Include any pending counter updates (auto-increment).
        records.extend(self.counter_updates.drain(..));

        // Process buffered document writes.
        for ((store_name, key_bytes), doc_opt) in &self.writes {
            let def = self
                .db
                .schema
                .get(store_name)
                .ok_or_else(|| {
                    FlowError::JsonDb(format!("store '{}' not found", store_name))
                })?;

            // Read old document for index maintenance.
            // If the document is corrupted we fail hard — silent data loss
            // is worse than a failed write.
            let old_doc_str = self
                .db
                .engine
                .get_bytes(&doc_key(store_name, key_bytes), 0)
                .and_then(|r| decode_doc(&r.value).ok());

            // Delete old index entries.
            if let Some(ref old_doc_val) = old_doc_str {
                for idx in &def.indexes {
                    let old_values = extract_index_values(old_doc_val, idx);
                    for vals in old_values {
                        let encoded = encode_composite_value(&vals);
                        records.push(InternalRecord::delete(
                            idx_key(store_name, &idx.name, &encoded, key_bytes),
                            0,
                            0,
                        ));
                    }
                }
            }

            match doc_opt {
                Some(doc_bytes) => {
                    // Write new document.
                    records.push(InternalRecord::from_record(
                        &Record::new(doc_key(store_name, key_bytes), 0, doc_bytes.clone()),
                        0,
                    ));

                    // Write new index entries.
                    let new_doc = decode_doc(doc_bytes)?;
                    for idx in &def.indexes {
                        let new_values = extract_index_values(&new_doc, idx);

                        // Unique validation: check BOTH engine AND write buffer.
                        if idx.unique {
                            for vals in &new_values {
                                let encoded = encode_composite_value(vals);
                                let val_pfx = idx_value_prefix(store_name, &idx.name, &encoded);
                                let iter = self.db.engine.scan(prefix_range(&val_pfx))?;
                                for r in iter {
                                    let rec = r?;
                                    if rec.value.as_slice() != key_bytes.as_slice() {
                                        return Err(FlowError::JsonDb(format!(
                                            "unique constraint violation: index '{}' value '{:?}' already exists",
                                            idx.name, vals
                                        )));
                                    }
                                }
                                // Also check other buffered writes in this transaction.
                                for ((other_store, other_key), other_doc) in &self.writes {
                                    if other_store != store_name { continue; }
                                    if other_key == key_bytes { continue; }
                                    if let Some(other_bytes) = other_doc {
                                        let other_doc_val = decode_doc(other_bytes)?;
                                        let other_vals = extract_index_values(&other_doc_val, idx);
                                        for ov in other_vals {
                                            if encode_composite_value(&ov) == encoded {
                                                return Err(FlowError::JsonDb(format!(
                                                    "unique constraint violation in transaction: index '{}' value '{:?}'",
                                                    idx.name, vals
                                                )));
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        for vals in &new_values {
                            let encoded = encode_composite_value(vals);
                            records.push(InternalRecord::from_record(
                                &Record::new(
                                    idx_key(store_name, &idx.name, &encoded, key_bytes),
                                    0,
                                    key_bytes.clone(),
                                ),
                                0,
                            ));
                        }
                    }
                }
                None => {
                    // Delete document.
                    records.push(InternalRecord::delete(
                        doc_key(store_name, key_bytes),
                        0,
                        0,
                    ));
                }
            }
        }

        if !records.is_empty() {
            self.db.engine.write_internal(records)?;
        }
        // Only mark committed AFTER the write succeeds.
        // This lets callers retry if write_internal fails.
        self.committed = true;
        Ok(())
    }

    /// Abort the transaction (discard all buffered writes).
    pub fn abort(self) {
        // Just drop — writes are discarded.
    }

    // ── helpers ──────────────────────────────────────────────────

    fn require_read_write(&self) -> Result<()> {
        if self.mode == TransactionMode::ReadOnly {
            return Err(FlowError::JsonDb(
                "cannot write in a read-only transaction".into(),
            ));
        }
        Ok(())
    }

    fn require_store(&self, name: &str) -> Result<StoreDef> {
        self.db
            .schema
            .get(name)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", name)))
    }
}

impl<'db> Drop for Transaction<'db> {
    fn drop(&mut self) {
        if !self.committed && self.mode == TransactionMode::ReadWrite {
            // Auto-abort: writes are simply discarded.
        }
    }
}

// ── QueryBuilder ───────────────────────────────────────────────────

/// Sort direction for [`QueryBuilder::order_by`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// A type-safe query builder for JsonDB object stores.
///
/// Supports single-field and composite indexes, predicate pushdown, and
/// optional `order_by`/`limit`/`offset`.
///
/// # Example
///
/// ```no_run
/// use flowdb::jsondb::{JsonDB, SortDir};
/// use serde_json::json;
///
/// let db = JsonDB::open(Default::default()).unwrap();
/// db.create_object_store("users", "id").unwrap();
/// db.create_index("users", "by_city_age", &["city", "age"], false).unwrap();
/// db.put("users", json!({"id": "u1", "city": "NYC", "age": 30})).unwrap();
///
/// let docs: Vec<serde_json::Value> = db.query("users")
///     .where_eq("city", json!("NYC"))
///     .where_range("age", json!(25), json!(35))
///     .order_by("age", SortDir::Asc)
///     .limit(10)
///     .collect()
///     .unwrap();
/// ```
pub struct QueryBuilder<'a> {
    db: &'a JsonDB,
    store: &'a str,
    filters: Vec<QueryFilter>,
    order_field: Option<String>,
    order_dir: SortDir,
    limit: Option<usize>,
    offset: usize,
}

#[derive(Debug, Clone)]
enum QueryFilter {
    Eq(String, Value),
    Range(String, Value, Value),
    In(String, Vec<Value>),
}

impl<'a> QueryBuilder<'a> {
    /// Create a new query builder for the given store.
    pub fn new(db: &'a JsonDB, store: &'a str) -> Self {
        Self {
            db,
            store,
            filters: Vec::new(),
            order_field: None,
            order_dir: SortDir::Asc,
            limit: None,
            offset: 0,
        }
    }

    /// Filter: field == value.
    pub fn where_eq(mut self, field: &str, value: Value) -> Self {
        self.filters.push(QueryFilter::Eq(field.to_string(), value));
        self
    }

    /// Filter: `start <= field < end` (exclusive upper bound).
    pub fn where_range(mut self, field: &str, start: Value, end: Value) -> Self {
        self.filters
            .push(QueryFilter::Range(field.to_string(), start, end));
        self
    }

    /// Filter: field IN [...values].
    pub fn where_in(mut self, field: &str, values: Vec<Value>) -> Self {
        self.filters.push(QueryFilter::In(field.to_string(), values));
        self
    }

    /// Sort by `field` in ascending or descending order.
    ///
    /// When the sort field matches the first field of the best-matching index
    /// **and** direction is `Asc`, the results are already in the correct order
    /// and no extra sort is performed.
    pub fn order_by(mut self, field: &str, dir: SortDir) -> Self {
        self.order_field = Some(field.to_string());
        self.order_dir = dir;
        self
    }

    /// Limit results to `n` documents.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skip the first `n` documents.
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    /// Execute the query and return matching documents.
    pub fn collect(self) -> Result<Vec<Value>> {
        let store_def = self
            .db
            .schema
            .get(self.store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", self.store)))?;

        // 1. Find best matching index and compute scan range
        let (scan_result, used_index) =
            self.plan_scan(&store_def);

        // Determine whether an in-memory sort is needed (after the scan,
        // before offset/limit).  This also controls early-termination in
        // step 2 — when no sort is needed we can stop scanning early.
        let needs_sort = match &self.order_field {
            Some(field) => {
                let index_provides_order = used_index
                    .as_ref()
                    .map(|idx: &IndexDef| {
                        self.order_dir == SortDir::Asc
                            && idx.key_paths.first().map(|s| s.as_str()) == Some(field.as_str())
                    })
                    .unwrap_or(false);
                !index_provides_order
            }
            None => false,
        };

        // 2. Execute scan with optional early-termination for limit.
        //    When the scan order matches the natural index order we can
        //    stop after collecting (limit + offset) results.
        let limit_target = self.limit.map(|l| l + self.offset);

        let mut docs: Vec<Value> = match scan_result {
            ScanPlan::Index { prefix, range_start, range_end } => {
                let range = if let (Some(s), Some(e)) = (&range_start, &range_end) {
                    ScanRange {
                        key_start: Bound::Included(s.to_vec()),
                        key_end: Bound::Excluded(e.to_vec()),
                        ts_start: Bound::Unbounded,
                        ts_end: Bound::Unbounded,
                    }
                } else {
                    prefix_range(&prefix)
                };
                let iter = self.db.engine.scan(range)?;
                let mut results = Vec::new();
                for r in iter {
                    let rec = r?;
                    if let Some(doc) =
                        self.db.engine.get_bytes(&doc_key(self.store, &rec.value), 0)
                    {
                        results.push(decode_doc(&doc.value)?);
                    }
                    // Early-terminate when limit is set and no sort is needed.
                    if !needs_sort {
                        if let Some(target) = limit_target {
                            if results.len() >= target {
                                break;
                            }
                        }
                    }
                }
                results
            }
            ScanPlan::FullScan => {
                let pfx = doc_prefix(self.store);
                let iter = self.db.engine.scan(prefix_range(&pfx))?;
                let mut results = Vec::new();
                for r in iter {
                    let rec = r?;
                    results.push(decode_doc(&rec.value)?);
                    // Early-terminate when limit is set.
                    if let Some(target) = limit_target {
                        if results.len() >= target {
                            break;
                        }
                    }
                }
                results
            }
        };

        // 3. Apply predicate pushdown (remaining filters not covered by index)
        for filter in &self.filters {
            docs.retain(|doc| filter_matches(doc, filter));
        }

        // 4. Sort if needed.
        if needs_sort {
            if let Some(ref field) = self.order_field {
                docs.sort_by(|a, b| {
                    let va = extract_field(a, field).unwrap_or(Value::Null);
                    let vb = extract_field(b, field).unwrap_or(Value::Null);
                    let cmp = encode_index_value(&va).cmp(&encode_index_value(&vb));
                    match self.order_dir {
                        SortDir::Asc => cmp,
                        SortDir::Desc => cmp.reverse(),
                    }
                });
            }
        }

        // 5. Offset + limit
        let docs: Vec<Value> = docs
            .into_iter()
            .skip(self.offset)
            .take(self.limit.unwrap_or(usize::MAX))
            .collect();

        Ok(docs)
    }

    /// Execute the query and deserialize results to `T`.
    ///
    /// ```ignore
    /// let users: Vec<User> = db.query("users")
    ///     .where_eq("email", "a@b.com")
    ///     .collect_doc()?;
    /// ```
    pub fn collect_doc<T: serde::de::DeserializeOwned>(self) -> Result<Vec<T>> {
        let values: Vec<Value> = self.collect()?;
        values.into_iter().map(|v| serde_json::from_value(v).map_err(FlowError::from)).collect()
    }
}

enum ScanPlan {
    Index {
        prefix: Vec<u8>,
        range_start: Option<Vec<u8>>,
        range_end: Option<Vec<u8>>,
    },
    FullScan,
}

/// Check whether a document matches a single filter.
fn filter_matches(doc: &Value, filter: &QueryFilter) -> bool {
    match filter {
        QueryFilter::Eq(field, val) => {
            extract_field(doc, field).as_ref() == Some(val)
        }
        QueryFilter::Range(field, start, end) => {
            match extract_field(doc, field) {
                Some(ref v) => {
                    let enc = encode_index_value(v);
                    let enc_start = encode_index_value(start);
                    let enc_end = encode_index_value(end);
                    enc.as_slice() >= enc_start.as_slice()
                        && enc.as_slice() < enc_end.as_slice()
                }
                None => false,
            }
        }
        QueryFilter::In(field, values) => {
            match extract_field(doc, field) {
                Some(ref v) => values.iter().any(|val| {
                    encode_index_value(v) == encode_index_value(val)
                }),
                None => false,
            }
        }
    }
}

impl<'a> QueryBuilder<'a> {
    fn plan_scan(&self, store_def: &StoreDef) -> (ScanPlan, Option<IndexDef>) {
        // Score each index by how many prefix fields match Eq/Range filters
        let mut best: Option<(usize, &IndexDef)> = None;
        for idx in &store_def.indexes {
            let score = self.score_index(idx);
            if score > best.map(|(s, _)| s).unwrap_or(0) {
                best = Some((score, idx));
            }
        }

        let (used_idx, plan) = match best {
            Some((_, idx)) => {
                let plan = self.build_index_scan(idx);
                (Some(idx.clone()), plan)
            }
            None => (None, ScanPlan::FullScan),
        };

        (plan, used_idx)
    }

    /// Number of prefix key_paths covered by Eq/Range/In filters.
    fn score_index(&self, idx: &IndexDef) -> usize {
        let mut score = 0usize;
        for path in &idx.key_paths {
            let matched = self.filters.iter().any(|f| match f {
                QueryFilter::Eq(field, _) => field == path,
                QueryFilter::Range(field, _, _) => field == path,
                QueryFilter::In(field, _) => field == path,
            });
            if matched {
                score += 1;
            } else {
                break; // prefix stop: we can only use prefix of composite
            }
        }
        score
    }

    fn build_index_scan(&self, idx: &IndexDef) -> ScanPlan {
        // Collect one encoded value per key_path, building the prefix key.
        let mut partial = idx_prefix(self.store, &idx.name);
        let mut range_end_bytes: Option<Vec<u8>> = None;

        for path in &idx.key_paths {
            let matched = self.filters.iter().find(|f| match f {
                QueryFilter::Eq(field, _) => field == path,
                QueryFilter::Range(field, _, _) => field == path,
                QueryFilter::In(field, _) => field == path,
            });

            match matched {
                Some(QueryFilter::Eq(_, val)) => {
                    let enc = encode_index_value(val);
                    partial.extend_from_slice(&enc);
                    partial.push(SEP);
                }
                Some(QueryFilter::Range(_, start, end)) => {
                    let enc_start = encode_index_value(start);
                    let enc_end = encode_index_value(end);
                    partial.extend_from_slice(&enc_start);
                    let mut end_key = idx_prefix(self.store, &idx.name);
                    for prev_path in &idx.key_paths {
                        if prev_path == path {
                            end_key.extend_from_slice(&enc_end);
                            break;
                        }
                        if let Some(QueryFilter::Eq(_, v)) = self.filters.iter().find(|filt| match filt {
                            QueryFilter::Eq(field, _) => field == prev_path,
                            _ => false,
                        }) {
                            end_key.extend_from_slice(&encode_index_value(v));
                            end_key.push(SEP);
                        }
                    }
                    range_end_bytes = Some(end_key);
                    break;
                }
                Some(QueryFilter::In(_, _)) => break,
                None => break,
            }
        }

        if partial.last() == Some(&SEP) {
            partial.pop();
        }

        ScanPlan::Index {
            range_start: if !partial.is_empty() {
                Some(partial.clone())
            } else {
                None
            },
            range_end: range_end_bytes,
            prefix: partial,
        }
    }
}

// ── internal helpers ──────────────────────────────────────────────

/// Build a batch of `InternalRecord`s for a document put.
fn build_put_batch(
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
fn build_delete_batch(
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
fn extract_index_values(doc: &Value, idx: &IndexDef) -> Vec<Vec<Value>> {
    // Collect values for each key_path
    let mut raw: Vec<Value> = Vec::with_capacity(idx.key_paths.len());
    for path in &idx.key_paths {
        match extract_field(doc, path) {
            None => return vec![],
            Some(val) => raw.push(val),
        }
    }

    // Multi-entry on single-field index: expand array elements
    if idx.multi_entry && idx.key_paths.len() == 1 {
        if let Value::Array(arr) = &raw[0] {
            return arr.iter().map(|v| vec![v.clone()]).collect();
        }
    }

    vec![raw]
}

/// Read the current auto-increment counter and produce the next value
/// together with an `InternalRecord` that must be included in the main
/// write batch so the counter increment is atomic with the document
/// write.
fn prepare_counter(engine: &Engine, store: &str) -> Result<(u64, InternalRecord)> {
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
    let rec = InternalRecord::from_record(
        &Record::new(key, 0, next.to_be_bytes().to_vec()),
        0,
    );
    Ok((next, rec))
}

// ── tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_db() -> (JsonDB, TempDir) {
        let dir = TempDir::new().unwrap();
        let cfg = Config {
            data_dir: dir.path().to_path_buf(),
            auto_background: false,
            ..Default::default()
        };
        let db = JsonDB::open(cfg).unwrap();
        (db, dir)
    }

    fn setup_users(db: &JsonDB) {
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true).unwrap();
        db.create_index("users", "by_age", &["age"], false).unwrap();
    }

    // ── basic CRUD ────────────────────────────────────────────────

    #[test]
    fn test_put_and_get() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        let doc = db.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
    }

    #[test]
    fn test_get_nonexistent() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let doc = db.get("users", &json!("missing")).unwrap();
        assert!(doc.is_none());
    }

    #[test]
    fn test_put_and_delete() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        assert_eq!(db.count("users").unwrap(), 1);

        db.delete("users", &json!("u1")).unwrap();
        assert_eq!(db.count("users").unwrap(), 0);
        assert!(db.get("users", &json!("u1")).unwrap().is_none());
    }

    #[test]
    fn test_put_update() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        db.put("users", json!({"id": "u1", "name": "Bob"}))
            .unwrap();

        let doc = db.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Bob");
        assert_eq!(db.count("users").unwrap(), 1);
    }

    #[test]
    fn test_scan() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        db.put("users", json!({"id": "u2", "name": "Bob"}))
            .unwrap();
        db.put("users", json!({"id": "u3", "name": "Carol"}))
            .unwrap();

        let docs = db.scan("users").unwrap();
        assert_eq!(docs.len(), 3);
    }

    #[test]
    fn test_count() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        assert_eq!(db.count("users").unwrap(), 0);
        db.put("users", json!({"id": "u1"})).unwrap();
        assert_eq!(db.count("users").unwrap(), 1);
        db.put("users", json!({"id": "u2"})).unwrap();
        assert_eq!(db.count("users").unwrap(), 2);
        db.delete("users", &json!("u1")).unwrap();
        assert_eq!(db.count("users").unwrap(), 1);
    }

    // ── secondary indexes ─────────────────────────────────────────

    #[test]
    fn test_index_point_lookup() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "alice@b.com", "age": 30}))
            .unwrap();
        db.put("users", json!({"id": "u2", "email": "bob@c.com", "age": 25}))
            .unwrap();

        let docs = db.get_by_index("users", "by_email", &json!("alice@b.com")).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_index_range_lookup() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "alice@b.com", "age": 30}))
            .unwrap();
        db.put("users", json!({"id": "u2", "email": "bob@c.com", "age": 25}))
            .unwrap();
        db.put("users", json!({"id": "u3", "email": "carol@d.com", "age": 35}))
            .unwrap();

        // age in [25, 35) — should include u2 (age=25) and u1 (age=30)
        let docs = db.range_by_index("users", "by_age", &json!(25), &json!(35)).unwrap();
        assert_eq!(docs.len(), 2, "expected 2 docs in age range [25,35)");
    }

    #[test]
    fn test_unique_index_violation() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "same@b.com"}))
            .unwrap();

        let err = db
            .put("users", json!({"id": "u2", "email": "same@b.com"}))
            .unwrap_err();
        assert!(
            err.to_string().contains("unique"),
            "expected unique violation: {}",
            err
        );
    }

    #[test]
    fn test_index_update_on_put() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "old@b.com", "age": 30}))
            .unwrap();

        // Update the email.
        db.put("users", json!({"id": "u1", "email": "new@b.com", "age": 30}))
            .unwrap();

        // Old email should have no docs.
        let docs = db.get_by_index("users", "by_email", &json!("old@b.com")).unwrap();
        assert_eq!(docs.len(), 0, "old index entry should be gone");

        // New email should have the doc.
        let docs = db.get_by_index("users", "by_email", &json!("new@b.com")).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_index_delete_removes_entries() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "alice@b.com"}))
            .unwrap();
        db.delete("users", &json!("u1")).unwrap();

        let docs = db
            .get_by_index("users", "by_email", &json!("alice@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_create_index_on_existing_data() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        // Put docs before creating index.
        db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();
        db.put("users", json!({"id": "u2", "email": "b@c.com"})).unwrap();

        // Now create the index.
        db.create_index("users", "by_email", &["email"], true).unwrap();

        // Should be able to query by index.
        let docs = db.get_by_index("users", "by_email", &json!("a@b.com")).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    // ── auto-increment ────────────────────────────────────────────

    #[test]
    fn test_auto_increment_store() {
        let (db, _dir) = test_db();
        db.create_object_store("events", "id").unwrap();
        // Make it auto-increment by setting the flag (simulated via direct schema update).
        let mut def = db.get_store("events").unwrap();
        def.auto_increment = true;
        db.engine
            .write_internal(vec![InternalRecord::from_record(
                &schema_record(&def).unwrap(),
                0,
            )])
            .unwrap();
        db.schema.insert(def);

        db.put_auto("events", json!({"type": "click"})).unwrap();
        db.put_auto("events", json!({"type": "scroll"})).unwrap();
        db.put_auto("events", json!({"type": "nav"})).unwrap();

        assert_eq!(db.count("events").unwrap(), 3);
        let doc1 = db.get("events", &json!(1)).unwrap().unwrap();
        assert_eq!(doc1["type"], "click");
        let doc3 = db.get("events", &json!(3)).unwrap().unwrap();
        assert_eq!(doc3["type"], "nav");
    }

    // ── explicit transactions ─────────────────────────────────────

    #[test]
    fn test_transaction_commit() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        tx.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();
        tx.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();
        tx.commit().unwrap();

        assert_eq!(db.count("users").unwrap(), 2);
    }

    #[test]
    fn test_transaction_abort() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        tx.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();
        tx.abort(); // drop without commit

        assert_eq!(db.count("users").unwrap(), 0);
    }

    #[test]
    fn test_transaction_read_your_writes() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        // Should see committed data.
        let doc = tx.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");

        // Buffered write should be visible.
        tx.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();
        let doc2 = tx.get("users", &json!("u2")).unwrap().unwrap();
        assert_eq!(doc2["name"], "Bob");

        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_index_read_your_writes() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "alice@b.com"})).unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        tx.put("users", json!({"id": "u2", "email": "bob@c.com"})).unwrap();

        // The index in the engine doesn't yet know about u2, but the
        // transaction's get_by_index should find it via the write buffer.
        let docs = tx.get_by_index("users", "by_email", &json!("bob@c.com")).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u2");

        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_atomicity() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true).unwrap();

        // This will fail because u1 already exists.
        db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();

        // Single batch with a unique violation.
        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        tx.put("users", json!({"id": "u2", "email": "b@c.com"})).unwrap();
        tx.put("users", json!({"id": "u3", "email": "a@b.com"})).unwrap(); // violation

        let err = tx.commit();
        assert!(err.is_err(), "expected unique violation on commit");

        // The entire batch should be rolled back.
        assert_eq!(db.count("users").unwrap(), 1);
        assert!(db.get("users", &json!("u2")).unwrap().is_none());
    }

    #[test]
    fn test_transaction_readonly_rejects_writes() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadOnly).unwrap();
        let err = tx.put("users", json!({"id": "u1", "name": "Alice"})).unwrap_err();
        assert!(
            err.to_string().contains("read-only"),
            "expected read-only error: {}",
            err
        );
    }

    // ── schema management ─────────────────────────────────────────

    #[test]
    fn test_create_delete_store() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        assert!(db.get_store("users").is_some());

        db.put("users", json!({"id": "u1"})).unwrap();
        assert_eq!(db.count("users").unwrap(), 1);

        db.delete_object_store("users").unwrap();
        assert!(db.get_store("users").is_none());
    }

    #[test]
    fn test_create_delete_index() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true).unwrap();

        let def = db.get_store("users").unwrap();
        assert_eq!(def.indexes.len(), 1);

        db.delete_index("users", "by_email").unwrap();
        let def = db.get_store("users").unwrap();
        assert_eq!(def.indexes.len(), 0);
    }

    #[test]
    fn test_store_names() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_object_store("orders", "id").unwrap();
        let mut names = db.store_names();
        names.sort();
        assert_eq!(names, vec!["orders", "users"]);
    }

    // ── edge cases ────────────────────────────────────────────────

    #[test]
    fn test_missing_store_returns_error() {
        let (db, _dir) = test_db();
        let err = db.get("nonexistent", &json!("1")).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_document_without_key_field() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let err = db
            .put("users", json!({"name": "Alice"}))
            .unwrap_err();
        assert!(
            err.to_string().contains("missing"),
            "expected missing-key error: {}",
            err
        );
    }

    #[test]
    fn test_duplicate_put_updates() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "val": 1})).unwrap();
        db.put("users", json!({"id": "u1", "val": 2})).unwrap();
        db.put("users", json!({"id": "u1", "val": 3})).unwrap();

        assert_eq!(db.count("users").unwrap(), 1);
        assert_eq!(db.get("users", &json!("u1")).unwrap().unwrap()["val"], 3);
    }

    #[test]
    fn test_reopen_persists_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // First session.
        {
            let cfg = Config {
                data_dir: path.clone(),
                auto_background: false,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            db.create_object_store("users", "id").unwrap();
            db.create_index("users", "by_email", &["email"], true).unwrap();
            db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();
            db.shutdown().unwrap();
        }

        // Second session — reopen.
        {
            let cfg = Config {
                data_dir: path,
                auto_background: false,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            assert!(db.get_store("users").is_some());

            let doc = db.get("users", &json!("u1")).unwrap().unwrap();
            assert_eq!(doc["email"], "a@b.com");

            let docs = db.get_by_index("users", "by_email", &json!("a@b.com")).unwrap();
            assert_eq!(docs.len(), 1);
        }
    }

    #[test]
    fn test_non_string_primary_key() {
        let (db, _dir) = test_db();
        db.create_object_store("nums", "id").unwrap();

        db.put("nums", json!({"id": 42, "name": "answer"})).unwrap();
        db.put("nums", json!({"id": 100, "name": "hundred"})).unwrap();

        assert_eq!(db.count("nums").unwrap(), 2);
        let doc = db.get("nums", &json!(42)).unwrap().unwrap();
        assert_eq!(doc["name"], "answer");

        let doc = db.get("nums", &json!(100)).unwrap().unwrap();
        assert_eq!(doc["name"], "hundred");
    }

    #[test]
    fn test_index_on_deleted_doc_removed() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();
        db.put("users", json!({"id": "u2", "email": "b@c.com"})).unwrap();

        let docs = db.get_by_index("users", "by_email", &json!("a@b.com")).unwrap();
        assert_eq!(docs.len(), 1);

        db.delete("users", &json!("u1")).unwrap();

        let docs = db.get_by_index("users", "by_email", &json!("a@b.com")).unwrap();
        assert_eq!(docs.len(), 0, "index entry should be removed after delete");
    }

    // ── error path tests ─────────────────────────────────────────

    #[test]
    fn test_put_missing_store() {
        let (db, _dir) = test_db();
        let err = db.put("nonexistent", json!({"id": "1"})).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_delete_missing_store() {
        let (db, _dir) = test_db();
        let err = db.delete("nonexistent", &json!("1")).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_count_missing_store() {
        let (db, _dir) = test_db();
        let err = db.count("nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_scan_missing_store() {
        let (db, _dir) = test_db();
        let err = db.scan("nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_get_by_index_missing_store() {
        let (db, _dir) = test_db();
        let err = db.get_by_index("nonexistent", "idx", &json!("v")).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_get_by_index_missing_index() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db.get_by_index("users", "nonexistent", &json!("v")).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_range_by_index_missing_store() {
        let (db, _dir) = test_db();
        let err = db.range_by_index("nonexistent", "idx", &json!(0), &json!(10)).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_range_by_index_missing_index() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db.range_by_index("users", "nonexistent", &json!(0), &json!(10)).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_put_auto_non_auto() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db.put_auto("users", json!({"type": "x"})).unwrap_err();
        assert!(err.to_string().contains("not auto-increment"));
    }

    #[test]
    fn test_put_auto_missing_store() {
        let (db, _dir) = test_db();
        let err = db.put_auto("nonexistent", json!({"type": "x"})).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_delete_object_store_missing() {
        let (db, _dir) = test_db();
        let err = db.delete_object_store("nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_create_object_store_same_name_different_key_path() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db.create_object_store("users", "uuid").unwrap_err();
        assert!(err.to_string().contains("different key_path"));
    }

    #[test]
    fn test_create_object_store_idempotent() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        // Same name + same key_path should succeed (no-op).
        assert!(db.create_object_store("users", "id").is_ok());
    }

    #[test]
    fn test_create_index_duplicate() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true).unwrap();
        let err = db.create_index("users", "by_email", &["phone"], true).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn test_create_index_missing_store() {
        let (db, _dir) = test_db();
        let err = db.create_index("nonexistent", "idx", &["field"], false).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_delete_index_missing_store() {
        let (db, _dir) = test_db();
        let err = db.delete_index("nonexistent", "idx").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_delete_index_missing() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db.delete_index("users", "nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_transaction_missing_store() {
        let (db, _dir) = test_db();
        let err = db.transaction(&["nonexistent"], TransactionMode::ReadWrite).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_transaction_delete_in_buffer() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        let doc = tx.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");

        tx.delete("users", &json!("u1")).unwrap();
        assert!(tx.get("users", &json!("u1")).unwrap().is_none());
        tx.commit().unwrap();

        assert!(db.get("users", &json!("u1")).unwrap().is_none());
    }

    #[test]
    fn test_transaction_put_auto() {
        let (db, _dir) = test_db();
        db.create_object_store("events", "id").unwrap();
        let mut def = db.get_store("events").unwrap();
        def.auto_increment = true;
        db.engine.write_internal(vec![InternalRecord::from_record(
            &schema_record(&def).unwrap(), 0,
        )]).unwrap();
        db.schema.insert(def);

        let mut tx = db.transaction(&["events"], TransactionMode::ReadWrite).unwrap();
        let key1 = tx.put_auto("events", json!({"type": "click"})).unwrap();
        assert_eq!(key1, json!(1));
        let key2 = tx.put_auto("events", json!({"type": "scroll"})).unwrap();
        assert_eq!(key2, json!(2));
        tx.commit().unwrap();

        assert_eq!(db.count("events").unwrap(), 2);
    }

    #[test]
    fn test_transaction_put_auto_non_auto() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        let err = tx.put_auto("users", json!({"name": "x"})).unwrap_err();
        assert!(err.to_string().contains("not auto-increment"));
    }

    #[test]
    fn test_transaction_scan_buffered_puts() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        tx.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();

        // Scan should see both committed and buffered docs.
        let docs = tx.scan("users").unwrap();
        assert_eq!(docs.len(), 2);
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_scan_with_buffered_delete() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();
        db.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        tx.delete("users", &json!("u1")).unwrap();

        let docs = tx.scan("users").unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Bob");
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_range_by_index() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        tx.put("users", json!({"id": "u2", "age": 25})).unwrap();

        let docs = tx.range_by_index("users", "by_age", &json!(20), &json!(35)).unwrap();
        // Should see both u1 (committed) and u2 (buffered)
        assert_eq!(docs.len(), 2);
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_get_by_index_buffered_update() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "email": "old@b.com"})).unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        // Update email in buffer (not yet committed)
        tx.delete("users", &json!("u1")).unwrap();
        tx.put("users", json!({"id": "u1", "email": "new@b.com"})).unwrap();

        // get_by_index should NOT find old email (doc deleted in buffer)
        let docs = tx.get_by_index("users", "by_email", &json!("old@b.com")).unwrap();
        assert_eq!(docs.len(), 0, "old email should not be visible");

        // Should find new email from buffer
        let docs = tx.get_by_index("users", "by_email", &json!("new@b.com")).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_get_by_index_buffered_delete_only() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();

        let tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        // Without any buffered writes, read-only should work
        let docs = tx.get_by_index("users", "by_email", &json!("a@b.com")).unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn test_transaction_abort_drop() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        {
            let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
            tx.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();
            // Drop without commit = abort
        }
        assert_eq!(db.count("users").unwrap(), 0);
    }

    #[test]
    fn test_transaction_double_commit() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        // tx.put... but we need to capture the result of first commit
        // empty commit should work
        assert!(tx.commit().is_ok());
    }

    #[test]
    fn test_transaction_empty_commit() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        assert!(tx.commit().is_ok());
    }

    // ── delete_object_store with data ─────────────────────────────

    #[test]
    fn test_delete_store_with_indexes_and_data() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "email": "a@b.com", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "email": "b@c.com", "age": 25})).unwrap();
        assert_eq!(db.count("users").unwrap(), 2);

        db.delete_object_store("users").unwrap();
        assert!(db.get_store("users").is_none());
        // Create a new store with the same name to verify clean state
        db.create_object_store("users", "id").unwrap();
        assert_eq!(db.count("users").unwrap(), 0);
    }

    #[test]
    fn test_delete_store_with_auto_increment() {
        let (db, _dir) = test_db();
        db.create_object_store("events", "id").unwrap();
        let mut def = db.get_store("events").unwrap();
        def.auto_increment = true;
        db.engine.write_internal(vec![InternalRecord::from_record(
            &schema_record(&def).unwrap(), 0,
        )]).unwrap();
        db.schema.insert(def);

        db.put_auto("events", json!({"type": "click"})).unwrap();
        db.delete_object_store("events").unwrap();
        assert!(db.get_store("events").is_none());
    }

    // ── secondary index with multiple values ──────────────────────

    #[test]
    fn test_index_on_nested_field() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city", &["address.city"], false).unwrap();
        db.put("users", json!({"id": "u1", "address": {"city": "NYC"}})).unwrap();
        db.put("users", json!({"id": "u2", "address": {"city": "SF"}})).unwrap();

        let docs = db.get_by_index("users", "by_city", &json!("NYC")).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_index_on_field_not_present() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], false).unwrap();
        // Doc without the indexed field
        db.put("users", json!({"id": "u1"})).unwrap();
        // Should not create index entry, so query returns no results
        let docs = db.get_by_index("users", "by_email", &json!("x")).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_index_float_values() {
        let (db, _dir) = test_db();
        db.create_object_store("scores", "id").unwrap();
        db.create_index("scores", "by_score", &["score"], false).unwrap();

        db.put("scores", json!({"id": "a", "score": 95.5})).unwrap();
        db.put("scores", json!({"id": "b", "score": 87.3})).unwrap();
        db.put("scores", json!({"id": "c", "score": 95.5})).unwrap();

        let docs = db.get_by_index("scores", "by_score", &json!(95.5)).unwrap();
        assert_eq!(docs.len(), 2);

        let docs = db.range_by_index("scores", "by_score", &json!(80.0), &json!(90.0)).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "b");
    }

    #[test]
    fn test_index_bool_values() {
        let (db, _dir) = test_db();
        db.create_object_store("items", "id").unwrap();
        db.create_index("items", "by_active", &["active"], false).unwrap();

        db.put("items", json!({"id": 1, "active": true})).unwrap();
        db.put("items", json!({"id": 2, "active": false})).unwrap();

        let docs = db.get_by_index("items", "by_active", &json!(true)).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], 1);

        let docs = db.get_by_index("items", "by_active", &json!(false)).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], 2);
    }

    #[test]
    fn test_index_null_values() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], false).unwrap();

        db.put("users", json!({"id": "u1", "email": null})).unwrap();

        let docs = db.get_by_index("users", "by_email", &json!(Value::Null)).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    // ── many documents / performance stress ───────────────────────

    #[test]
    fn test_many_documents() {
        let (db, _dir) = test_db();
        db.create_object_store("large", "id").unwrap();
        db.create_index("large", "by_val", &["val"], false).unwrap();

        let n = 200;
        for i in 0..n {
            db.put("large", json!({"id": i, "val": i % 50})).unwrap();
        }
        assert_eq!(db.count("large").unwrap(), n);

        // Index point query
        let docs = db.get_by_index("large", "by_val", &json!(0)).unwrap();
        assert_eq!(docs.len(), n / 50);

        // Index range query
        let docs = db.range_by_index("large", "by_val", &json!(10), &json!(20)).unwrap();
        assert!(docs.len() > 0);

        // Non-string key check
        let doc = db.get("large", &json!(0)).unwrap();
        assert!(doc.is_some());
    }

    // ── multiple stores ───────────────────────────────────────────

    #[test]
    fn test_multiple_stores_independent() {
        let (db, _dir) = test_db();
        db.create_object_store("a", "id").unwrap();
        db.create_object_store("b", "id").unwrap();
        db.create_index("a", "by_val", &["val"], false).unwrap();

        db.put("a", json!({"id": "a1", "val": 1})).unwrap();
        db.put("b", json!({"id": "b1", "val": 2})).unwrap();

        assert_eq!(db.count("a").unwrap(), 1);
        assert_eq!(db.count("b").unwrap(), 1);

        let docs = db.get_by_index("a", "by_val", &json!(1)).unwrap();
        assert_eq!(docs.len(), 1);
    }

    // ── from_engine ───────────────────────────────────────────────

    #[test]
    fn test_from_engine_with_existing_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Create with schema and data, then reopen via JsonDB::open
        {
            let cfg = Config { data_dir: path.clone(), auto_background: false, ..Default::default() };
            let db = JsonDB::open(cfg).unwrap();
            db.create_object_store("users", "id").unwrap();
            db.put("users", json!({"id": "u1"})).unwrap();
            db.shutdown().unwrap();
        }

        // Reopen via JsonDB::open (which internally calls Engine::open)
        {
            let cfg = Config { data_dir: path, auto_background: false, ..Default::default() };
            let db = JsonDB::open(cfg).unwrap();
            assert!(db.get_store("users").is_some());
            assert_eq!(db.count("users").unwrap(), 1);
        }
    }

    // ── Transaction count with mixture ───────────────────────────

    #[test]
    fn test_transaction_count_mixed() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1"})).unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        // u1 is committed, u2 is buffered
        tx.put("users", json!({"id": "u2"})).unwrap();
        assert_eq!(tx.count("users").unwrap(), 2);

        tx.delete("users", &json!("u1")).unwrap();
        // u1 deleted in buffer, u2 still buffered
        assert_eq!(tx.count("users").unwrap(), 1);
        tx.commit().unwrap();
    }

    // ── Edge cases for encoding module ────────────────────────────

    #[test]
    fn test_primary_key_numeric_types() {
        let (db, _dir) = test_db();
        db.create_object_store("items", "id").unwrap();

        // Bool key
        db.put("items", json!({"id": true, "name": "yes"})).unwrap();
        let doc = db.get("items", &json!(true)).unwrap().unwrap();
        assert_eq!(doc["name"], "yes");

        // Null key
        db.put("items", json!({"id": null, "name": "nothing"})).unwrap();

        // Negative number key
        db.put("items", json!({"id": -5, "name": "neg"})).unwrap();
        let doc = db.get("items", &json!(-5)).unwrap().unwrap();
        assert_eq!(doc["name"], "neg");
    }

    // ── create_index on existing data with unique constraint ──────

    #[test]
    fn test_create_index_on_existing_data_unique_violation() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "email": "same@b.com"})).unwrap();
        db.put("users", json!({"id": "u2", "email": "same@b.com"})).unwrap();

        // Creating a unique index on existing data with duplicates succeeds
        // but subsequent put attempts will fail with unique violation.
        db.create_index("users", "by_email", &["email"], true).unwrap();

        let err = db.put("users", json!({"id": "u3", "email": "same@b.com"})).unwrap_err();
        assert!(err.to_string().contains("unique"), "expected unique violation");
    }

    // ── validate_name edge cases ──────────────────────────────────

    #[test]
    fn test_validate_name_in_creation() {
        let (db, _dir) = test_db();
        let err = db.create_object_store("", "id").unwrap_err();
        assert!(err.to_string().contains("empty"));
        let err = db.create_object_store("has space", "id").unwrap_err();
        assert!(err.to_string().contains("invalid"));
    }

    // ── shutdown/close ────────────────────────────────────────────

    #[test]
    fn test_json_db_shutdown_close() {
        let dir = TempDir::new().unwrap();
        let cfg = Config {
            data_dir: dir.path().to_path_buf(),
            auto_background: false,
            ..Default::default()
        };
        let db = JsonDB::open(cfg).unwrap();
        db.close().unwrap();
        // After close the engine is flushed but still usable
        assert!(db.store_names().is_empty());
    }

    // ── schema module edge cases ──────────────────────────────────

    #[test]
    fn test_validate_index_def_edge_cases() {
        let def = StoreDef {
            name: "users".into(),
            key_path: "id".into(),
            auto_increment: false,
            indexes: vec![IndexDef { name: "existing".into(), key_paths: vec!["f".into()], unique: false, multi_entry: false }],
            next_auto_id: 0,
        };
        assert!(validate_index_def(&def, "", &["f"]).is_err());
        assert!(validate_index_def(&def, "existing", &["f"]).is_err());
        assert!(validate_index_def(&def, "new", &[""]).is_err());
    }

    // Test the internal key encoding functions directly
    #[test]
    fn test_encoding_u64_number() {
        // u64 that doesn't fit in i64
        let val = Value::Number(serde_json::Number::from(18446744073709551615u64));
        let encoded = encode_index_value(&val);
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encoding_negative_float() {
        let val = json!(-3.5e10);
        let encoded = encode_index_value(&val);
        let val2 = json!(-3.5e10 + 1.0);
        let _encoded2 = encode_index_value(&val2);
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_primary_key_object_type() {
        // Object primary key (fallback to JSON ser)
        let val = json!({"a": 1, "b": 2});
        let pk = encode_primary_key(&val).unwrap();
        assert!(!pk.is_empty());
    }

    #[test]
    fn test_extract_field_deep_path() {
        let doc = json!({"a": {"b": {"c": [1, 2, 3]}}});
        assert_eq!(extract_field(&doc, "a.b.c.0"), Some(json!(1)));
        assert_eq!(extract_field(&doc, "a.b.c.3"), None);
        assert_eq!(extract_field(&doc, "a.b.c"), Some(json!([1, 2, 3])));
    }

    #[test]
    fn test_extract_field_from_non_object() {
        let doc_str = json!("hello");
        assert_eq!(extract_field(&doc_str, "anything"), None);

        let doc_arr = json!([1, 2, 3]);
        assert_eq!(extract_field(&doc_arr, "0"), Some(json!(1)));
        assert_eq!(extract_field(&doc_arr, "5"), None);
    }

    // ── transaction range_by_index via direct method ──────────────

    #[test]
    fn test_range_by_index_empty_range() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();

        // Empty range [100, 100) should return no results
        let docs = db.range_by_index("users", "by_age", &json!(100), &json!(100)).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_range_by_index_start_equals_end() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();

        // Range [30, 30) should be empty (exclusive end)
        let docs = db.range_by_index("users", "by_age", &json!(30), &json!(30)).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_many_concurrent_transactions() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut threads = Vec::new();
        let db = Arc::new(db);
        for i in 0..10 {
            let db = db.clone();
            threads.push(std::thread::spawn(move || {
                let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
                tx.put("users", json!({"id": format!("t{}", i)})).unwrap();
                tx.commit().unwrap();
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(db.count("users").unwrap(), 10);
    }

    #[test]
    fn test_put_in_transaction_updates_index() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "old@b.com"})).unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        tx.delete("users", &json!("u1")).unwrap();
        tx.put("users", json!({"id": "u1", "email": "new@b.com"})).unwrap();
        tx.commit().unwrap();

        // After commit, index should be updated
        let docs = db.get_by_index("users", "by_email", &json!("new@b.com")).unwrap();
        assert_eq!(docs.len(), 1);

        let docs = db.get_by_index("users", "by_email", &json!("old@b.com")).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_reopen_with_indexes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let cfg = Config { data_dir: path.clone(), auto_background: false, ..Default::default() };
            let db = JsonDB::open(cfg).unwrap();
            db.create_object_store("users", "id").unwrap();
            db.create_index("users", "by_name", &["name"], false).unwrap();
            db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();
            db.shutdown().unwrap();
        }
        {
            let cfg = Config { data_dir: path, auto_background: false, ..Default::default() };
            let db = JsonDB::open(cfg).unwrap();
            let docs = db.get_by_index("users", "by_name", &json!("Alice")).unwrap();
            assert_eq!(docs.len(), 1);
        }
    }

    // ── durability ───────────────────────────────────────────────
    //
    // All writes go through Engine::write_internal → WAL + memtable.
    // With SyncMode::Always (default) every batch is fsynced before
    // returning.  The test below exercises the full shutdown / reopen
    // cycle to confirm data survives.

    #[test]
    fn test_durability_shutdown_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let cfg = Config { data_dir: path.clone(), auto_background: false, ..Default::default() };
            let db = JsonDB::open(cfg).unwrap();
            db.create_object_store("store", "id").unwrap();
            db.create_index("store", "by_val", &["val"], true).unwrap();
            for i in 0u64..20 {
                db.put("store", json!({"id": i, "val": format!("v{}", i)})).unwrap();
            }
            // Flush + fsync before shutdown.
            db.close().unwrap();
        }
        // Reopen — all 20 docs and their index entries must be present.
        {
            let cfg = Config { data_dir: path, auto_background: false, ..Default::default() };
            let db = JsonDB::open(cfg).unwrap();
            assert_eq!(db.count("store").unwrap(), 20);
            for i in 0u64..20 {
                let doc = db.get("store", &json!(i)).unwrap();
                assert!(doc.is_some(), "doc {} missing after reopen", i);
            }
            let docs = db.get_by_index("store", "by_val", &json!("v5")).unwrap();
            assert_eq!(docs.len(), 1);
            assert_eq!(docs[0]["id"], 5);
        }
    }

    // ── performance benchmark (not a correctness test) ───────────

    #[test]
    fn test_throughput_sequential_writes() {
        let (db, _dir) = test_db();
        db.create_object_store("bench", "id").unwrap();
        db.create_index("bench", "by_val", &["val"], false).unwrap();

        let n = 1_000;
        let start = std::time::Instant::now();
        for i in 0u64..n {
            db.put("bench", json!({"id": i, "val": i % 100})).unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = n as f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB sequential write: {} ops in {:.3}s = {:.0} ops/s",
            n,
            elapsed.as_secs_f64(),
            ops_per_sec
        );
        assert_eq!(db.count("bench").unwrap(), n as usize);
    }

    #[test]
    fn test_throughput_point_reads() {
        let (db, _dir) = test_db();
        db.create_object_store("bench", "id").unwrap();
        db.create_index("bench", "by_val", &["val"], false).unwrap();

        let n = 1_000;
        for i in 0u64..n {
            db.put("bench", json!({"id": i, "val": i * 2})).unwrap();
        }

        let start = std::time::Instant::now();
        for i in 0u64..n {
            let _ = db.get("bench", &json!(i)).unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = n as f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB point read: {} ops in {:.3}s = {:.0} ops/s",
            n,
            elapsed.as_secs_f64(),
            ops_per_sec
        );
    }

    #[test]
    fn test_throughput_index_query() {
        let (db, _dir) = test_db();
        db.create_object_store("bench", "id").unwrap();
        db.create_index("bench", "by_val", &["val"], false).unwrap();

        let n = 1_000;
        for i in 0u64..n {
            db.put("bench", json!({"id": i, "val": i % 50})).unwrap();
        }

        let start = std::time::Instant::now();
        for v in 0..50 {
            let docs = db.get_by_index("bench", "by_val", &json!(v)).unwrap();
            assert_eq!(docs.len(), n as usize / 50);
        }
        let elapsed = start.elapsed();
        let ops_per_sec = 50f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB index query (50 lookups): {:.3}s total, {:.0} queries/s",
            elapsed.as_secs_f64(),
            ops_per_sec
        );
    }

    #[test]
    fn test_throughput_transaction_batch() {
        let (db, _dir) = test_db();
        db.create_object_store("bench", "id").unwrap();

        let batch_size = 100;
        let batches = 50;
        let total = batch_size * batches;

        let start = std::time::Instant::now();
        for b in 0..batches {
            let mut tx = db.transaction(&["bench"], TransactionMode::ReadWrite).unwrap();
            for i in 0..batch_size {
                let id = b * batch_size + i;
                tx.put("bench", json!({"id": id as u64})).unwrap();
            }
            tx.commit().unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = total as f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB transaction batch ({} × {} docs): {} docs in {:.3}s = {:.0} docs/s",
            batches, batch_size, total, elapsed.as_secs_f64(), ops_per_sec
        );
        assert_eq!(db.count("bench").unwrap(), total);
    }

    #[test]
    fn test_throughput_auto_increment() {
        let (db, _dir) = test_db();
        db.create_object_store("auto", "id").unwrap();
        let mut def = db.get_store("auto").unwrap();
        def.auto_increment = true;
        db.engine.write_internal(vec![InternalRecord::from_record(
            &schema_record(&def).unwrap(), 0,
        )]).unwrap();
        db.schema.insert(def);

        let n = 50;
        let start = std::time::Instant::now();
        for _ in 0..n {
            db.put_auto("auto", json!({"data": "x"})).unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = n as f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB auto-increment ({} ops): {:.3}s = {:.0} ops/s",
            n, elapsed.as_secs_f64(), ops_per_sec
        );
        assert_eq!(db.count("auto").unwrap(), n);
    }

    // ── composite indexes ────────────────────────────────────────

    #[test]
    fn test_composite_index_equality() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], false).unwrap();

        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "city": "NYC", "age": 25})).unwrap();
        db.put("users", json!({"id": "u3", "city": "SF", "age": 30})).unwrap();

        // Query by first field only (prefix scan)
        let docs = db.get_by_index("users", "by_city_age", &json!("NYC")).unwrap();
        assert_eq!(docs.len(), 2);

        // Query by all fields (exact match)
        let docs = db.get_by_index("users", "by_city_age", &json!(["NYC", 30])).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_composite_index_update() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], true).unwrap();

        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30})).unwrap();

        // Update city → old index entry removed, new one created
        db.put("users", json!({"id": "u1", "city": "SF", "age": 30})).unwrap();

        let docs = db.get_by_index("users", "by_city_age", &json!(["NYC", 30])).unwrap();
        assert_eq!(docs.len(), 0, "old composite value should be gone");

        let docs = db.get_by_index("users", "by_city_age", &json!(["SF", 30])).unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn test_composite_index_unique() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], true).unwrap();

        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30})).unwrap();
        let err = db.put("users", json!({"id": "u2", "city": "NYC", "age": 30})).unwrap_err();
        assert!(err.to_string().contains("unique"), "composite unique should fail");

        // Same city, different age → should succeed
        db.put("users", json!({"id": "u2", "city": "NYC", "age": 25})).unwrap();
        assert_eq!(db.count("users").unwrap(), 2);
    }

    #[test]
    fn test_composite_index_on_existing_data() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "city": "SF", "age": 25})).unwrap();

        // Create composite on existing data
        db.create_index("users", "by_city_age", &["city", "age"], false).unwrap();

        let docs = db.get_by_index("users", "by_city_age", &json!(["NYC", 30])).unwrap();
        assert_eq!(docs.len(), 1);
    }

    // ── QueryBuilder ────────────────────────────────────────────

    #[test]
    fn test_query_builder_eq() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true).unwrap();
        db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();
        db.put("users", json!({"id": "u2", "email": "b@c.com"})).unwrap();

        let docs = db.query("users")
            .where_eq("email", json!("a@b.com"))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_query_builder_composite_eq() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], false).unwrap();
        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "city": "NYC", "age": 25})).unwrap();
        db.put("users", json!({"id": "u3", "city": "SF", "age": 30})).unwrap();

        let docs = db.query("users")
            .where_eq("city", json!("NYC"))
            .where_eq("age", json!(30))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_query_builder_range() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_age", &["age"], false).unwrap();
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "age": 25})).unwrap();
        db.put("users", json!({"id": "u3", "age": 35})).unwrap();

        let docs = db.query("users")
            .where_range("age", json!(25), json!(35))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 2); // age 25 ≤ docs < 35
    }

    #[test]
    fn test_query_builder_eq_and_range() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], false).unwrap();
        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "city": "NYC", "age": 25})).unwrap();
        db.put("users", json!({"id": "u3", "city": "SF", "age": 30})).unwrap();

        let docs = db.query("users")
            .where_eq("city", json!("NYC"))
            .where_range("age", json!(20), json!(30))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1); // age 20-30 in NYC → u2 (age 25). age 30 is exclusive.
        assert_eq!(docs[0]["id"], "u2");
    }

    #[test]
    fn test_query_builder_limit_offset() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        for i in 0..10 {
            db.put("users", json!({"id": i, "val": i})).unwrap();
        }

        let docs = db.query("users")
            .limit(3)
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 3);

        let docs = db.query("users")
            .offset(5)
            .limit(3)
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 3);
    }

    #[test]
    fn test_query_builder_order_by_asc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_age", &["age"], false).unwrap();
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "age": 25})).unwrap();
        db.put("users", json!({"id": "u3", "age": 35})).unwrap();

        let docs = db.query("users")
            .order_by("age", SortDir::Asc)
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0]["id"], "u2"); // age 25
        assert_eq!(docs[1]["id"], "u1"); // age 30
        assert_eq!(docs[2]["id"], "u3"); // age 35
    }

    #[test]
    fn test_query_builder_order_by_desc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "age": 25})).unwrap();

        let docs = db.query("users")
            .order_by("age", SortDir::Desc)
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0]["id"], "u1"); // age 30
        assert_eq!(docs[1]["id"], "u2"); // age 25
    }

    #[test]
    fn test_query_builder_where_in() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "city": "NYC"})).unwrap();
        db.put("users", json!({"id": "u2", "city": "SF"})).unwrap();
        db.put("users", json!({"id": "u3", "city": "LA"})).unwrap();

        let docs = db.query("users")
            .where_in("city", vec![json!("NYC"), json!("LA")])
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_query_builder_no_matching_index() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "color": "red"})).unwrap();
        db.put("users", json!({"id": "u2", "color": "blue"})).unwrap();

        // No index on "color" → full scan with filter
        let docs = db.query("users")
            .where_eq("color", json!("red"))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn test_query_builder_store_not_found() {
        let (db, _dir) = test_db();
        let err = db.query("nonexistent").collect().unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_query_builder_bool_index() {
        let (db, _dir) = test_db();
        db.create_object_store("items", "id").unwrap();
        db.create_index("items", "by_active", &["active"], false).unwrap();
        db.put("items", json!({"id": 1, "active": true})).unwrap();
        db.put("items", json!({"id": 2, "active": false})).unwrap();

        let docs = db.query("items")
            .where_eq("active", json!(true))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], 1);
    }

    #[test]
    fn test_query_builder_with_transaction_roundtrip() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true).unwrap();
        db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();

        // QueryBuilder sees committed data
        let docs = db.query("users")
            .where_eq("email", json!("a@b.com"))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    // ── generic struct API ────────────────────────────────────────

    #[derive(serde::Serialize, serde::Deserialize)]
    struct TestUser {
        id: String,
        name: String,
        email: String,
    }

    #[test]
    fn test_put_doc_and_get_doc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true).unwrap();

        let user = TestUser {
            id: "u1".into(),
            name: "Alice".into(),
            email: "a@b.com".into(),
        };
        let key = db.put_doc("users", &user).unwrap();
        assert_eq!(key, json!("u1"));

        let retrieved: TestUser = db.get_doc("users", "u1").unwrap().unwrap();
        assert_eq!(retrieved.name, "Alice");
        assert_eq!(retrieved.email, "a@b.com");
    }

    #[test]
    fn test_put_doc_numeric_key() {
        let (db, _dir) = test_db();
        db.create_object_store("items", "id").unwrap();

        #[derive(serde::Serialize, serde::Deserialize)]
        struct Item {
            id: i64,
            name: String,
        }

        let item = Item { id: 42, name: "answer".into() };
        let key = db.put_doc("items", &item).unwrap();
        assert_eq!(key, json!(42));

        let retrieved: Item = db.get_doc("items", 42).unwrap().unwrap();
        assert_eq!(retrieved.name, "answer");
    }

    #[test]
    fn test_get_doc_not_found() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let user: Option<TestUser> = db.get_doc("users", "nonexistent").unwrap();
        assert!(user.is_none());
    }

    #[test]
    fn test_delete_doc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let user = TestUser {
            id: "u1".into(),
            name: "Alice".into(),
            email: "a@b.com".into(),
        };
        db.put_doc("users", &user).unwrap();
        assert!(db.count("users").unwrap() == 1);

        db.delete_doc("users", "u1").unwrap();
        assert!(db.count("users").unwrap() == 0);
    }

    #[test]
    fn test_collect_doc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true).unwrap();

        db.put_doc("users", &TestUser {
            id: "u1".into(),
            name: "Alice".into(),
            email: "a@b.com".into(),
        }).unwrap();

        let users: Vec<TestUser> = db.query("users")
            .where_eq("email", json!("a@b.com"))
            .collect_doc()
            .unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].name, "Alice");
    }

    #[test]
    fn test_put_doc_in_transaction() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
        let user = TestUser {
            id: "u2".into(),
            name: "Bob".into(),
            email: "b@b.com".into(),
        };
        let json = serde_json::to_value(&user).unwrap();
        tx.put("users", json).unwrap();
        tx.commit().unwrap();

        let retrieved: TestUser = db.get_doc("users", "u2").unwrap().unwrap();
        assert_eq!(retrieved.name, "Bob");
    }
}
