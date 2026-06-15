//! Schema definitions for JsonDB object stores and indexes.

use crate::error::{FlowError, Result};
use crate::jsondb::encoding::{schema_key, schema_prefix, validate_name};
use crate::record::{InternalRecord, ScanRange};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub key_path: String,
    pub unique: bool,
    pub multi_entry: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreDef {
    pub name: String,
    pub key_path: String,
    pub auto_increment: bool,
    pub indexes: Vec<IndexDef>,
    pub next_auto_id: u64,
}

/// Thread-safe in-memory cache of all store schemas.
#[derive(Debug)]
pub struct Schema {
    stores: RwLock<HashMap<String, StoreDef>>,
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl Schema {
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, name: &str) -> Option<StoreDef> {
        self.stores.read().get(name).cloned()
    }

    pub fn insert(&self, def: StoreDef) {
        self.stores.write().insert(def.name.clone(), def);
    }

    pub fn remove(&self, name: &str) -> Option<StoreDef> {
        self.stores.write().remove(name)
    }

    pub fn list(&self) -> Vec<StoreDef> {
        self.stores.read().values().cloned().collect()
    }
}

/// Load all schemas from the engine by scanning the schema-key prefix.
pub(crate) fn load_schemas(
    scan_fn: impl Fn(ScanRange) -> crate::error::Result<Vec<crate::record::Record>>,
) -> Result<Schema> {
    let schema = Schema::new();
    let range = crate::jsondb::encoding::prefix_range(&schema_prefix());
    let records = scan_fn(range)?;
    for rec in records {
        let def: StoreDef = serde_json::from_slice(&rec.value).map_err(FlowError::from)?;
        schema.insert(def);
    }
    Ok(schema)
}

/// Validate and create a new store definition.
pub(crate) fn validate_store_def(name: &str, key_path: &str) -> Result<()> {
    validate_name(name)?;
    if key_path.is_empty() {
        return Err(FlowError::JsonDb("key_path cannot be empty".into()));
    }
    Ok(())
}

/// Validate and create a new index definition.
pub(crate) fn validate_index_def(
    store: &StoreDef,
    name: &str,
    key_path: &str,
) -> Result<()> {
    validate_name(name)?;
    if key_path.is_empty() {
        return Err(FlowError::JsonDb("index key_path cannot be empty".into()));
    }
    if store.indexes.iter().any(|i| i.name == name) {
        return Err(FlowError::JsonDb(format!(
            "index '{}' already exists in store '{}'",
            name, store.name
        )));
    }
    Ok(())
}

/// Serialize a StoreDef to a put record for the given store name.
pub(crate) fn schema_record(def: &StoreDef) -> Result<crate::record::Record> {
    Ok(crate::record::Record::new(
        schema_key(&def.name),
        0,
        serde_json::to_vec(def).map_err(FlowError::from)?,
    ))
}

/// Create a delete record for a store's schema.
pub(crate) fn schema_delete_record(store: &str) -> InternalRecord {
    InternalRecord::delete(schema_key(store), 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_basic() {
        let schema = Schema::new();
        let def = StoreDef {
            name: "users".into(),
            key_path: "id".into(),
            auto_increment: false,
            indexes: vec![IndexDef {
                name: "by_email".into(),
                key_path: "email".into(),
                unique: true,
                multi_entry: false,
            }],
            next_auto_id: 0,
        };
        schema.insert(def.clone());
        assert_eq!(schema.get("users").unwrap().name, "users");
        assert_eq!(schema.list().len(), 1);
        schema.remove("users");
        assert!(schema.get("users").is_none());
    }

    #[test]
    fn test_validate_store_def() {
        assert!(validate_store_def("users", "id").is_ok());
        assert!(validate_store_def("", "id").is_err());
        assert!(validate_store_def("users", "").is_err());
        assert!(validate_store_def("has space", "id").is_err());
    }

    #[test]
    fn test_validate_index_def_cases() {
        let store = StoreDef {
            name: "users".into(),
            key_path: "id".into(),
            auto_increment: false,
            indexes: vec![IndexDef {
                name: "existing".into(),
                key_path: "f".into(),
                unique: true,
                multi_entry: false,
            }],
            next_auto_id: 0,
        };
        // empty name
        assert!(validate_index_def(&store, "", "f").is_err());
        // empty key_path
        assert!(validate_index_def(&store, "new", "").is_err());
        // duplicate name
        assert!(validate_index_def(&store, "existing", "f").is_err());
        // valid
        assert!(validate_index_def(&store, "new", "f").is_ok());
    }

    #[test]
    fn test_schema_record_roundtrip() {
        let def = StoreDef {
            name: "t".into(),
            key_path: "id".into(),
            auto_increment: true,
            indexes: vec![],
            next_auto_id: 100,
        };
        let rec = schema_record(&def).unwrap();
        let decoded: StoreDef = serde_json::from_slice(&rec.value).unwrap();
        assert_eq!(decoded.name, "t");
        assert_eq!(decoded.next_auto_id, 100);
        assert!(decoded.auto_increment);
    }

    #[test]
    fn test_schema_delete_record_roundtrip() {
        let rec = schema_delete_record("test_store");
        assert_eq!(rec.key, schema_key("test_store"));
        assert_eq!(rec.op, crate::record::Op::Delete);
    }

    #[test]
    fn test_schema_list_empty() {
        let schema = Schema::new();
        assert!(schema.list().is_empty());
        assert!(schema.get("x").is_none());
    }

    #[test]
    fn test_schema_default() {
        let schema: Schema = Default::default();
        assert!(schema.list().is_empty());
    }
}
