//! JsonDB – A JSON document database interface built on top of FlowDB.
//!
//! Provides an IndexedDB-like API with ACID transactions, secondary indexes,
//! and auto-increment support.

pub(crate) mod db;
pub(crate) mod helpers;
pub(crate) mod query;
pub(crate) mod transaction;
#[cfg(test)]
mod tests;

mod encoding;
mod schema;

use serde_json::Value;

pub use db::JsonDB;
pub use schema::{IndexDef as IndexSchema, StoreDef as StoreSchema};
pub use transaction::Transaction;
pub use query::{QueryBuilder, SortDir};

/// Trait for types that can be used as a primary key argument.
///
/// Implemented for `&str`, `String`, `i64`, `i32`, `u64`, `u32`, `Value`, and `&Value`.
pub trait KeyArg {
    fn into_value(self) -> Value;
}

impl KeyArg for &str { fn into_value(self) -> Value { Value::String(self.to_string()) } }
impl KeyArg for String { fn into_value(self) -> Value { Value::String(self) } }
impl KeyArg for i64 { fn into_value(self) -> Value { Value::Number(self.into()) } }
impl KeyArg for i32 { fn into_value(self) -> Value { Value::Number((self as i64).into()) } }
impl KeyArg for u64 { fn into_value(self) -> Value { Value::Number(self.into()) } }
impl KeyArg for u32 { fn into_value(self) -> Value { Value::Number((self as u64).into()) } }
impl KeyArg for Value { fn into_value(self) -> Value { self } }
impl KeyArg for &Value { fn into_value(self) -> Value { self.clone() } }

/// Transaction mode (read-only vs read-write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Read-only — queries only.
    ReadOnly,
    /// Read-write — queries, puts, deletes, and index updates.
    ReadWrite,
}
