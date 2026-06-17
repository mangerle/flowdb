# FlowDB JsonDB Tutorial — Document Store with Indexes

[« Back to Tutorials](../index.md)

---

### Objective

Learn how to use FlowDB's built-in JsonDB layer — a JSON document store with ACID transactions, secondary indexes, and a serde-compatible API.

### Prerequisites

```toml
[dependencies]
flowdb = "0.6"
serde_json = "1"
serde = { version = "1", features = ["derive"] }
tempfile = "3"
```

### Step-by-Step

#### 1. Open JsonDB

```rust
use flowdb::jsondb::JsonDB;
use flowdb::Config;

let db = JsonDB::open(Config {
    data_dir: "./my_jsondb".into(),
    ..Default::default()
}).unwrap();
```

#### 2. Create an Object Store

```rust
db.create_object_store("users", "id").unwrap();
```

The second argument `"id"` specifies the **key path** — the document field used as the primary key.

#### 3. Create Secondary Indexes

```rust
db.create_index("users", "by_email", &["email"], true).unwrap();       // unique
db.create_index("users", "by_city_age", &["city", "age"], false).unwrap(); // composite
```

- `unique: true` — the index enforces uniqueness.
- Multiple `key_paths` create a **composite index** (e.g. `["city", "age"]`).

#### 4. Insert Documents

```rust
use serde_json::json;

db.put("users", json!({"id": "u1", "email": "alice@ex.com", "age": 30, "city": "NYC"})).unwrap();
db.put("users", json!({"id": "u2", "email": "bob@ex.com",   "age": 25, "city": "NYC"})).unwrap();
```

Indexes are maintained automatically on every `put` and `delete`.

#### 5. Point Get

```rust
let doc = db.get("users", &json!("u1")).unwrap();
```

#### 6. Index Lookup (Equality)

```rust
let docs = db.get_by_index("users", "by_email", &json!("alice@ex.com")).unwrap();
```

For composite indexes, pass an array:

```rust
let docs = db.get_by_index("users", "by_city_age", &json!(["NYC", 30])).unwrap();
```

#### 7. Index Range Query

```rust
let docs = db.range_by_index("users", "by_email", &json!("a"), &json!("z")).unwrap();
```

#### 8. QueryBuilder (Predicates + Sort + Limit)

```rust
let docs = db.query("users")
    .where_eq("city", json!("NYC"))
    .where_range("age", json!(25), json!(35))
    .order_by("age", flowdb::jsondb::SortDir::Asc)
    .limit(10)
    .collect()
    .unwrap();
```

The query planner automatically picks the best index for the given filters.

#### 9. Typed Serde API

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: String,
    email: String,
    age: u32,
    city: String,
}

let user = User { id: "u3".into(), email: "carol@ex.com".into(), age: 28, city: "SF" };
db.put_doc("users", &user).unwrap();

let back: Option<User> = db.get_doc("users", "u3").unwrap();
let users: Vec<User> = db.query("users").where_eq("age", json!(28)).collect_doc().unwrap();
```

#### 10. Transactions

```rust
use flowdb::jsondb::TransactionMode;

let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();

// All writes are buffered until commit
tx.put("users", json!({"id": "u4", "email": "dave@ex.com", "age": 35, "city": "LA"})).unwrap();
tx.delete("users", &json!("u1")).unwrap();

// Atomic commit — all or nothing
tx.commit().unwrap();
```

- Dropping a transaction without `commit` aborts all buffered writes.
- `TransactionMode::ReadOnly` rejects writes.

#### 11. Count & Scan

```rust
let count = db.count("users").unwrap();
let all = db.scan("users").unwrap();
```

#### 12. Schema Inspection

```rust
println!("stores: {:?}", db.store_names());
let store = db.get_store("users").unwrap();
println!("key_path: {}", store.key_path);
for idx in &store.indexes {
    println!("  index {} fields={:?} unique={}", idx.name, idx.key_paths, idx.unique);
}
```

#### 13. Shutdown

```rust
db.shutdown().unwrap();
```

### Full Working Example

See [`examples/basic_jsondb.rs`](https://github.com/restsend/flowdb/blob/main/examples/basic_jsondb.rs).

Run it:

```bash
cargo run --example basic_jsondb
```
