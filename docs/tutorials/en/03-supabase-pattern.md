# FlowDB Supabase Pattern — Embedded Auth & App Data

[« Back to Tutorials](../index.md)

---

### Objective

Model a Supabase-like backend — user auth, session management, Row-Level Security (RLS), and application data — entirely in-process using FlowDB + JsonDB. No PostgreSQL required.

### When to Use This Pattern

- **Edge Functions / Serverless** — embed FlowDB directly in your Wasm or VM function for low-latency local state.
- **Local Dev Tooling** — replace a remote Supabase instance with a local embedded database during development.
- **Offline-First Apps** — use the same pattern on-device, sync to remote Supabase when online.
- **Testing** — spin up a fully isolated backend in tests without Docker or network.

### Architecture

```
User Store  ────  Session Store  ────  Todo Store
  │                                        │
  └── unique index on email                ├── index on (user_id, status)
       (get_user_by_email)                 ├── index on (user_id, priority)
                                           └── RLS: queries scoped by user_id
```

### Step-by-Step

#### 1. Define the Schema

```rust
use flowdb::jsondb::{JsonDB, StoreSchema};
use flowdb::Config;

let db = JsonDB::open(Config { data_dir: "./supa".into(), ..Default::default() }).unwrap();

// Auth + App data — all schema in one place
db.apply_schemas(&[
    StoreSchema::new("users", "id")
        .with_index("by_email", &["email"], true),
    StoreSchema::new("sessions", "token")
        .with_index("by_user", &["user_id"], false),
    StoreSchema::new("todos", "id")
        .with_index("by_user_status", &["user_id", "status"], false)
        .with_index("by_user_priority", &["user_id", "priority"], false),
]).unwrap();
```

#### 2. Sign Up (Atomic User + Session)

Use an explicit transaction to atomically create a user and a session:

```rust
use flowdb::jsondb::TransactionMode;
use serde_json::json;

fn sign_up(db: &JsonDB, email: &str, password: &str) -> serde_json::Value {
    let user_id = uuid_v4();
    let token = uuid_v4();

    let mut tx = db.transaction(&["users", "sessions"], TransactionMode::ReadWrite).unwrap();

    tx.put("users", json!({
        "id": user_id,
        "email": email,
        "password": password,  // real app: hash this!
        "created_at": now_iso(),
    })).unwrap();

    tx.put("sessions", json!({
        "token": token,
        "user_id": user_id,
        "expires_at": "2026-12-31T23:59:59Z",
    })).unwrap();

    tx.commit().unwrap();
    json!({"user_id": user_id, "token": token})
}
```

#### 3. Validate Session

```rust
fn validate_session(db: &JsonDB, token: &str) -> Option<String> {
    let session = db.get("sessions", &json!(token)).unwrap()?;
    session.get("user_id").and_then(|v| v.as_str()).map(String::from)
}
```

#### 4. Row-Level Security (RLS)

Every query filters by `user_id` — this is your RLS:

```rust
fn my_open_todos(db: &JsonDB, user_id: &str) -> Vec<serde_json::Value> {
    db.query("todos")
        .where_eq("user_id", json!(user_id))
        .where_eq("status", json!("open"))
        .order_by("priority", flowdb::jsondb::SortDir::Desc)
        .collect()
        .unwrap()
}
```

The compound index `by_user_status` makes this query efficient — it seeks directly to `(user_id, "open")` within the index.

#### 5. RLS on Writes

Check ownership before mutating:

```rust
fn complete_todo(db: &JsonDB, user_id: &str, todo_id: &str) -> bool {
    let doc = match db.get("todos", &json!(todo_id)).unwrap() {
        Some(d) => d,
        None => return false,
    };
    if doc["user_id"] != user_id {
        return false; // RLS: not owner
    }
    let mut doc = doc;
    doc["status"] = json!("done");
    db.put("todos", doc).unwrap();
    true
}
```

#### 6. Unique Email Lookup

```rust
fn get_user_by_email(db: &JsonDB, email: &str) -> Option<serde_json::Value> {
    db.get_by_index("users", "by_email", &json!(email))
        .unwrap()
        .into_iter()
        .next()
}
```

### Full Working Example

See [`examples/supabase_example.rs`](https://github.com/restsend/flowdb/blob/main/examples/supabase_example.rs).

Run it:

```bash
cargo run --example supabase_example
```

### Going Further

| Concept | How to Implement |
|---------|-----------------|
| **Password hashing** | Hash with `argon2` or `bcrypt` before storing |
| **Session expiry** | Check `expires_at` on each `validate_session` call |
| **Pagination** | Use `QueryBuilder::limit(n).offset(m)` |
| **Soft delete** | Add `"deleted": true` field + index filter |
| **Audit log** | Append-only `logs` store with auto-increment (`put_auto`) |
| **Multi-tenant** | Prefix all queries with `tenant_id` + compound index |
