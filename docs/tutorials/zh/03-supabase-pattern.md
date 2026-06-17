# FlowDB Supabase 模式 — 嵌入式认证与应用数据

[« 返回教程](../index.md)

---

### 目标

使用 FlowDB + JsonDB 在进程中模拟 Supabase 后端：用户认证、会话管理、行级安全（RLS）和业务数据。无需 PostgreSQL。

### 适用场景

- **Edge Functions / Serverless** — 将 FlowDB 直接嵌入函数，零延迟本地状态。
- **本地开发工具** — 用本地嵌入式数据库替代远端 Supabase 实例。
- **离线优先应用** — 设备端使用相同模式，在线时同步到远端 Supabase。
- **测试** — 无需 Docker 或网络，快速启动完全隔离的后端。

### 架构

```
用户存储  ────  会话存储  ────  待办存储
  │                                │
  └── email 唯一索引                ├── (user_id, status) 索引
       (get_user_by_email)          ├── (user_id, priority) 索引
                                     └── RLS: 按 user_id 过滤
```

### 分步指南

#### 1. 定义 Schema

```rust
use flowdb::jsondb::JsonDB;
use flowdb::Config;

let db = JsonDB::open(Config { data_dir: "./supa".into(), ..Default::default() }).unwrap();

db.create_object_store("users", "id").unwrap();
db.create_index("users", "by_email", &["email"], true).unwrap();

db.create_object_store("sessions", "token").unwrap();
db.create_index("sessions", "by_user", &["user_id"], false).unwrap();

db.create_object_store("todos", "id").unwrap();
db.create_index("todos", "by_user_status", &["user_id", "status"], false).unwrap();
db.create_index("todos", "by_user_priority", &["user_id", "priority"], false).unwrap();
```

#### 2. 注册（原子创建用户 + 会话）

使用显式事务原子化地创建用户和会话：

```rust
fn sign_up(db: &JsonDB, email: &str, password: &str) -> serde_json::Value {
    let user_id = uuid_v4();
    let token = uuid_v4();
    let mut tx = db.transaction(&["users", "sessions"], TransactionMode::ReadWrite).unwrap();
    tx.put("users", json!({"id": user_id, "email": email, "password": password})).unwrap();
    tx.put("sessions", json!({"token": token, "user_id": user_id})).unwrap();
    tx.commit().unwrap();
    json!({"user_id": user_id, "token": token})
}
```

#### 3. 验证会话

```rust
fn validate_session(db: &JsonDB, token: &str) -> Option<String> {
    let session = db.get("sessions", &json!(token)).unwrap()?;
    session.get("user_id").and_then(|v| v.as_str()).map(String::from)
}
```

#### 4. 行级安全（RLS）

所有查询按 `user_id` 过滤——这就是你的 RLS：

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

复合索引 `by_user_status` 使该查询高效——它直接在索引中定位到 `(user_id, "open")`。

#### 5. 写入时 RLS

在修改前检查所有权：

```rust
fn complete_todo(db: &JsonDB, user_id: &str, todo_id: &str) -> bool {
    let doc = match db.get("todos", &json!(todo_id)).unwrap() {
        Some(d) => d,
        None => return false,
    };
    if doc["user_id"] != user_id { return false; } // RLS
    let mut doc = doc;
    doc["status"] = json!("done");
    db.put("todos", doc).unwrap();
    true
}
```

#### 6. 唯一邮箱查询

```rust
fn get_user_by_email(db: &JsonDB, email: &str) -> Option<serde_json::Value> {
    db.get_by_index("users", "by_email", &json!(email)).unwrap().into_iter().next()
}
```

### 完整示例

参考 [`examples/supabase_example.rs`](https://github.com/restsend/flowdb/blob/main/examples/supabase_example.rs)。

运行：

```bash
cargo run --example supabase_example
```

### 扩展方向

| 概念 | 实现方式 |
|------|---------|
| **密码哈希** | 存储前用 `argon2` 或 `bcrypt` 哈希 |
| **会话过期** | 每次 `validate_session` 检查 `expires_at` |
| **分页** | 使用 `QueryBuilder::limit(n).offset(m)` |
| **软删除** | 添加 `"deleted": true` 字段 + 索引过滤 |
| **审计日志** | 追加写入的 `logs` 存储，使用 `put_auto` |
| **多租户** | 所有查询加上 `tenant_id` 前缀 + 复合索引 |
