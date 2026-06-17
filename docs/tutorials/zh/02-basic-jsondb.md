# FlowDB JsonDB 教程 — 文档存储与索引

[« 返回教程](../index.md)

---

### 目标

学习使用 FlowDB 内置的 JsonDB JSON 文档存储层，包括索引、事务、序列化集成等功能。

### 前置条件

```toml
[dependencies]
flowdb = "0.6"
serde_json = "1"
serde = { version = "1", features = ["derive"] }
tempfile = "3"
```

### 分步指南

#### 1. 打开 JsonDB

```rust
use flowdb::jsondb::JsonDB;
use flowdb::Config;

let db = JsonDB::open(Config {
    data_dir: "./my_jsondb".into(),
    ..Default::default()
}).unwrap();
```

#### 2. 创建对象存储

```rust
db.create_object_store("users", "id").unwrap();
```

`"id"` 指定了**键路径**，即文档中作为主键的字段。

> **v0.7 新增：** 也可以使用 [`StoreDef` 构建器](#storedef-构建器) 或
> [`#[derive(ObjectStore)]`](#derive-宏) 宏 — 见下文。

#### 3. 创建二级索引

```rust
db.create_index("users", "by_email", &["email"], true).unwrap();        // 唯一索引
db.create_index("users", "by_city_age", &["city", "age"], false).unwrap(); // 复合索引
```

- `unique: true` 强制索引唯一性。
- 多个 `key_paths` 创建**复合索引**。

#### 4. 插入文档

```rust
use serde_json::json;

db.put("users", json!({"id": "u1", "email": "alice@ex.com", "age": 30, "city": "NYC"})).unwrap();
```

索引会在每次 `put` 和 `delete` 时自动维护。

#### 5. 精确查找

```rust
let doc = db.get("users", &json!("u1")).unwrap();
```

#### 6. 索引等值查询

```rust
let docs = db.get_by_index("users", "by_email", &json!("alice@ex.com")).unwrap();
```

复合索引传入数组：

```rust
let docs = db.get_by_index("users", "by_city_age", &json!(["NYC", 30])).unwrap();
```

#### 7. 索引范围查询

```rust
let docs = db.range_by_index("users", "by_email", &json!("a"), &json!("z")).unwrap();
```

#### 8. QueryBuilder（谓词 + 排序 + 分页）

```rust
let docs = db.query("users")
    .where_eq("city", json!("NYC"))
    .where_range("age", json!(25), json!(35))
    .order_by("age", flowdb::jsondb::SortDir::Asc)
    .limit(10)
    .collect()
    .unwrap();
```

查询规划器会自动为给定条件选择最优索引。

#### 9. Serde 类型安全 API

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: String,
    email: String,
    age: u32,
    city: String,
}

db.put_doc("users", &user).unwrap();
let back: Option<User> = db.get_doc("users", "u3").unwrap();
```

#### 10. 事务

```rust
use flowdb::jsondb::TransactionMode;

let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
tx.put("users", json!({"id": "u4", "email": "dave@ex.com", "age": 35, "city": "LA"})).unwrap();
tx.delete("users", &json!("u1")).unwrap();
tx.commit().unwrap(); // 原子提交
```

- 不调用 `commit` 直接 drop 事务会丢弃所有缓冲写入。
- `TransactionMode::ReadOnly` 拒绝写入操作。

#### 11. 计数与全量扫描

```rust
let count = db.count("users").unwrap();
let all = db.scan("users").unwrap();
```

#### 12. Schema 自省

```rust
let store = db.get_store("users").unwrap();
for idx in &store.indexes {
    println!("  {} fields={:?} unique={}", idx.name, idx.key_paths, idx.unique);
}
```

#### 13. 关闭

```rust
db.shutdown().unwrap();
```

---

## StoreDef 构建器（v0.7+）

使用 `StoreDef` 构建器将整个 schema 定义集中在一处：

```rust
use flowdb::jsondb::StoreSchema;

let schema = StoreSchema::new("users", "id")
    .with_index("by_email", &["email"], true)
    .with_index("by_city_age", &["city", "age"], false);

db.apply_store(&schema).unwrap();
```

`apply_store` 是幂等的——可以安全地在每次启动时调用。它会自动：
- 创建不存在的 store
- 创建缺失的索引（对已有数据做 backfill）
- 删除多余的索引

批量应用：

```rust
db.apply_schemas(&[
    StoreSchema::new("users", "id").with_index("by_email", &["email"], true),
    StoreSchema::new("posts", "id").with_index("by_author", &["author_id"], false),
]).unwrap();
```

---

## Derive 宏（v0.7+）

使用 `#[derive(ObjectStore)]` 从 struct 定义自动生成 `StoreDef`：

```rust
use flowdb::ObjectStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, ObjectStore)]
#[store(name = "users", key_path = "id")]
struct User {
    id: String,
    #[index(name = "by_email", unique)]
    email: String,
    #[index(name = "by_age")]
    age: u32,
    city: String,
}
```

一键应用：

```rust
db.apply_schema::<User>().unwrap();
```

### 容器属性

| 属性 | 说明 |
|------|------|
| `key_path = "..."` | **必需。** 主键字段路径 |
| `name = "..."` | Store 名称（默认 struct 名） |
| `auto_increment` | 启用自增主键 |

### 字段属性

| 属性 | 说明 |
|------|------|
| `#[index]` | 创建非唯一索引 |
| `#[index(unique)]` | 创建唯一索引 |
| `#[index(name = "custom")]` | 自定义索引名（默认字段名） |

### 完整示例

参考 [`examples/basic_jsondb.rs`](https://github.com/restsend/flowdb/blob/main/examples/basic_jsondb.rs)。

运行：

```bash
cargo run --example basic_jsondb
```
