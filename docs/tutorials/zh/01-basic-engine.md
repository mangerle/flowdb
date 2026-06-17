# FlowDB Engine 教程 — LSM 引擎入门

[« 返回教程](../index.md)

---

### 目标

学习使用 FlowDB LSM 存储引擎的基本操作：打开、写入、读取、查询、删除、刷盘、合并和关闭。

### 前置条件

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
flowdb = "0.6"
tempfile = "3"
```

### 分步指南

#### 1. 打开引擎

```rust
use flowdb::{Config, Engine};

let config = Config {
    data_dir: "./my_data".into(),
    auto_background: true,   // 自动刷盘、合并、GC
    ..Default::default()
};
let engine = Engine::open(config).unwrap();
```

- `create_if_missing: true`（默认）会在目录不存在时自动创建。
- `auto_background: true` 会启动一个后台 OS 线程，定时执行刷盘、合并和垃圾回收。

#### 2. 写入数据

每条记录包含 `key`（二进制）、`ts`（微秒级时间戳）、`expire_at` 和 `value`（二进制）：

```rust
use flowdb::Record;

let records = vec![
    Record::new("sensor:temp", 1_700_000_000_000_000, b"22.5".to_vec()),
    Record::new("sensor:hum",  1_700_000_000_001_000, b"60%".to_vec()),
];
engine.write_batch_owned(records).unwrap();
```

#### 3. 精确查找

```rust
let rec = engine.get("sensor:temp", 1_700_000_000_000_000).unwrap();
```

#### 4. 获取最新版本

```rust
let latest = engine.get_latest("sensor:temp").unwrap();
```

返回 `ts` 值最大的记录。

#### 5. 前缀查询

```rust
use flowdb::Query;
let results = engine.query(Query::prefix("sensor:")).unwrap();
```

#### 6. 键范围查询

```rust
let results = engine.query(Query::key_range("sensor:a", "sensor:z")).unwrap();
```

#### 7. 时间范围查询

```rust
let results = engine.query(Query::time_range(start_ts, end_ts)).unwrap();
```

#### 8. 组合查询（前缀 + 时间范围）

```rust
let results = engine.query(Query::prefix_time_range("sensor:", start_ts, end_ts)).unwrap();
```

#### 9. 懒扫描迭代器

适合大数据集：

```rust
use flowdb::ScanRange;
let mut iter = engine.scan(ScanRange::prefix("sensor:")).unwrap();
while let Some(Ok(rec)) = iter.next() {
    // 逐条处理，不一次性加载所有数据
}
```

#### 10. 删除

```rust
engine.delete_batch(&[("sensor:temp".into(), 1_700_000_000_000_000)]).unwrap();
engine.delete_range("sensor:old_a", "sensor:old_z").unwrap();
```

#### 11. 刷盘与合并

```rust
engine.flush().unwrap();              // 内存表 → SST 文件
engine.trigger_compaction().unwrap(); // 合并 SST 文件
engine.trigger_gc().unwrap();         // 清理已过期的 SST
```

#### 12. 统计信息

```rust
let s = engine.stats();
println!("{} 个 SST 文件，写入 {} MB", s.sstable_count, s.total_bytes_written / 1024 / 1024);
```

#### 13. 关闭

```rust
engine.shutdown().unwrap();
```

### 完整示例

参考 [`examples/basic_engine.rs`](https://github.com/restsend/flowdb/blob/main/examples/basic_engine.rs)。

运行：

```bash
cargo run --example basic_engine
```
