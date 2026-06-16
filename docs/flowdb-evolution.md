# 做减法的艺术：FlowDB 从全功能数据库到超越 RocksDB 的嵌入式引擎

> 一个 Rust 存储引擎的演进故事：从 v0.1.5 到 v0.3.0，我们砍掉了 HTTP 服务器、UDP 协议、Admin UI、Auth 模块、甚至 async runtime 本身。每一次做减法，性能都更好、代码都更简单。本文记录这一路的技术决策和背后的思考。

## 一、做减法的勇气：从"什么都做"到"只做一件事"

### v0.1.5：一个大而全的时序数据库

2026 年初，FlowDB v0.1.5 发布时，它的定位是"高性能时序数据库"。这句话意味着什么？意味着它要和 InfluxDB、TDengine、QuestDB 竞争。所以 v0.1.5 的架构是这样的：

![架构演进](svg/architecture-evolution.svg)

```
┌─────────────────────────────────────────────────────┐
│                  FlowDB v0.1.5                      │
├─────────────┬─────────────┬─────────────────────────┤
│  HTTP Server│  UDP Server │    Admin UI (HTML)      │
│  (axum)     │  (V1/V2)    │    + Auth (API Key)     │
├─────────────┴─────────────┴─────────────────────────┤
│              ServerConfig (TOML)                    │
├─────────────────────────────────────────────────────┤
│              Engine (async API)                     │
│  ┌──────────┬──────────┬──────────┬───────────────┐ │
│  │ MemTable │  WAL     │ SSTable  │ Compaction    │ │
│  │(BTreeMap)│(SipHash) │          │               │ │
│  └──────────┴──────────┴──────────┴───────────────┘ │
├─────────────────────────────────────────────────────┤
│              tokio runtime (full)                   │
└─────────────────────────────────────────────────────┘
```

它有：
- **HTTP 服务**：基于 axum，提供写入、查询、管理端点
- **UDP 协议**：V1/V2 两种帧格式，带鉴权和速率限制
- **Admin UI**：内嵌 HTML 管理界面
- **Auth 模块**：API Key 鉴权，HTTP 和 Admin 共享
- **TOML 配置**：`ServerConfig` 包含 `http_addr`、`udp_addr`、`api_keys` 等

这些加起来大概 3000+ 行代码和 4 个额外依赖（axum、tower-http、base64、tokio full）。

### 问题出在哪里？

问题在于，**我们同时在做两件完全不同的事情**：

1. **存储引擎**——LSM-tree、WAL、SSTable、Compaction、Bloom Filter、Block Cache
2. **网络服务**——HTTP 路由、UDP 帧编解码、HTML 模板、API Key 鉴权

这两件事的技能树、测试策略、性能瓶颈、用户期望完全不同。存储引擎的核心竞争力是**写入吞吐和查询延迟**，而网络服务需要的是**连接管理、协议兼容、安全加固**。

当我们 benchmark 发现写入吞吐只有 RocksDB 的 63% 时，瓶颈在 MemTable 的 BTreeMap 插入开销，而不是 HTTP 路由。但我们花了大量精力在 UDP 速率限制器、Admin 端点鉴权这些与存储引擎核心竞争力无关的事情上。

### v0.2.1：砍掉整个 server 层

决定很果断：**FlowDB 只做嵌入式存储引擎，不做网络服务**。

```
v0.1.5                          v0.2.1
┌────────────────────┐          ┌────────────────────┐
│  HTTP / UDP / Auth │  ← 删除  │                    │
│  Admin UI          │          │                    │
├────────────────────┤          ├────────────────────┤
│  Engine (async)    │          │  Engine (async)    │
│  MemTable / WAL    │  保留    │  MemTable / WAL    │
│  SSTable / Compact │          │  SSTable / Compact │
├────────────────────┤          ├────────────────────┤
│  tokio runtime     │          │  tokio runtime     │
└────────────────────┘          └────────────────────┘
```

删除了什么：
- `src/http.rs` — axum 路由、handler、JSON 序列化
- `src/udp.rs` — UDP 帧编解码、V1/V2 协议
- `src/admin.rs` — 内嵌 HTML 管理界面
- `src/auth.rs` — API Key 鉴权
- `src/bin/flowdb-server.rs` — 服务端二进制
- `tests/http_integration.rs` — 32 个 HTTP 集成测试
- `tests/network_integration.rs` — 15 个网络集成测试
- `ServerConfig`、`server` feature、axum/tower-http/base64 依赖

这大概是整个代码库的 40%。删完之后，代码更清晰了，编译更快了，测试更聚焦了。

> **架构师最难的决定不是"加什么"，而是"砍什么"。** 每一行代码都是负债——维护成本、测试成本、认知负担。砍掉不创造核心价值的代码，是提升工程效率最有效的方式。

## 二、正确性是一切的基石

### 在做性能优化之前，先确保功能是对的

v0.1.5 虽然功能"大而全"，但存在几个严重的正确性问题。我们在 v0.2.0 集中修复了它们。

### P0：MemTable::get 返回已删除的数据

这是最严重的 bug。当用户先写入一条记录，再删除同一个 `(key, ts)`，随后的 `get` 查询竟然返回了已删除的数据。

**根因**：v0.1.5 的 `MemTable` 基于 `BTreeMap<(key, ts, seq), Record>`。`get` 方法用迭代器 `.next()` 取**第一个**匹配的记录，而 BTreeMap 按 `seq` 升序排列，所以取到的是**最旧**的版本。

但写入时 seq 是单调递增的——新写入的 delete tombstone 有更高的 seq，应该在查询时优先返回。正确做法是取**最后一个**匹配记录，即 `.rev().next()`。

```rust
// Bug: 返回 seq 最小的记录（最旧的版本）
records.range(start..=end).next()

// Fix: 返回 seq 最大的记录（最新的版本）
records.range(start..=end).rev().next()
```

这个 bug 意味着任何 "write → delete → read" 序列都会返回错误结果。对于数据库来说，这是致命的。

### P1：WAL 截断逻辑错误

WAL（Write-Ahead Log）在 memtable flush 到 SSTable 后应该被截断，释放磁盘空间。但 v0.1.5 的截断条件是 `max_seq < seq`，这意味着只要 WAL 中有任何一条记录的 seq >= 已 flush 的 max_seq，整个 segment 就不会被清理。

实际上应该是 `max_seq <= seq`——当 segment 的最大 seq 小于等于已 flush 的 seq 时，说明所有记录都已持久化到 SSTable，可以安全删除。

这个 bug 导致的后果：写入 50K 条记录后 flush，WAL 文件仍然占用 35.8MB。修复后降到 2.0MB。

### 方法论：系统性硬化

v0.2.0 不只是修了两个 bug，而是做了一次系统性的审查（P0-P3）：

| 级别 | 问题 | 修复 |
|------|------|------|
| P0 | MemTable::get 返回已删除数据 | `.rev().next()` 取最新 seq |
| P1 | WAL 永不截断 | `< seq` → `<= seq` |
| P2 | Config 无校验，0 值导致除零 | 添加 `Config::validate()` |
| P2 | shutdown 不 flush，数据丢失 | `shutdown()` 先 flush 再退出 |
| P3 | 无 frozen memtable 背压 | 超过 `max_frozen_memtables` 时写阻塞 |
| P3 | SST 读取器不清理过期引用 | GC/Compaction 后 `evict_stale_readers()` |
| P3 | WAL 无单条记录校验和 | 每条记录附加 FxHash 校验和 |

> **先修正确性，再做性能优化。** 一个快的数据库如果返回错误结果，那它毫无价值。我们在性能优化之前，花了整整一个版本确保每一行数据都能正确写入、查询、删除和恢复。

## 三、Benchmark 驱动的性能突围

### v0.2.2 的起点：比 RocksDB 慢

v0.2.0 修完 bug 后，我们做了第一次正式的 FlowDB vs RocksDB benchmark。结果不理想：

| 类别 | FlowDB v0.2.0 | RocksDB | 差距 |
|------|---------------|---------|------|
| 顺序写入 | 2.0M ops/s | 3.1M ops/s | 慢 1.58x |
| 并发写入 | 3.2M ops/s | 4.4M ops/s | 慢 1.38x |
| 点查询 | 4.7M ops/s | 539K ops/s | **快 8.7x** |
| 前缀扫描 | 71K ops/s | 11K ops/s | **快 6.3x** |

读取已经碾压 RocksDB，但写入明显落后。问题出在哪里？

### 诊断：BTreeMap 是写入瓶颈

用 profiler 分析写入热点，发现 60% 的时间花在 `BTreeMap::insert` 上。原因很直接：

```rust
// v0.1.5 的写入路径
memtable.insert((rec.key.clone(), rec.ts, seq), rec);
//                ^^^^^^^^^^^^^^^^
//                每条记录都要 clone 一份 key 作为 BTreeMap 的排序键
```

`BTreeMap` 的 `insert` 需要：
1. **Clone key**（`Vec<u8>` 深拷贝）
2. **树遍历**（O(log n) 比较 + 平衡）
3. **可能的节点分裂**（内存分配）

对于 128 字节的 key，每次 clone 就是一次 `malloc + memcpy`。写入 100K 条记录 = 100K 次 key clone = 100K 次 malloc。

### 优化 1：Vec 替代 BTreeMap

核心洞察：**active memtable 不需要排序**。排序可以推迟到 freeze 时一次性做。

![写入路径优化](svg/write-path-optimization.svg)

```
BTreeMap 方案 (v0.1.5):              Vec 方案 (v0.2.2):
                                     
每次 insert:                         每次 push:
  1. clone key (~100B)                 1. push record (移动，无拷贝)
  2. BTreeMap 遍历 O(log n)            2. 摊还 O(1)
  3. 可能触发节点分裂                   
                                    
查询: O(log n)                        查询: O(n) 线性扫描
冻结: O(1)                            冻结: O(n log n) 原地排序
```

写入是热路径（每次 `write_batch` 都走），冻结是冷路径（只在 memtable 满时触发一次）。把排序从热路径移到冷路径，是典型的**延迟计算**优化。

```rust
// v0.2.2 的写入路径
active.push(rec);  // O(1)，无 key clone，无树遍历

// freeze 时排序
fn freeze(&mut self) {
    self.active.sort();  // 原地排序，O(n log n)
    // ... swap 到 frozen BTreeMap
}
```

### 优化 2：async 包装的隐性开销

v0.1.5 的写入方法是 `async fn`：

```rust
pub async fn write_batch(&self, batch: &[Record]) -> Result<()> {
    // ...
    self.do_write(records).await
}

async fn do_write(&self, records: Vec<InternalRecord>) -> Result<()> {
    // 全是同步代码！没有 .await！
    self.worker.lock().process_batch_encoded(...);
    Ok(())
}
```

审计发现：**`do_write` 的函数体里没有任何 `.await`**。它完全是同步的——获取锁、写 WAL、写 memtable，全是阻塞操作。但因为它被声明为 `async fn`，编译器为它生成了一个状态机（`Future`），每次调用都要经历 `poll` → `Pending`/`Ready` 的状态转换。

对于写入这种微秒级的操作，状态机的开销不可忽视。把 `write_batch` 从 `async fn` 改成 `fn`，直接消除了这部分开销。

### 优化 3：FxHash 替代 SipHash

WAL 的每条记录都需要校验和（checksum）来检测磁盘损坏。v0.1.5 用的是 Rust 标准库的 `DefaultHasher`（SipHash-1-3），这是一种加密级哈希函数。

但 WAL 校验和的场景是**检测磁盘损坏**，不是**防御恶意篡改**。SipHash 的安全性在这里完全用不上，而它的速度比非加密哈希慢约 10 倍。

我们实现了一个 FxHash 风格的快速哈希：

```rust
const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;
fn fxhash(val: u64) -> u64 {
    (val.rotate_left(5) ^ val).wrapping_mul(SEED)
}
```

对于 100 字节的输入，SipHash 约 300ns，FxHash 约 30ns。写入 100K 条记录 = 节省 27ms，对于微秒级操作来说是显著的。

### 优化 4：WAL 截断修复（已在 v0.2.0 修复但效果在此显现）

v0.2.0 修复了 `max_seq < seq` → `max_seq <= seq` 后，flush 后 WAL 从 35.8MB 降到 2.0MB。这不只是省磁盘——更小的 WAL 意味着更少的 I/O，更快的恢复。

### v0.2.2 Benchmark 结果

![性能对比](svg/benchmark-comparison.svg)

| 类别 | v0.2.0 | v0.2.2 | RocksDB | vs RocksDB |
|------|--------|--------|---------|------------|
| 顺序写入 | 2.0M | **4.5M** | 3.1M | **快 1.42x** |
| 并发写入 | 3.2M | **9.4M** | 4.7M | **快 2.02x** |
| 点查询 | 4.7M | **6.0M** | 549K | **快 10.95x** |
| 前缀扫描 | 71K | **72K** | 11K | **快 6.39x** |
| 全表扫描 | 73 | **65** | 40 | **快 1.63x** |

从全面落后到全面领先。

> **Benchmark 是性能优化的指南针。** 没有 profiler，我们不会知道 60% 的时间花在 key clone 上；没有 RocksDB 对照，我们不会知道"慢 1.58x"需要优化。每一次改动后跑一次 benchmark，让数据驱动决策。

## 四、回归本质：彻底去掉 async

### 触发点：一次完整的 async 审计

v0.2.2 把写入方法改成了同步，但读取方法（`get`、`query`、`flush`、`shutdown`）仍然是 async。这造成了一个尴尬的局面：用户需要启动 tokio runtime 才能调用 `engine.open()` 和 `engine.get()`，即使他们可能在同步代码中使用 FlowDB。

于是我们做了一次彻底的审计：**哪些方法真正需要 async？**

| 方法 | 内部有 `.await`？ | 用了 `spawn_blocking`？ | 结论 |
|------|-------------------|------------------------|------|
| `open` | 否 | 否 | 假 async |
| `query` | 否 | 否 | 假 async |
| 5× `query_*` | 仅委托 `query` | 否 | 假 async |
| `get` | 否 | 否 | 假 async（已有 `get_sync`） |
| `get_latest_async` | 否 | 否 | 假 async（已有 `get_latest`） |
| `shutdown` | 否 | 否 | 假 async |
| `flush` | 否 | **是** | spawn_blocking 包装 |
| `trigger_gc` | 否 | **是** | spawn_blocking 包装 |
| `trigger_compaction` | 否 | **是** | spawn_blocking 包装 |
| `close` | 否 | **是** | spawn_blocking 包装 |

**12 个 async 方法，8 个是"假 async"**——函数体里没有任何 `.await`，async 关键字纯粹是 API 一致性的产物。另外 4 个用了 `spawn_blocking`，但 `spawn_blocking` 的本质是"把同步操作丢到线程池"——既然操作本身是同步的，为什么要在 API 层面包一层 async？

![Async 审计](svg/async-audit.svg)

### async 为什么成了负担

async 在 FlowDB 中造成的问题：

**1. 强制运行时依赖**

```rust
// v0.2.2: 必须在 tokio runtime 内才能调用
#[tokio::main]
async fn main() {
    let engine = Engine::open(config).await?;  // 需要 runtime
    engine.write_batch(&records)?;              // 这个已经是同步的了
    let results = engine.query_by_prefix("k").await?;  // 又需要 runtime
}
```

用户如果在 `async-std` 或 `smol` 项目中用 FlowDB，就需要 `tokio::runtime::Handle::current()` 来桥接，增加了集成复杂度。

**2. 隐性性能开销**

每个 async fn 都会被编译成状态机。即使函数体是同步的，调用时仍然要走 `Future::poll` 流程。对于 `get()` 这种纳秒级操作，状态机开销占比不可忽视。

**3. 测试复杂度**

所有测试都必须标 `#[tokio::test] async fn`，并发测试必须用 `tokio::spawn`。这使得测试代码无法在非 tokio 环境中运行。

### 重构方案

核心思路：**所有 public 方法改为同步，后台维护改用 `std::thread`**。

```
v0.2.2 (async)                      v0.3.0 (sync)
                                    
Engine::open().await          →     Engine::open()
engine.get().await            →     engine.get()
engine.flush().await          →     engine.flush()
engine.shutdown().await       →     engine.shutdown()
                                    
后台维护:                             后台维护:
tokio::spawn {                       std::thread::spawn {
  tokio::select! {                     loop {
    flush_tick → spawn_blocking          if stop_flag { break }
    compact_tick → spawn_blocking        sleep(poll_interval)
    gc_tick → spawn_blocking             if flush_due { do_flush() }
    sync_tick → spawn_blocking           if compact_due { compact() }
  }                                     ...
}                                     }
                                    }
```

### MaintenanceHandle：优雅的生命周期管理

tokio 的 `JoinHandle` 有 `abort()` 方法可以强制中止 task。但 `std::thread::JoinHandle` 没有等价方法——Rust 不支持强制杀死线程。

解决方案是 `MaintenanceHandle` + `Arc<AtomicBool>` stop flag：

```rust
pub struct MaintenanceHandle {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for MaintenanceHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);  // 通知线程退出
        if let Some(t) = self.thread.take() {
            let _ = t.join();  // 等待线程清理完毕
        }
    }
}
```

当 `Engine::shutdown()` 被调用时，`maintenance` 字段被 drop，触发 `MaintenanceHandle::drop()`，自动通知后台线程退出并 join。用户不需要显式管理线程生命周期。

### 后台线程的 poll loop

原来的 tokio 版本用了 4 个 `tokio::time::interval` + `tokio::select!`，行为是"哪个 timer 先到就执行哪个"。std::thread 版本用一个简单的 poll loop 替代：

```rust
std::thread::spawn(move || {
    loop {
        if stop.load(Relaxed) { break; }
        thread::sleep(poll_interval);  // 最短间隔（flush_interval 的 1/4）
        
        if now - last_flush >= flush_dur    { do_flush(); }
        if now - last_compact >= compact_dur { compact(); }
        if now - last_gc >= gc_dur          { gc(); }
        if now - last_sync >= sync_dur      { wal_sync(); }
    }
});
```

这个方案的优势：
- **无运行时依赖**：不需要 tokio runtime
- **更简单的调度**：一个线程，一个 loop，四个时间检查
- **更低的开销**：没有 async 状态机、没有 tokio scheduler
- **可控的退出**：stop flag 在每次循环开始检查，保证及时退出

### 最终结果

v0.3.0 实现了**零 tokio 依赖**：

```
[dependencies]          # v0.2.2                    # v0.3.0
tokio                   { version = "1", features = ["full"] }  ← 移除
serde                   ✓                           ✓
parking_lot             ✓                           ✓
zstd                    ✓                           ← 移除 (now only lz4)
...                     ✓                           ✓
```

| 指标 | v0.2.2 | v0.3.0 |
|------|--------|--------|
| async 方法数 | 12 | 0 |
| tokio 依赖 | required | 无 |
| 测试中的 `#[tokio::test]` | ~70 | 0 |
| 顺序写入 | 3.7M ops/s | **4.5M ops/s** (+22%) |
| 编译依赖数 | ~300 | ~250 |
| 用户集成成本 | 需要 tokio runtime | 零运行时要求 |

> **技术选型要看场景。** async/await 是网络服务的利器——高并发 I/O 场景下它能用极少的线程处理海量连接。但存储引擎的 API 调用是同步的——用户写入一批数据，等它完成，再写下一批。在同步场景上叠加 async，就像在跑步鞋上穿高跟鞋——不仅没有帮助，还会绊倒你。

## 五、写在最后

### 四个版本，一条主线

从 v0.1.5 到 v0.3.0，FlowDB 经历了四次大改，但有一条清晰的主线：**做减法**。

```
v0.1.5 ────→ v0.2.0 ────→ v0.2.1 ────→ v0.2.2 ────→ v0.3.0
大而全       修正确性     砍 server    优化写入     去掉 async
15 模块      15 模块      12 模块      12 模块      12 模块
HTTP/UDP    P0-P3 fix   纯引擎       Vec memtable  零 tokio
Admin/Auth  WAL fix     无 server    FxHash        纯同步
tokio full  背压         async API    sync write    std::thread
```

每一次做减法，我们都问自己同一个问题：**这行代码/这个依赖/这个抽象层是否在创造核心价值？**

如果答案是否定的，就砍掉它。

### 工程方法论

回顾这四个版本，有几条方法论值得总结：

1. **先正确后性能**：v0.2.0 修完所有正确性 bug，才开始 v0.2.2 的性能优化。快的数据库如果返回错误结果，毫无价值。

2. **Benchmark 驱动**：每一次性能改动都跑 benchmark 对比 RocksDB。没有测量就没有优化——"感觉变快了"不是工程方法。

3. **Profile 定位瓶颈**：60% 的时间花在 key clone 上，这个发现来自 profiler，而不是猜测。定位到瓶颈后，优化方案（Vec 替代 BTreeMap）就显而易见了。

4. **审计驱动重构**：v0.3.0 去掉 async 的决策来自一次完整的 async 审计——12 个方法中 8 个是假 async。数据驱动的决策比直觉更可靠。

5. **覆盖率守护质量**：整个项目保持 94.8% 的行覆盖率，每次改动后跑 `cargo llvm-cov --summary-only`。高覆盖率让我们有信心做大重构。

### FlowDB 的定位

今天的 FlowDB v0.3.0 是：

- **纯嵌入式存储引擎**——没有网络层，不需要独立进程
- **完全同步 API**——不绑定任何 async runtime
- **原生时序支持**——`(key, ts)` 双维索引，TTL，多版本
- **全面超越 RocksDB**——写入快 1.4x，点查快 11x，扫描快 6x

它的用户画像是：**需要在应用进程内嵌入一个高性能时序存储引擎，且不想引入额外数据库进程的开发者**。

```toml
[dependencies]
flowdb = "0.3"
```

```rust
let engine = Engine::open(Config::default())?;
engine.write_batch(&records)?;
let results = engine.query_by_prefix("sensor.")?;
engine.shutdown()?;
// 没有 .await，没有 tokio，没有额外进程
```

这就是 FlowDB 的故事：**通过不断做减法，找到产品的核心价值**。

---

*FlowDB 是开源项目，代码托管在 [GitHub](https://github.com/restsend/flowdb)，crate 发布在 [crates.io](https://crates.io/crates/flowdb)。*
