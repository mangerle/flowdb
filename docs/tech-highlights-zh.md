# FlowDB 技术亮点

## 为时序场景而生的 LSM 存储引擎

FlowDB 是一款基于 Rust 构建的高性能嵌入式时序存储引擎，采用定制化 LSM-Tree 架构，专为时序数据和日志类负载深度优化。

---

### 1. 极致写入性能

- **无锁序列号分配**：基于 `AtomicU64` + Relaxed Ordering 的批量序列号分配，零竞争写入
- **锁外 WAL 预编码**：WAL 缓冲区在获取写锁前完成全部编码，临界区仅负责追加和插入，极大缩短锁持有时间
- **双态 MemTable 设计**：活跃状态使用 `Vec + HashMap`（追加写入 O(1)，缓存友好），冻结时才转换为 `BTreeMap`，将排序开销延迟到刷盘阶段批量处理
- **零拷贝写入路径**：`write_batch_owned` 利用 Rust 所有权语义，直接 move 数据，无需 clone

> 对比 RocksDB 基准测试：**顺序写入 5.7M ops/s（1.92x）、并发写入 6.7M ops/s（1.63x）**

### 2. 级联式高效查询

独创五层过滤级联，逐层过滤减少磁盘访问和解压开销：

```
MemTable 命中 → 二维 Block 索引 → Bloom Filter → Block 内二分查找 → 按需解压
```

- **二维 Block 索引**：按 Key 和按时间桶双重索引，支持前缀查询、范围查询、时间范围查询的精准裁剪
- **Key 粒度 Bloom Filter**：针对时序场景优化——同一 Key 对应多个时间戳，Bloom Filter 按 Key 而非 `(Key, TS)` 构建，命中率更高
- **mmap 零拷贝读取**：SST 文件通过 `memmap2` 内存映射读取，避免 `read()` 系统调用

> 对比 RocksDB：**点查 6.6M ops/s（12.7x）、前缀扫描 71K ops/s（6.6x）**

### 3. 双压缩策略

| 阶段 | 算法 | 设计考量 |
|------|------|----------|
| Flush（写入路径） | **LZ4** | 极致速度，减少写入延迟 |
| Compaction（后台合并） | **Zstd**（多线程） | 极致压缩比，降低存储成本 |

### 4. 智能垃圾回收

- **整文件级过期淘汰**：检查 SST 的 `max_expire` 是否已过期，直接删除整个文件，无需逐条扫描
- 配合 TTL 机制（微秒精度），特别适合时序数据按时间窗口批量过期的场景

### 5. 流式 Compaction

- **Size-Tiered 策略**：按大小分组，将 4x 范围内的 SST 合并，避免大小悬殊导致的写放大
- **堆式 K路归并**：基于 `BinaryHeap` 的流式合并，内存占用与 `block_size × merge_fanin` 成正比，而非 SST 总大小
- 合并过程中自动去重、清除墓碑记录

### 6. 企业级工程实践

- **64 分片 LRU Block Cache**：基于 `parking_lot::RwLock`，降低并发读取的锁竞争
- **全扫描缓存旁路**：全表扫描主动绕过 Block Cache，避免冷数据驱逐热数据
- **RocksDB 风格惰性迭代器**：`ScanIterator` 不物化完整结果集，支持 `FusedIterator`，内存占用恒定
- **单源快速路径**：仅有一个数据源且无墓碑时，跳过堆归并直接 yield 记录

### 7. 灵活部署模式

| 模式 | 说明 |
|------|------|
| **嵌入式库** | 作为 Rust crate 直接集成，零运维开销 |
| **独立服务** | HTTP + UDP 双协议写入，内嵌 Web 管理面板 |

- HTTP API 支持 JSON 和 Binary 两种写入格式
- UDP 二进制协议适合高频小包写入场景（IoT、监控指标）
- 内置 Prometheus 格式指标输出（`/metrics`），p50/p90/p99 延迟直方图
- 所有服务端功能通过 Feature Gate 按需启用，嵌入式场景编译体积更小

### 8. 可靠性与可观测性

- **WAL 分段轮转**：默认 64MB 自动轮转，崩溃恢复通过 WAL 回放保证数据完整
- **JSON-Lines Manifest**：持久化记录所有 SST、Flush、Compaction、GC 操作，支持检查点恢复
- **6 大模糊测试套件**：覆盖 WAL 编解码、SST 读写、MemTable 查询、Manifest 恢复、Block 索引、UDP 帧协议
- **HdrHistogram 延迟追踪**：精准统计 p50/p90/p99 尾部延迟

---

### 性能概览（vs RocksDB，100K 记录，128B Value）

| 指标 | FlowDB | RocksDB | 倍数 |
|------|--------|---------|------|
| 顺序写入 | 5.7M ops/s | 3.0M ops/s | **1.9x** |
| 并发写入 (8线程) | 6.7M ops/s | 4.1M ops/s | **1.6x** |
| 点查询 | 6.6M ops/s | 524K ops/s | **12.7x** |
| 前缀扫描 | 71K ops/s | 10.7K ops/s | **6.6x** |
| 全表扫描 | 65 ops/s | 39 ops/s | **1.7x** |
| 存储空间 | 1.9MB | 1.8MB | ≈ 相当 |

---

**FlowDB** — 以 Rust 的零成本抽象，重新定义时序存储的性能边界。
