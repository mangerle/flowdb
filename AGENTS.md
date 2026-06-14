# AGENTS.md – FlowDB

## Build & Test Commands

- Build (debug): `cargo build`
- Build all targets: `cargo build --tests --all-features`
- Run all tests: `cargo test`
- Run only library tests: `cargo test --lib`
- Run a single integration suite: `cargo test --test http_integration`
- Run benches: `cargo bench`

## Coverage

This project enforces **≥ 90% line/region/function coverage** across the entire crate
(including the CLI binaries).

Run coverage locally:

```bash
cargo llvm-cov                       # text summary
cargo llvm-cov --html --open         # interactive HTML report
cargo llvm-cov --summary-only        # just the totals
```

How we hit 90%+ across the whole crate:

1. The `#[tokio::main] async fn main()` entry points in `src/bin/flowdb-server.rs`
   and `src/bin/flowdb-stress.rs` are tagged with
   `#[cfg_attr(coverage_nightly, coverage(off))]` because they are pure process
   bootstrap (parse args, bind sockets, spawn runtime) and are exercised via
   integration tests, not unit tests. The same files enable the unstable
   `coverage_attribute` feature only under `cfg(coverage_nightly)`, which
   `cargo-llvm-cov` sets automatically on nightly toolchains.

2. All logic in the binaries (CLI parsing, `resolve_server_config`, all
   `bench_*` / `format_*` / `print_*` helpers in the stress tool) is factored
   into free functions with unit tests in the same file's `mod tests`.

3. `Cargo.toml` declares the `lints.rust.unexpected_cfgs` table so the
   `cfg(coverage)` / `cfg(coverage_nightly)` cfgs don't trigger warnings.

4. HTTP and UDP server code is covered through `tests/http_integration.rs`
   (32 tests) and `tests/network_integration.rs` (15 tests, including a real
   socket round-trip through `start_udp_listener`).

5. Engine background maintenance (`spawn_background_maintenance`,
   `auto_background = true`) is covered by
   `engine::tests::test_engine_auto_background_starts_maintenance` and
   `engine::tests::test_spawn_background_maintenance_explicit`.

If you add new code, please add corresponding tests and run
`cargo llvm-cov --summary-only` before committing — the TOTAL line coverage
must stay at or above 90%.

## Project Layout

```
src/
  lib.rs              – public API surface (Config, Engine, Record, Query, ...)
  engine.rs           – Engine + ScanIterator (the core)
  memtable.rs         – in-memory write buffer (MemTables)
  wal.rs              – write-ahead log
  sstable.rs          – on-disk sorted-string table reader/writer
  block_meta_index.rs – fine-grained block-level index
  bloom.rs            – bloom filter for SST point queries
  cache.rs            – block cache (LRU)
  compaction.rs       – size-tiered compaction
  gc.rs               – expired-SST garbage collection
  manifest.rs         – append-only manifest log
  record.rs           – Record / InternalRecord / Query / Config types
  write_worker.rs     – single-writer worker driving WAL + memtable
  stats.rs            – engine stats + Prometheus exporter
  udp.rs              – binary UDP write protocol (V1/V2 with auth tag)
  http.rs             – axum HTTP server (write, query, admin endpoints)
  admin.rs            – embedded admin UI HTML
  auth.rs             – API-key auth state shared by HTTP and admin
  error.rs            – FlowError / Result
  bin/
    flowdb-server.rs  – `flowdb-server` binary
    flowdb-stress.rs  – `flowdb-stress` benchmarking binary
tests/
  http_integration.rs     – router-level HTTP tests (32)
  network_integration.rs  – real-socket HTTP + UDP tests (15)
  fuzz_tests.rs           – property/fuzz tests with arbitrary
benches/
  flowdb_bench.rs         – criterion micro-benchmarks
```

## Test Conventions

- Inline `#[cfg(test)] mod tests` per module for fine-grained unit tests.
- Integration tests live under `tests/` and use `tower::ServiceExt::oneshot`
  for HTTP and `tokio::net::UdpSocket` for UDP.
- Tests use `tempfile::TempDir` for filesystem isolation. The lone exception
  is `src/bin/flowdb-stress.rs`'s `make_temp_dir()` helper, which uses an
  atomic counter to give each parallel test its own directory.
- Tests must clean up after themselves (`engine.shutdown().await.unwrap()`
  for engines owned by value; `engine.flush().await` for engines behind an
  `Arc` that cannot be moved out).
