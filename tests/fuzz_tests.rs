use arbitrary::{Arbitrary, Unstructured};
use flowdb::{Config, Engine, Query, Record};
use tempfile::TempDir;

const FUZZ_ITERS: u64 = 200;

fn make_config(dir: &std::path::Path) -> Config {
    Config {
        data_dir: dir.to_path_buf(),
        memtable_size_mb: 1,
        block_size: 100,
        gc_interval_secs: 3600,
        max_frozen_memtables: 2,
        zstd_level: 1,
        flush_interval_ms: 60000,
        time_bucket_secs: 3600,
        block_cache_capacity_mb: 16,
        index_memory_budget_mb: 64,
        default_ttl_secs: None,
        bloom_bits_per_key: 10,
        wal_segment_size_mb: 64,
        compaction_threshold: 2,
        create_if_missing: true,
        wal_sync_mode: flowdb::SyncMode::Always,
    }
}

#[derive(Debug, Clone)]
struct FuzzRecord {
    key: String,
    ts: i64,
    value: Vec<u8>,
}

impl<'a> Arbitrary<'a> for FuzzRecord {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let key_len = u.int_in_range(1usize..=16)?;
        let mut key_bytes = vec![0u8; key_len];
        u.fill_buffer(&mut key_bytes)?;
        for b in key_bytes.iter_mut() {
            *b = (*b % 26) + b'a';
        }
        let key = String::from_utf8(key_bytes).unwrap();
        let ts = u.int_in_range(0i64..=1_000_000)?;
        let val_len = u.int_in_range(0usize..=32)?;
        let mut value = vec![0u8; val_len];
        u.fill_buffer(&mut value)?;
        Ok(FuzzRecord { key, ts, value })
    }
}

fn to_record(fr: &FuzzRecord) -> Record {
    Record {
        key: fr.key.clone().into_bytes(),
        ts: fr.ts,
        expire_at: i64::MAX,
        value: fr.value.clone(),
    }
}

fn generate_seed_data(seed: u64) -> Vec<u8> {
    let size = 32768;
    let mut data = Vec::with_capacity(size);
    let mut state = seed;
    for _ in 0..size {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        data.push((state >> 33) as u8);
    }
    data
}

fn gen_records(u: &mut Unstructured, max: usize) -> Vec<FuzzRecord> {
    let count = u.int_in_range(1usize..=max).unwrap_or(1);
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        match FuzzRecord::arbitrary(u) {
            Ok(r) => records.push(r),
            Err(_) => break,
        }
    }
    records
}

#[tokio::test]
async fn fuzz_wal_encode_decode() {
    for seed in 0..FUZZ_ITERS {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());

        let data = generate_seed_data(seed);
        let mut u = Unstructured::new(&data);
        let fuzz_records = gen_records(&mut u, 10);
        if fuzz_records.is_empty() {
            continue;
        }

        let records: Vec<Record> = fuzz_records.iter().map(to_record).collect();

        {
            let engine = Engine::open(config.clone()).await.unwrap();
            engine.write_batch(&records).await.unwrap();
            engine.shutdown().await.unwrap();
        }

        let engine2 = Engine::open(config).await.unwrap();
        for fr in &fuzz_records {
            let results = engine2.query_by_prefix(&fr.key).await.unwrap();
            let found = results.iter().any(|r| r.ts == fr.ts && r.value == fr.value);
            assert!(
                found,
                "seed={}: record key={} ts={} not recovered",
                seed, fr.key, fr.ts
            );
        }
        engine2.shutdown().await.unwrap();

        drop(dir);
    }
}

#[tokio::test]
async fn fuzz_sstable_write_read() {
    for seed in 0..FUZZ_ITERS {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());

        let data = generate_seed_data(seed);
        let mut u = Unstructured::new(&data);
        let fuzz_records = gen_records(&mut u, 20);
        if fuzz_records.is_empty() {
            continue;
        }

        let records: Vec<Record> = fuzz_records.iter().map(to_record).collect();

        let engine = Engine::open(config).await.unwrap();
        engine.write_batch(&records).await.unwrap();
        engine.flush().await.unwrap();

        for fr in &fuzz_records {
            let results = engine.query_by_prefix(&fr.key).await.unwrap();
            let found = results.iter().any(|r| r.ts == fr.ts && r.value == fr.value);
            assert!(
                found,
                "seed={}: record key={} ts={} not found after flush",
                seed, fr.key, fr.ts
            );
        }
        engine.shutdown().await.unwrap();
        drop(dir);
    }
}

#[tokio::test]
async fn fuzz_memtable_query() {
    for seed in 0..FUZZ_ITERS {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(dir.path());
        config.memtable_size_mb = 64;

        let data = generate_seed_data(seed);
        let mut u = Unstructured::new(&data);
        let fuzz_records = gen_records(&mut u, 10);
        if fuzz_records.is_empty() {
            continue;
        }

        let records: Vec<Record> = fuzz_records.iter().map(to_record).collect();

        let engine = Engine::open(config).await.unwrap();
        engine.write_batch(&records).await.unwrap();

        for fr in &fuzz_records {
            let results = engine.query_by_prefix(&fr.key).await.unwrap();
            assert!(
                !results.is_empty(),
                "seed={}: memtable query returned empty for key={}",
                seed,
                fr.key
            );
        }

        let all_keys: Vec<String> = fuzz_records.iter().map(|r| r.key.clone()).collect();
        let all_results = engine
            .query(Query::key_range(
                all_keys.iter().min().unwrap().clone(),
                all_keys.iter().max().unwrap().clone(),
            ))
            .await
            .unwrap();
        assert!(
            !all_results.is_empty(),
            "seed={}: key_range query returned empty",
            seed
        );

        engine.shutdown().await.unwrap();
        drop(dir);
    }
}

#[cfg(feature = "server")]
#[test]
fn fuzz_udp_frame_encode_decode() {
    for seed in 0..1000 {
        let data = generate_seed_data(seed);
        let mut u = Unstructured::new(&data);

        let key_len = u.int_in_range(1usize..=64).unwrap_or(1);
        let mut key_bytes = vec![0u8; key_len];
        if u.fill_buffer(&mut key_bytes).is_err() {
            continue;
        }
        let key = String::from_utf8_lossy(&key_bytes).into_owned();

        let ts: i64 = match u.arbitrary() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let has_ttl: bool = match u.arbitrary() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let ttl = if has_ttl {
            Some(u.int_in_range(0u32..=86400).unwrap_or(3600))
        } else {
            None
        };

        let val_len = u.int_in_range(0usize..=200).unwrap_or(0);
        let mut value = vec![0u8; val_len];
        if u.fill_buffer(&mut value).is_err() {
            continue;
        }

        let rec = flowdb::Record {
            key: key.clone().into_bytes(),
            ts,
            expire_at: if has_ttl {
                ts + (ttl.unwrap_or(3600) as i64 * 1_000_000)
            } else {
                i64::MAX
            },
            value: value.clone(),
        };
        let encoded = flowdb::udp::encode_frame(&[rec], None);

        match flowdb::udp::decode_frame(&encoded, None) {
            Ok(decoded) => {
                assert_eq!(decoded.len(), 1);
                assert_eq!(decoded[0].key, key.into_bytes());
                assert_eq!(decoded[0].ts, ts);
                assert_eq!(decoded[0].value, value);
            }
            Err(_) => {
                panic!("seed={}: valid frame failed to decode", seed);
            }
        }
    }
}

#[tokio::test]
async fn fuzz_engine_write_query() {
    for seed in 0..FUZZ_ITERS {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());

        let data = generate_seed_data(seed);
        let mut u = Unstructured::new(&data);

        let engine = Engine::open(config).await.unwrap();

        let batch_count = u.int_in_range(1usize..=3).unwrap_or(1);
        let mut all_written: Vec<Record> = Vec::new();

        for _ in 0..batch_count {
            let fuzz_recs = gen_records(&mut u, 5);
            let records: Vec<Record> = fuzz_recs.iter().map(to_record).collect();
            if records.is_empty() {
                continue;
            }
            engine.write_batch(&records).await.unwrap();
            all_written.extend(records);
        }

        if all_written.is_empty() {
            engine.shutdown().await.unwrap();
            continue;
        }

        if let Some(key) = all_written.first().map(|r| r.key.clone()) {
            let key_str = String::from_utf8_lossy(&key);
            let results = engine.query_by_prefix(&key_str).await.unwrap();
            assert!(
                !results.is_empty(),
                "seed={}: prefix query empty for key={:?}",
                seed,
                key
            );
        }

        let min_ts = all_written.iter().map(|r| r.ts).min().unwrap();
        let max_ts = all_written.iter().map(|r| r.ts).max().unwrap();
        let results = engine.query_time_range(min_ts, max_ts).await.unwrap();
        assert!(!results.is_empty(), "seed={}: time_range query empty", seed);

        engine.shutdown().await.unwrap();
        drop(dir);
    }
}

#[tokio::test]
async fn fuzz_manifest_recovery() {
    for seed in 0..FUZZ_ITERS {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());

        let data = generate_seed_data(seed);
        let mut u = Unstructured::new(&data);

        let fuzz_records = gen_records(&mut u, 20);
        if fuzz_records.is_empty() {
            continue;
        }
        let records: Vec<Record> = fuzz_records.iter().map(to_record).collect();

        {
            let engine = Engine::open(config.clone()).await.unwrap();
            engine.write_batch(&records).await.unwrap();
            engine.flush().await.unwrap();

            let fuzz_more = gen_records(&mut u, 10);
            let more_records: Vec<Record> = fuzz_more.iter().map(to_record).collect();
            if !more_records.is_empty() {
                engine.write_batch(&more_records).await.unwrap();
            }

            engine.shutdown().await.unwrap();
        }

        let engine2 = Engine::open(config).await.unwrap();
        for fr in &fuzz_records {
            let results = engine2.query_by_prefix(&fr.key).await.unwrap();
            let found = results.iter().any(|r| r.ts == fr.ts && r.value == fr.value);
            assert!(
                found,
                "seed={}: record key={} ts={} lost after recovery",
                seed, fr.key, fr.ts
            );
        }
        engine2.shutdown().await.unwrap();
        drop(dir);
    }
}

#[tokio::test]
async fn fuzz_block_meta_index_queries() {
    for seed in 0..FUZZ_ITERS {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());

        let data = generate_seed_data(seed);
        let mut u = Unstructured::new(&data);

        let engine = Engine::open(config).await.unwrap();

        let mut all_keys = Vec::new();
        for _ in 0..3 {
            let fuzz_recs = gen_records(&mut u, 10);
            let records: Vec<Record> = fuzz_recs.iter().map(to_record).collect();
            if records.is_empty() {
                continue;
            }
            all_keys.extend(fuzz_recs.iter().map(|r| r.key.clone()));
            engine.write_batch(&records).await.unwrap();
            engine.flush().await.unwrap();
        }

        if all_keys.is_empty() {
            engine.shutdown().await.unwrap();
            continue;
        }

        for key in &all_keys {
            let _ = engine.query_by_prefix(key).await.unwrap();
        }

        if all_keys.len() >= 2 {
            let mut sorted = all_keys.clone();
            sorted.sort();
            let _ = engine
                .query_by_key_range(&sorted[0], sorted.last().unwrap())
                .await
                .unwrap();
        }

        let _ = engine.query_time_range(0, 1_000_000).await.unwrap();

        if let Some(key) = all_keys.first() {
            let _ = engine
                .query_prefix_time_range(key, 0, 1_000_000)
                .await
                .unwrap();
        }

        engine.shutdown().await.unwrap();
        drop(dir);
    }
}

#[cfg(feature = "server")]
#[test]
fn fuzz_udp_batch_frame() {
    for seed in 0..500 {
        let data = generate_seed_data(seed);
        let mut u = Unstructured::new(&data);

        let record_count = u.int_in_range(1usize..=5).unwrap_or(1);
        let mut records = Vec::new();

        let mut ok = true;
        for _ in 0..record_count {
            let key_len = u.int_in_range(1usize..=16).unwrap_or(1);
            let mut key_bytes = vec![0u8; key_len];
            if u.fill_buffer(&mut key_bytes).is_err() {
                ok = false;
                break;
            }
            for b in key_bytes.iter_mut() {
                *b = (*b % 26) + b'a';
            }
            let key = String::from_utf8(key_bytes).unwrap();

            let ts: i64 = match u.arbitrary() {
                Ok(v) => v,
                Err(_) => {
                    ok = false;
                    break;
                }
            };
            let val_len = u.int_in_range(0usize..=32).unwrap_or(0);
            let mut value = vec![0u8; val_len];
            if u.fill_buffer(&mut value).is_err() {
                ok = false;
                break;
            };

            records.push(flowdb::Record {
                key: key.into_bytes(),
                ts,
                expire_at: i64::MAX,
                value,
            });
        }

        if !ok || records.is_empty() {
            continue;
        }

        let encoded = flowdb::udp::encode_frame(&records, None);
        match flowdb::udp::decode_frame(&encoded, None) {
            Ok(decoded) => {
                assert_eq!(
                    decoded.len(),
                    records.len(),
                    "seed={}: record count mismatch",
                    seed
                );
                for (i, exp) in records.iter().enumerate() {
                    assert_eq!(
                        decoded[i].key, exp.key,
                        "seed={}: key mismatch at {}",
                        seed, i
                    );
                    assert_eq!(decoded[i].ts, exp.ts, "seed={}: ts mismatch at {}", seed, i);
                    assert_eq!(
                        decoded[i].value, exp.value,
                        "seed={}: value mismatch at {}",
                        seed, i
                    );
                }
            }
            Err(e) => {
                panic!("seed={}: batch decode failed: {}", seed, e);
            }
        }
    }
}

#[cfg(feature = "server")]
#[test]
fn fuzz_udp_corrupt_frame() {
    let rec = flowdb::Record {
        key: "test".into(),
        ts: 100,
        expire_at: i64::MAX,
        value: b"val".to_vec(),
    };
    let mut frame = flowdb::udp::encode_frame(&[rec], None);

    for seed in 0..1000 {
        let data = generate_seed_data(seed);
        let mut u = Unstructured::new(&data);

        let corrupt_pos = u
            .int_in_range(0usize..=frame.len().saturating_sub(1))
            .unwrap_or(0);
        let flip = u.int_in_range(1u8..=255).unwrap_or(0xFF);
        frame[corrupt_pos] ^= flip;

        if frame.len() < 4 {
            frame[corrupt_pos] ^= flip;
            continue;
        }

        let _ = flowdb::udp::decode_frame(&frame, None);
        frame[corrupt_pos] ^= flip;
    }
}
