#![cfg(feature = "server")]

use flowdb::auth::AuthState;
use flowdb::http::{AppState, start_http_server};
use flowdb::stats::StatsCounters;
use flowdb::udp::{decode_frame, encode_frame, start_udp_listener};
use flowdb::{Config, Engine, Record};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::UdpSocket;
use tokio::time::timeout;

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
        wal_sync_mode: flowdb::SyncMode::IntervalMs(u64::MAX),
        auto_background: false,
    }
}

/// `start_http_server` binds to an ephemeral port and serves a health-check.
#[tokio::test]
async fn test_start_http_server_real_socket() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(make_config(dir.path())).await.unwrap());
    let state = AppState {
        engine,
        auth: AuthState::new(vec![]),
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    // Bind our own listener to discover the free port, then drop it so
    // start_http_server can rebind. This avoids needing to parse the bound
    // address out of the server task.
    let probe = tokio::net::TcpListener::bind(addr).await.unwrap();
    let bound = probe.local_addr().unwrap();
    drop(probe);

    let server_handle = tokio::spawn(async move {
        start_http_server(state, bound).await
    });

    // Give the server a moment to start, then send a /health request
    // over a plain TCP socket.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = tokio::net::TcpStream::connect(bound).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(b"GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 256];
    let n = stream.read(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf[..n]);
    assert!(
        text.starts_with("HTTP/1.0 200") || text.starts_with("HTTP/1.1 200"),
        "server should respond with 200, got: {}",
        text
    );

    server_handle.abort();
}

/// UDP listener writes received records into the engine.
#[tokio::test]
async fn test_udp_listener_round_trip() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(make_config(dir.path())).await.unwrap());
    let stats = Arc::new(StatsCounters::new());

    // Bind a probe to discover a free port, then drop and rebind.
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let bound = probe.local_addr().unwrap();
    drop(probe);

    let server_engine = engine.clone();
    let server_stats = stats.clone();
    let listener_handle = tokio::spawn(async move {
        let _ = start_udp_listener(
            server_engine,
            server_stats,
            bound,
            64 * 1024,
            None,
            0,
        )
        .await;
    });

    // Give the listener a moment to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a single-record V1 frame.
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let rec = Record {
        key: b"udp_key".to_vec(),
        ts: 12345,
        expire_at: i64::MAX,
        value: b"hello".to_vec(),
    };
    let frame = encode_frame(&[rec], None);
    sock.send_to(&frame, bound).await.unwrap();

    // Wait for the write to land.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if engine
            .query(flowdb::Query::prefix("udp_key"))
            .await
            .map(|v| !v.is_empty())
            .unwrap_or(false)
        {
            break;
        }
    }
    let records = engine
        .query(flowdb::Query::prefix("udp_key"))
        .await
        .unwrap();
    assert_eq!(records.len(), 1, "UDP write should land");
    assert_eq!(records[0].key, b"udp_key");

    // Stats should reflect the received packet.
    assert!(stats
        .udp_packets_received
        .load(std::sync::atomic::Ordering::Relaxed)
        >= 1);

    listener_handle.abort();
}

/// UDP listener with rate limiting drops packets over the budget.
#[tokio::test]
async fn test_udp_listener_rate_limit() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(make_config(dir.path())).await.unwrap());
    let stats = Arc::new(StatsCounters::new());

    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let bound = probe.local_addr().unwrap();
    drop(probe);

    let server_engine = engine.clone();
    let server_stats = stats.clone();
    let listener_handle = tokio::spawn(async move {
        let _ = start_udp_listener(
            server_engine,
            server_stats,
            bound,
            64 * 1024,
            None,
            // very low rate — 1 token/sec, so the second packet must drop.
            1,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let rec = Record {
        key: b"rl".to_vec(),
        ts: 1,
        expire_at: i64::MAX,
        value: vec![],
    };
    let frame = encode_frame(&[rec.clone()], None);

    // Send several packets back-to-back.
    for _ in 0..5 {
        let _ = sock.send_to(&frame, bound).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    let dropped = stats
        .udp_packets_dropped
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(dropped >= 1, "expected at least one rate-limited drop");

    listener_handle.abort();
}

/// UDP listener with auth configured accepts a valid V2 frame.
#[tokio::test]
async fn test_udp_listener_v2_auth() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(make_config(dir.path())).await.unwrap());
    let stats = Arc::new(StatsCounters::new());
    let api_key = "s3cret-udp-key".to_string();

    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let bound = probe.local_addr().unwrap();
    drop(probe);

    let server_engine = engine.clone();
    let server_stats = stats.clone();
    let key_clone = api_key.clone();
    let listener_handle = tokio::spawn(async move {
        let _ = start_udp_listener(
            server_engine,
            server_stats,
            bound,
            64 * 1024,
            Some(key_clone),
            0,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let rec = Record {
        key: b"v2_key".to_vec(),
        ts: 1,
        expire_at: i64::MAX,
        value: b"v".to_vec(),
    };
    let frame = encode_frame(&[rec], Some(&api_key));
    sock.send_to(&frame, bound).await.unwrap();

    // An invalid (V1) frame should be rejected.
    let v1_frame = encode_frame(
        &[Record {
            key: b"v1_rejected".to_vec(),
            ts: 1,
            expire_at: i64::MAX,
            value: vec![],
        }],
        None,
    );
    sock.send_to(&v1_frame, bound).await.unwrap();

    // Wait for write to land.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if engine
            .query(flowdb::Query::prefix("v2_key"))
            .await
            .map(|v| !v.is_empty())
            .unwrap_or(false)
        {
            break;
        }
    }
    let records = engine.query(flowdb::Query::prefix("v2_key")).await.unwrap();
    assert_eq!(records.len(), 1, "V2 frame should be accepted");

    // V1 should never have been written.
    let v1_records = engine
        .query(flowdb::Query::prefix("v1_rejected"))
        .await
        .unwrap();
    assert!(v1_records.is_empty(), "V1 frame should be rejected");

    listener_handle.abort();
}

/// UDP listener rejects malformed frames (triggers the decode-error branch).
#[tokio::test]
async fn test_udp_listener_decode_error() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(make_config(dir.path())).await.unwrap());
    let stats = Arc::new(StatsCounters::new());

    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let bound = probe.local_addr().unwrap();
    drop(probe);

    let server_engine = engine.clone();
    let server_stats = stats.clone();
    let listener_handle = tokio::spawn(async move {
        let _ = start_udp_listener(
            server_engine,
            server_stats,
            bound,
            64 * 1024,
            None,
            0,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // Send garbage — bad magic.
    sock.send_to(&[0x00, 0x00, 0x00, 0x00], bound).await.unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    let dropped = stats
        .udp_packets_dropped
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(dropped >= 1, "decode error should bump dropped counter");

    listener_handle.abort();
}

/// Encode/decode with V2 (auth) frame round-trips correctly.
#[test]
fn test_udp_v2_frame_round_trip() {
    let key = "the-key";
    let rec = Record {
        key: b"k".to_vec(),
        ts: 42,
        expire_at: i64::MAX,
        value: b"value".to_vec(),
    };
    let frame = encode_frame(&[rec.clone()], Some(key));
    // The frame must be V2 (second byte == 0x02).
    assert_eq!(frame[1], 0x02);
    let decoded = decode_frame(&frame, Some(key)).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].key, b"k");
    assert_eq!(decoded[0].value, b"value");
}

/// V2 frame with wrong key must be rejected.
#[test]
fn test_udp_v2_wrong_key_rejected() {
    let rec = Record {
        key: b"k".to_vec(),
        ts: 1,
        expire_at: i64::MAX,
        value: vec![],
    };
    let frame = encode_frame(&[rec], Some("right"));
    assert!(decode_frame(&frame, Some("wrong")).is_err());
}

/// V2 frame on server with no api_key must be rejected.
#[test]
fn test_udp_v2_frame_no_server_key() {
    let rec = Record {
        key: b"k".to_vec(),
        ts: 1,
        expire_at: i64::MAX,
        value: vec![],
    };
    let frame = encode_frame(&[rec], Some("any"));
    assert!(decode_frame(&frame, None).is_err());
}

/// V1 frame on server with api_key must be rejected.
#[test]
fn test_udp_v1_frame_with_server_key() {
    let rec = Record {
        key: b"k".to_vec(),
        ts: 1,
        expire_at: i64::MAX,
        value: vec![],
    };
    let frame = encode_frame(&[rec], None);
    assert!(decode_frame(&frame, Some("any")).is_err());
}

/// Unknown frame version is rejected.
#[test]
fn test_udp_unknown_version() {
    let frame = [0x54, 0xFF, 0x00, 0x00];
    assert!(decode_frame(&frame, None).is_err());
    assert!(decode_frame(&frame, Some("k")).is_err());
}

/// Frame with bad magic is rejected.
#[test]
fn test_udp_bad_magic() {
    let frame = [0xFF, 0x01, 0x00, 0x00];
    assert!(decode_frame(&frame, None).is_err());
}

/// Frame shorter than 4 bytes is rejected.
#[test]
fn test_udp_frame_too_short() {
    assert!(decode_frame(&[0x54, 0x01], None).is_err());
    assert!(decode_frame(&[], None).is_err());
}

/// Round-trip a record with TTL set.
#[test]
fn test_udp_record_with_ttl() {
    let rec = Record {
        key: b"ttl".to_vec(),
        ts: 1_000,
        expire_at: 1_000 + 60 * 1_000_000,
        value: b"v".to_vec(),
    };
    let frame = encode_frame(&[rec], None);
    let decoded = decode_frame(&frame, None).unwrap();
    assert_eq!(decoded[0].expire_at, 1_000 + 60 * 1_000_000);
}

/// `read_record` rejects oversize keys / values via the const caps.
#[test]
fn test_udp_oversize_key_value() {
    // Build a frame manually with key_len > MAX_KEY_BYTES.
    let mut bad = vec![0x54, 0x01];
    bad.extend_from_slice(&1u16.to_be_bytes()); // count = 1
    // key_len = 4097 (MAX_KEY_BYTES+1) — exceeds cap.
    bad.extend_from_slice(&4097u16.to_be_bytes());
    // Pad with zeros (won't be reached).
    bad.extend_from_slice(&[0u8; 8]);
    assert!(decode_frame(&bad, None).is_err());

    // Oversize value: build a V1 frame whose value length > MAX_VAL_BYTES.
    let mut bad2 = vec![0x54, 0x01];
    bad2.extend_from_slice(&1u16.to_be_bytes()); // count = 1
    bad2.extend_from_slice(&1u16.to_be_bytes()); // key_len = 1
    bad2.push(b'k');
    bad2.extend_from_slice(&0i64.to_be_bytes()); // ts
    bad2.extend_from_slice(&0u32.to_be_bytes()); // ttl
    bad2.extend_from_slice(&65535u16.to_be_bytes()); // val_len = 65535 > MAX_VAL_BYTES
    bad2.extend_from_slice(&[0u8; 8]);
    assert!(decode_frame(&bad2, None).is_err());
}

/// `timeout` helper usage guard — make sure the symbol is referenced so the
/// unused-import lint stays quiet.
#[tokio::test]
async fn test_timeout_import_is_used() {
    let _: Result<(), tokio::time::error::Elapsed> =
        timeout(Duration::from_millis(1), std::future::pending::<()>()).await;
}

/// UDP frames that declare more than MAX_FRAME_RECORDS (1024) must be
/// rejected with an error, not silently truncated.
#[tokio::test]
async fn test_udp_frame_count_exceeds_max() {
    let mut bad = vec![0x54, 0x01]; // magic + V1
    bad.extend_from_slice(&1025u16.to_be_bytes()); // count = 1025 > 1024
    bad.extend_from_slice(&[0u8; 8]); // padding
    let result = decode_frame(&bad, None);
    assert!(result.is_err(), "should reject count > 1024");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("too large"),
        "error should mention count too large, got: {}",
        msg
    );
}
