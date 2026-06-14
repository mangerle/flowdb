#![cfg(feature = "server")]

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use base64::{Engine as _, engine::general_purpose};
use flowdb::auth::AuthState;
use flowdb::http::{AppState, build_router};
use flowdb::{Config, Engine, Record};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

fn make_config(dir: &std::path::Path) -> Config {
    Config {
        data_dir: dir.to_path_buf(),
        memtable_size_mb: 64,
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

async fn setup() -> (axum::Router, Arc<Engine>) {
    let dir = TempDir::new().unwrap();
    let config = make_config(dir.path());
    let engine = Arc::new(Engine::open(config).await.unwrap());
    let state = AppState {
        engine: engine.clone(),
        auth: AuthState::new(vec![]),
    };
    let app = build_router(state);
    (app, engine)
}

async fn setup_with_auth(keys: Vec<String>) -> (axum::Router, Arc<Engine>) {
    let dir = TempDir::new().unwrap();
    let config = make_config(dir.path());
    let engine = Arc::new(Engine::open(config).await.unwrap());
    let state = AppState {
        engine: engine.clone(),
        auth: AuthState::new(keys),
    };
    let app = build_router(state);
    (app, engine)
}

#[tokio::test]
async fn test_http_health() {
    let (app, _engine) = setup().await;
    let req = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_http_stats() {
    let (app, engine) = setup().await;
    let req = Request::builder()
        .uri("/stats")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("uptime_secs").is_some());
    drop(engine);
}

#[tokio::test]
async fn test_http_write_json() {
    let (app, engine) = setup().await;
    let body = serde_json::json!({
        "records": [{"key": "test", "ts": 100, "value": "aGVsbG8="}]
    });
    let req = Request::builder()
        .method(Method::POST)
        .uri("/write")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(engine);
}

#[tokio::test]
async fn test_http_query() {
    let (app, engine) = setup().await;
    engine
        .write_batch(&[Record {
            key: "query_test".into(),
            ts: 100,
            expire_at: i64::MAX,
            value: vec![1, 2, 3],
        }])
        .await
        .unwrap();

    let req = Request::builder()
        .uri("/query?prefix=query_test")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 1);
    drop(engine);
}

#[tokio::test]
async fn test_http_metrics() {
    let (app, engine) = setup().await;
    let req = Request::builder()
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("flowdb_uptime_seconds"));
    drop(engine);
}

#[tokio::test]
async fn test_http_admin_page() {
    let (app, engine) = setup().await;
    let req = Request::builder()
        .uri("/admin")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(engine);
}

#[tokio::test]
async fn test_http_auth_required() {
    let (app, engine) = setup_with_auth(vec!["secret".into()]).await;
    let body = serde_json::json!({
        "records": [{"key": "test", "ts": 100, "value": "aGVsbG8="}]
    });
    let req = Request::builder()
        .method(Method::POST)
        .uri("/write")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    drop(engine);
}

#[tokio::test]
async fn test_http_admin_flush() {
    let (app, engine) = setup().await;
    engine
        .write_batch(&[Record {
            key: "flush_test".into(),
            ts: 100,
            expire_at: i64::MAX,
            value: vec![1],
        }])
        .await
        .unwrap();

    let req = Request::builder()
        .method(Method::POST)
        .uri("/admin/flush")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(engine);
}

#[tokio::test]
async fn test_http_query_key_range() {
    let (app, engine) = setup().await;
    engine
        .write_batch(&[
            Record {
                key: "a".into(),
                ts: 100,
                expire_at: i64::MAX,
                value: vec![1],
            },
            Record {
                key: "b".into(),
                ts: 200,
                expire_at: i64::MAX,
                value: vec![2],
            },
        ])
        .await
        .unwrap();

    let req = Request::builder()
        .uri("/query?key_start=a&key_end=b")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 2);
    drop(engine);
}

#[tokio::test]
async fn test_http_query_time_range() {
    let (app, engine) = setup().await;
    engine
        .write_batch(&[
            Record {
                key: "a".into(),
                ts: 100,
                expire_at: i64::MAX,
                value: vec![1],
            },
            Record {
                key: "b".into(),
                ts: 200,
                expire_at: i64::MAX,
                value: vec![2],
            },
        ])
        .await
        .unwrap();

    let req = Request::builder()
        .uri("/query?ts_start=50&ts_end=150")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 1);
    drop(engine);
}

/// Helper: build a POST request to the given URI with optional JSON body
/// and optional `X-API-Key` header.
fn admin_request(
    uri: &str,
    body: Option<serde_json::Value>,
    api_key: Option<&str>,
) -> Request<Body> {
    // admin_query is GET, all others are POST.
    let is_get = uri.starts_with("/admin/query");
    let mut builder = if is_get {
        Request::builder().method(Method::GET).uri(uri)
    } else {
        Request::builder().method(Method::POST).uri(uri)
    };
    if let Some(key) = api_key {
        builder = builder.header("X-API-Key", key);
    }
    if let Some(b) = body {
        builder
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&b).unwrap()))
            .unwrap()
    } else {
        builder.body(Body::empty()).unwrap()
    }
}

/// Every admin mutation endpoint must reject the request with 401 WHEN
/// API keys are configured and none is provided.
#[tokio::test]
async fn test_admin_endpoints_require_auth_when_keys_set() {
    let (app, engine) = setup_with_auth(vec!["secret".into()]).await;

    // flush
    let req = admin_request("/admin/flush", None, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/admin/flush without key"
    );

    // gc
    let req = admin_request("/admin/gc", None, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/admin/gc without key"
    );

    // compact
    let req = admin_request("/admin/compact", None, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/admin/compact without key"
    );

    // query
    let req = admin_request("/admin/query?prefix=test", None, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/admin/query without key"
    );

    // delete
    let body = serde_json::json!({"key": "x", "ts": 1});
    let req = admin_request("/admin/delete", Some(body), None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/admin/delete without key"
    );

    // patch
    let body = serde_json::json!({"key": "x", "ts": 1, "value": "aGVsbG8="});
    let req = admin_request("/admin/patch", Some(body), None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/admin/patch without key"
    );

    drop(engine);
}

/// Every admin endpoint must ACCEPT the request with a valid API key.
#[tokio::test]
async fn test_admin_endpoints_accept_valid_key() {
    let key = "s3cret-key";
    let (app, engine) = setup_with_auth(vec![key.into()]).await;

    // Write some data so flush / query / delete / patch have something to
    // work with.
    engine
        .write_batch(&[Record {
            key: b"admin_test".to_vec(),
            ts: 100,
            expire_at: i64::MAX,
            value: vec![1, 2, 3],
        }])
        .await
        .unwrap();

    // flush
    let req = admin_request("/admin/flush", None, Some(key));
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "/admin/flush with valid key"
    );

    // gc
    let req = admin_request("/admin/gc", None, Some(key));
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "/admin/gc with valid key"
    );

    // compact
    let req = admin_request("/admin/compact", None, Some(key));
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "/admin/compact with valid key"
    );

    // query (GET)
    let req = Request::builder()
        .method(Method::GET)
        .uri("/admin/query?prefix=admin_test")
        .header("X-API-Key", key)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "/admin/query with valid key"
    );
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 1, "query should find the test record");

    // patch (before delete — needs record to exist)
    let body = serde_json::json!({"key": "admin_test", "ts": 100, "value": "d29ybGQ="});
    let req = admin_request("/admin/patch", Some(body), Some(key));
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "/admin/patch with valid key"
    );

    // delete
    let body = serde_json::json!({"key": "admin_test", "ts": 100});
    let req = admin_request("/admin/delete", Some(body), Some(key));
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "/admin/delete with valid key"
    );

    drop(engine);
}

/// An incorrect API key must be rejected (NOT the same as no key).
#[tokio::test]
async fn test_admin_endpoints_reject_wrong_key() {
    let (app, engine) = setup_with_auth(vec!["correct".into()]).await;

    let req = admin_request("/admin/flush", None, Some("wrong"));
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/admin/flush with wrong key"
    );

    let req = admin_request("/admin/gc", None, Some("wrong"));
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/admin/gc with wrong key"
    );

    drop(engine);
}

/// When no API keys are configured (auth disabled), all admin endpoints
/// must work WITHOUT any key header.
#[tokio::test]
async fn test_admin_endpoints_work_without_auth_when_disabled() {
    let (app, engine) = setup().await;

    engine
        .write_batch(&[Record {
            key: b"noauth".to_vec(),
            ts: 1,
            expire_at: i64::MAX,
            value: vec![1],
        }])
        .await
        .unwrap();

    let req = admin_request("/admin/flush", None, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = admin_request("/admin/gc", None, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = admin_request("/admin/compact", None, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/admin/query?prefix=noauth")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    drop(engine);
}

/// The admin HTML page must never require auth — it is just a static
/// frontend that cannot do anything without the data endpoints.
#[tokio::test]
async fn test_admin_page_always_public() {
    // With auth enabled
    let (app, _engine) = setup_with_auth(vec!["key".into()]).await;
    let req = Request::builder()
        .method(Method::GET)
        .uri("/admin")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Without auth
    let (app, _engine) = setup().await;
    let req = Request::builder()
        .method(Method::GET)
        .uri("/admin")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ============================================================================
// Additional coverage tests for endpoints / error paths.
// ============================================================================

/// PUT method on /write should hit the same handler as POST.
#[tokio::test]
async fn test_http_write_put() {
    let (app, engine) = setup().await;
    let body = serde_json::json!({
        "records": [{"key": "put_key", "ts": 100, "value": "aGVsbG8="}]
    });
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/write")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(engine);
}

/// `/write` with invalid base64 should return BAD_REQUEST and exercise the
/// `to_record` error branch of the JSON handler.
#[tokio::test]
async fn test_http_write_invalid_base64() {
    let (app, engine) = setup().await;
    let body = serde_json::json!({
        "records": [{"key": "bad", "ts": 1, "value_base64": "!!!not-base64!!!"}]
    });
    let req = Request::builder()
        .method(Method::POST)
        .uri("/write")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    drop(engine);
}

/// `/write/binary` should accept a valid V1 frame and return written count.
#[tokio::test]
async fn test_http_write_binary() {
    let (app, engine) = setup().await;
    let rec = Record {
        key: b"bin_key".to_vec(),
        ts: 100,
        expire_at: i64::MAX,
        value: b"v".to_vec(),
    };
    let frame = flowdb::udp::encode_frame(&[rec], None);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/write/binary")
        .body(Body::from(frame))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["written"], 1);
    drop(engine);
}

/// `/write/binary` with garbage payload should yield BAD_REQUEST.
#[tokio::test]
async fn test_http_write_binary_invalid() {
    let (app, engine) = setup().await;
    let req = Request::builder()
        .method(Method::POST)
        .uri("/write/binary")
        .body(Body::from(&b"not a frame"[..]))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    drop(engine);
}

/// `/write/binary` with auth enabled but no key returns UNAUTHORIZED.
#[tokio::test]
async fn test_http_write_binary_auth_required() {
    let (app, engine) = setup_with_auth(vec!["secret".into()]).await;
    let req = Request::builder()
        .method(Method::POST)
        .uri("/write/binary")
        .body(Body::from(vec![0u8]))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    drop(engine);
}

/// `/query` with auth enabled but no key returns UNAUTHORIZED.
#[tokio::test]
async fn test_http_query_auth_required() {
    let (app, engine) = setup_with_auth(vec!["secret".into()]).await;
    let req = Request::builder()
        .uri("/query?prefix=x")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    drop(engine);
}

/// `/record` DELETE removes a record and returns ok=true.
#[tokio::test]
async fn test_http_delete_record() {
    let (app, engine) = setup().await;
    engine
        .write_batch(&[Record {
            key: b"del".to_vec(),
            ts: 100,
            expire_at: i64::MAX,
            value: vec![1],
        }])
        .await
        .unwrap();

    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/record?key=del&ts=100")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(engine);
}

/// `/record` DELETE with auth required rejects without a key.
#[tokio::test]
async fn test_http_delete_record_auth_required() {
    let (app, engine) = setup_with_auth(vec!["secret".into()]).await;
    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/record?key=del&ts=100")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    drop(engine);
}

/// `/record` PATCH modifies a record's value.
#[tokio::test]
async fn test_http_patch_record() {
    let (app, engine) = setup().await;
    engine
        .write_batch(&[Record {
            key: b"patch".to_vec(),
            ts: 100,
            expire_at: i64::MAX,
            value: b"old".to_vec(),
        }])
        .await
        .unwrap();

    let body = serde_json::json!({"key": "patch", "ts": 100, "value": "bmV3"});
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/record")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 1);
    drop(engine);
}

/// `/record` PATCH with invalid base64 returns BAD_REQUEST.
#[tokio::test]
async fn test_http_patch_record_invalid_base64() {
    let (app, engine) = setup().await;
    engine
        .write_batch(&[Record {
            key: b"patch".to_vec(),
            ts: 100,
            expire_at: i64::MAX,
            value: b"old".to_vec(),
        }])
        .await
        .unwrap();

    let body = serde_json::json!({"key": "patch", "ts": 100, "value_base64": "@@@@@"});
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/record")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    drop(engine);
}

/// `/record` PATCH on missing record returns INTERNAL_SERVER_ERROR.
#[tokio::test]
async fn test_http_patch_record_missing() {
    let (app, engine) = setup().await;
    let body = serde_json::json!({"key": "ghost", "ts": 1, "value": "d29ybGQ="});
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/record")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    drop(engine);
}

/// HTTP /write rejects batches larger than MAX_BATCH_RECORDS (10 000).
#[tokio::test]
async fn test_write_batch_too_large() {
    let (app, engine) = setup().await;
    let big_batch: Vec<_> = (0..10_001)
        .map(|i| serde_json::json!({"key": format!("k{}", i), "ts": 0, "value": "v"}))
        .collect();
    let body = serde_json::json!({"records": big_batch});
    let req = Request::builder()
        .method(Method::POST)
        .uri("/write")
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    drop(engine);
}

/// HTTP /write rejects records whose key exceeds MAX_KEY_BYTES (4 KiB).
#[tokio::test]
async fn test_write_key_too_long() {
    let (app, engine) = setup().await;
    let big_key = "x".repeat(4097);
    let body = serde_json::json!({"records": [{"key": big_key, "ts": 0}]});
    let req = Request::builder()
        .method(Method::POST)
        .uri("/write")
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    drop(engine);
}

/// HTTP /write rejects records whose value exceeds MAX_VAL_BYTES (64 KiB).
#[tokio::test]
async fn test_write_value_too_long() {
    let (app, engine) = setup().await;
    let big_val = general_purpose::STANDARD.encode(vec![0u8; 65537]);
    let body = serde_json::json!({"records": [{"key": "k", "ts": 0, "value_base64": big_val}]});
    let req = Request::builder()
        .method(Method::POST)
        .uri("/write")
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    drop(engine);
}

/// `/record` PATCH with auth required rejects without a key.
#[tokio::test]
async fn test_http_patch_record_auth_required() {
    let (app, engine) = setup_with_auth(vec!["secret".into()]).await;
    let body = serde_json::json!({"key": "x", "ts": 1, "value": "d29ybGQ="});
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/record")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    drop(engine);
}

/// `/range` DELETE removes a range of records.
#[tokio::test]
async fn test_http_delete_range() {
    let (app, engine) = setup().await;
    engine
        .write_batch(&[
            Record {
                key: b"a".to_vec(),
                ts: 100,
                expire_at: i64::MAX,
                value: vec![1],
            },
            Record {
                key: b"b".to_vec(),
                ts: 100,
                expire_at: i64::MAX,
                value: vec![1],
            },
            Record {
                key: b"c".to_vec(),
                ts: 100,
                expire_at: i64::MAX,
                value: vec![1],
            },
        ])
        .await
        .unwrap();

    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/range?key_start=a&key_end=c")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(engine);
}

/// `/range` DELETE with auth required rejects without a key.
#[tokio::test]
async fn test_http_delete_range_auth_required() {
    let (app, engine) = setup_with_auth(vec!["secret".into()]).await;
    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/range?key_start=a&key_end=z")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    drop(engine);
}

/// `/admin/patch` with valid key and existing record succeeds.
#[tokio::test]
async fn test_admin_patch_with_data() {
    let key = "adm-key";
    let (app, engine) = setup_with_auth(vec![key.into()]).await;
    engine
        .write_batch(&[Record {
            key: b"adm".to_vec(),
            ts: 1,
            expire_at: i64::MAX,
            value: b"v".to_vec(),
        }])
        .await
        .unwrap();
    let body = serde_json::json!({"key": "adm", "ts": 1, "value": "dmFsdWUy"});
    let req = admin_request("/admin/patch", Some(body), Some(key));
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(engine);
}

/// `/admin/patch` with invalid base64 returns BAD_REQUEST.
#[tokio::test]
async fn test_admin_patch_invalid_base64() {
    let key = "adm-key";
    let (app, engine) = setup_with_auth(vec![key.into()]).await;
    let body = serde_json::json!({"key": "adm", "ts": 1, "value_base64": "@@@@"});
    let req = admin_request("/admin/patch", Some(body), Some(key));
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    drop(engine);
}

/// `/admin/delete` with valid key returns OK.
#[tokio::test]
async fn test_admin_delete_with_data() {
    let key = "adm-key";
    let (app, engine) = setup_with_auth(vec![key.into()]).await;
    engine
        .write_batch(&[Record {
            key: b"adm_del".to_vec(),
            ts: 5,
            expire_at: i64::MAX,
            value: vec![],
        }])
        .await
        .unwrap();
    let body = serde_json::json!({"key": "adm_del", "ts": 5});
    let req = admin_request("/admin/delete", Some(body), Some(key));
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(engine);
}
