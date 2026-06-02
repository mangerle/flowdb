#![cfg(feature = "server")]

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
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
