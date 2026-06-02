use crate::admin::ADMIN_HTML;
use crate::auth::AuthState;
use crate::engine::Engine;
use crate::record::{Query as DbQuery, Record};
use crate::stats::EngineStats;
use axum::extract::DefaultBodyLimit;
use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, patch, post},
    Json, Router,
};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
    pub auth: AuthState,
}

#[derive(Debug, Deserialize)]
pub struct WriteRequest {
    pub records: Vec<WriteRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteRecord {
    pub key: String,
    pub ts: i64,
    #[serde(default)]
    pub ttl_secs: Option<u64>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub value_base64: Option<String>,
}

impl WriteRecord {
    pub fn to_record(&self) -> crate::error::Result<Record> {
        let value = if let Some(b64) = &self.value_base64 {
            general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| crate::error::FlowError::Other(format!("base64 decode: {}", e)))?
        } else if let Some(v) = &self.value {
            v.as_bytes().to_vec()
        } else {
            vec![]
        };
        Ok(Record {
            key: self.key.clone(),
            ts: self.ts,
            expire_at: i64::MAX,
            value,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub prefix: Option<String>,
    pub key_start: Option<String>,
    pub key_end: Option<String>,
    pub ts_start: Option<i64>,
    pub ts_end: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteParams {
    pub key: String,
    pub ts: i64,
}

#[derive(Debug, Deserialize)]
pub struct DeleteRangeParams {
    pub key_start: String,
    pub key_end: String,
}

#[derive(Debug, Deserialize)]
pub struct PatchRequest {
    pub key: String,
    pub ts: i64,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub value_base64: Option<String>,
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

#[derive(Serialize)]
pub struct WriteResponse {
    written: usize,
}

#[derive(Serialize)]
pub struct QueryResponse {
    records: Vec<QueryRecord>,
    count: usize,
}

#[derive(Serialize)]
struct QueryRecord {
    key: String,
    ts: i64,
    expire_at: i64,
    value: String,
}

#[derive(Serialize)]
pub struct HealthResponse {
    status: String,
}

#[derive(Serialize)]
pub struct ActionResponse {
    ok: bool,
    message: String,
}

fn check_auth(auth: &AuthState, headers: &HeaderMap) -> StatusCode {
    if !auth.is_enabled() {
        return StatusCode::OK;
    }
    if auth.check(headers, None) {
        StatusCode::OK
    } else {
        StatusCode::UNAUTHORIZED
    }
}

pub async fn write_json(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<WriteRequest>,
) -> Result<(StatusCode, Json<WriteResponse>), (StatusCode, Json<ActionResponse>)> {
    state.engine.stats();
    if check_auth(&state.auth, &headers) != StatusCode::OK {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ActionResponse {
                ok: false,
                message: "unauthorized".into(),
            }),
        ));
    }

    let records: Vec<Record> = req
        .records
        .iter()
        .map(|wr| wr.to_record())
        .collect::<Result<_, _>>()
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ActionResponse {
                    ok: false,
                    message: e.to_string(),
                }),
            )
        })?;

    let count = records.len();
    state.engine.write_batch(&records).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ActionResponse {
                ok: false,
                message: e.to_string(),
            }),
        )
    })?;

    Ok((StatusCode::OK, Json(WriteResponse { written: count })))
}

pub async fn write_binary(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<WriteResponse>), (StatusCode, Json<ActionResponse>)> {
    if check_auth(&state.auth, &headers) != StatusCode::OK {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ActionResponse {
                ok: false,
                message: "unauthorized".into(),
            }),
        ));
    }

    let records = crate::udp::decode_frame(&body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ActionResponse {
                ok: false,
                message: e.to_string(),
            }),
        )
    })?;

    let count = records.len();
    state.engine.write_batch(&records).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ActionResponse {
                ok: false,
                message: e.to_string(),
            }),
        )
    })?;

    Ok((StatusCode::OK, Json(WriteResponse { written: count })))
}

pub async fn query_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ActionResponse>)> {
    if check_auth(&state.auth, &headers) != StatusCode::OK {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ActionResponse {
                ok: false,
                message: "unauthorized".into(),
            }),
        ));
    }

    let db_query = build_query(&params);
    let results = state.engine.query(db_query).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ActionResponse {
                ok: false,
                message: e.to_string(),
            }),
        )
    })?;

    let records: Vec<QueryRecord> = results
        .into_iter()
        .map(|r| QueryRecord {
            key: r.key,
            ts: r.ts,
            expire_at: r.expire_at,
            value: general_purpose::STANDARD.encode(&r.value),
        })
        .collect();

    let count = records.len();
    Ok(Json(QueryResponse { records, count }))
}

pub async fn stats_handler(State(state): State<AppState>) -> Json<EngineStats> {
    Json(state.engine.stats())
}

pub async fn metrics_handler(State(state): State<AppState>) -> String {
    state.engine.metrics_text()
}

pub async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
    })
}

pub async fn admin_handler() -> axum::response::Html<&'static str> {
    axum::response::Html(ADMIN_HTML)
}

pub async fn admin_flush(
    State(state): State<AppState>,
) -> Result<Json<ActionResponse>, (StatusCode, Json<ActionResponse>)> {
    state
        .engine
        .flush()
        .await
        .map(|_| {
            Json(ActionResponse {
                ok: true,
                message: "flush completed".into(),
            })
        })
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ActionResponse {
                    ok: false,
                    message: e.to_string(),
                }),
            )
        })
}

pub async fn admin_gc(
    State(state): State<AppState>,
) -> Result<Json<ActionResponse>, (StatusCode, Json<ActionResponse>)> {
    state
        .engine
        .trigger_gc()
        .await
        .map(|purged| {
            Json(ActionResponse {
                ok: true,
                message: format!("gc completed, purged {}", purged),
            })
        })
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ActionResponse {
                    ok: false,
                    message: e.to_string(),
                }),
            )
        })
}

pub async fn admin_compact(
    State(state): State<AppState>,
) -> Result<Json<ActionResponse>, (StatusCode, Json<ActionResponse>)> {
    state
        .engine
        .trigger_compaction()
        .await
        .map(|did| {
            Json(ActionResponse {
                ok: true,
                message: format!("compaction: ran={}", did),
            })
        })
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ActionResponse {
                    ok: false,
                    message: e.to_string(),
                }),
            )
        })
}

pub async fn admin_query(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ActionResponse>)> {
    let db_query = build_query(&params);
    let results = state.engine.query(db_query).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ActionResponse {
                ok: false,
                message: e.to_string(),
            }),
        )
    })?;

    let records: Vec<QueryRecord> = results
        .into_iter()
        .map(|r| QueryRecord {
            key: r.key,
            ts: r.ts,
            expire_at: r.expire_at,
            value: general_purpose::STANDARD.encode(&r.value),
        })
        .collect();

    let count = records.len();
    Ok(Json(QueryResponse { records, count }))
}

pub async fn admin_delete(
    State(state): State<AppState>,
    Json(req): Json<PatchRequest>,
) -> Result<Json<ActionResponse>, (StatusCode, Json<ActionResponse>)> {
    state
        .engine
        .delete_batch(&[(req.key.clone(), req.ts)])
        .await
        .map(|_| {
            Json(ActionResponse {
                ok: true,
                message: "deleted".into(),
            })
        })
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ActionResponse {
                    ok: false,
                    message: e.to_string(),
                }),
            )
        })
}

pub async fn admin_patch(
    State(state): State<AppState>,
    Json(req): Json<PatchRequest>,
) -> Result<Json<ActionResponse>, (StatusCode, Json<ActionResponse>)> {
    let new_value = if let Some(b64) = &req.value_base64 {
        Some(general_purpose::STANDARD.decode(b64).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ActionResponse {
                    ok: false,
                    message: format!("base64 decode: {}", e),
                }),
            )
        })?)
    } else {
        req.value.as_ref().map(|v| v.as_bytes().to_vec())
    };

    state
        .engine
        .patch_record(&req.key, req.ts, new_value, req.ttl_secs)
        .await
        .map(|_| {
            Json(ActionResponse {
                ok: true,
                message: "patched".into(),
            })
        })
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ActionResponse {
                    ok: false,
                    message: e.to_string(),
                }),
            )
        })
}

pub async fn delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<DeleteParams>,
) -> Result<Json<ActionResponse>, (StatusCode, Json<ActionResponse>)> {
    if check_auth(&state.auth, &headers) != StatusCode::OK {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ActionResponse {
                ok: false,
                message: "unauthorized".into(),
            }),
        ));
    }

    state
        .engine
        .delete_batch(&[(params.key.clone(), params.ts)])
        .await
        .map(|_| {
            Json(ActionResponse {
                ok: true,
                message: "deleted".into(),
            })
        })
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ActionResponse {
                    ok: false,
                    message: e.to_string(),
                }),
            )
        })
}

pub async fn patch_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PatchRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ActionResponse>)> {
    if check_auth(&state.auth, &headers) != StatusCode::OK {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ActionResponse {
                ok: false,
                message: "unauthorized".into(),
            }),
        ));
    }

    let new_value = if let Some(b64) = &req.value_base64 {
        Some(general_purpose::STANDARD.decode(b64).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ActionResponse {
                    ok: false,
                    message: format!("base64 decode: {}", e),
                }),
            )
        })?)
    } else {
        req.value.as_ref().map(|v| v.as_bytes().to_vec())
    };

    let updated = state
        .engine
        .patch_record(&req.key, req.ts, new_value, req.ttl_secs)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ActionResponse {
                    ok: false,
                    message: e.to_string(),
                }),
            )
        })?;

    Ok(Json(QueryResponse {
        records: vec![QueryRecord {
            key: updated.key,
            ts: updated.ts,
            expire_at: updated.expire_at,
            value: general_purpose::STANDARD.encode(&updated.value),
        }],
        count: 1,
    }))
}

fn build_query(params: &QueryParams) -> DbQuery {
    match (&params.prefix, &params.key_start, &params.ts_start) {
        (Some(key), _, Some(ts_start)) => {
            let ts_end = params.ts_end.unwrap_or(i64::MAX);
            DbQuery::prefix_time_range(key, *ts_start, ts_end)
        }
        (Some(key), _, None) => DbQuery::prefix(key),
        (None, Some(ks), Some(ts_start)) => {
            let ke = params.key_end.as_deref().unwrap_or("~");
            let ts_end = params.ts_end.unwrap_or(i64::MAX);
            DbQuery::key_time_range(ks, ke, *ts_start, ts_end)
        }
        (None, Some(ks), None) => {
            let ke = params.key_end.as_deref().unwrap_or("~");
            DbQuery::key_range(ks, ke)
        }
        (None, None, Some(ts_start)) => {
            let ts_end = params.ts_end.unwrap_or(i64::MAX);
            DbQuery::time_range(*ts_start, ts_end)
        }
        _ => DbQuery::prefix(""),
    }
}

pub async fn delete_range_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<DeleteRangeParams>,
) -> Result<Json<ActionResponse>, (StatusCode, Json<ActionResponse>)> {
    if check_auth(&state.auth, &headers) != StatusCode::OK {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ActionResponse {
                ok: false,
                message: "unauthorized".into(),
            }),
        ));
    }

    state
        .engine
        .delete_range(&params.key_start, &params.key_end)
        .await
        .map(|_| {
            Json(ActionResponse {
                ok: true,
                message: "range deleted".into(),
            })
        })
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ActionResponse {
                    ok: false,
                    message: e.to_string(),
                }),
            )
        })
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/write", post(write_json).put(write_json))
        .route("/write/binary", post(write_binary))
        .route("/query", get(query_handler))
        .route("/record", delete(delete_handler))
        .route("/record", patch(patch_handler))
        .route("/range", delete(delete_range_handler))
        .route("/stats", get(stats_handler))
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/admin", get(admin_handler))
        .route("/admin/flush", post(admin_flush))
        .route("/admin/gc", post(admin_gc))
        .route("/admin/compact", post(admin_compact))
        .route("/admin/query", get(admin_query))
        .route("/admin/delete", post(admin_delete))
        .route("/admin/patch", post(admin_patch))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

pub async fn start_http_server(state: AppState, addr: SocketAddr) -> crate::error::Result<()> {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KeyFilter;

    #[test]
    fn test_build_query_prefix() {
        let params = QueryParams {
            prefix: Some("key1".into()),
            key_start: None,
            key_end: None,
            ts_start: None,
            ts_end: None,
        };
        let q = build_query(&params);
        match q.key_filter {
            KeyFilter::Prefix(k) => assert_eq!(k, b"key1".as_slice()),
            _ => panic!("expected prefix"),
        }
    }

    #[test]
    fn test_build_query_key_range() {
        let params = QueryParams {
            prefix: None,
            key_start: Some("a".into()),
            key_end: Some("b".into()),
            ts_start: None,
            ts_end: None,
        };
        let q = build_query(&params);
        match q.key_filter {
            KeyFilter::Range { start, end } => {
                assert_eq!(start, b"a".as_slice());
                assert_eq!(end, b"b".as_slice());
            }
            _ => panic!("expected range"),
        }
    }

    #[test]
    fn test_build_query_time_range() {
        let params = QueryParams {
            prefix: None,
            key_start: None,
            key_end: None,
            ts_start: Some(100),
            ts_end: Some(200),
        };
        let q = build_query(&params);
        assert_eq!(q.time_range, Some((100, 200)));
    }

    #[test]
    fn test_write_record_to_record() {
        let wr = WriteRecord {
            key: "test".into(),
            ts: 100,
            ttl_secs: Some(3600),
            value_base64: Some(general_purpose::STANDARD.encode(b"hello")),
            value: None,
        };
        let rec = wr.to_record().unwrap();
        assert_eq!(rec.key, "test");
        assert_eq!(rec.value, b"hello");
    }

    #[test]
    fn test_build_query_prefix_time_range() {
        let params = QueryParams {
            prefix: Some("key1".into()),
            key_start: None,
            key_end: None,
            ts_start: Some(100),
            ts_end: Some(200),
        };
        let q = build_query(&params);
        match q.key_filter {
            KeyFilter::Prefix(k) => assert_eq!(k, b"key1".as_slice()),
            _ => panic!("expected prefix"),
        }
        assert_eq!(q.time_range, Some((100, 200)));
    }

    #[test]
    fn test_build_query_prefix_time_range_no_end() {
        let params = QueryParams {
            prefix: Some("key1".into()),
            key_start: None,
            key_end: None,
            ts_start: Some(100),
            ts_end: None,
        };
        let q = build_query(&params);
        match q.key_filter {
            KeyFilter::Prefix(k) => assert_eq!(k, b"key1".as_slice()),
            _ => panic!("expected prefix"),
        }
        assert_eq!(q.time_range, Some((100, i64::MAX)));
    }

    #[test]
    fn test_build_query_key_time_range() {
        let params = QueryParams {
            prefix: None,
            key_start: Some("a".into()),
            key_end: Some("z".into()),
            ts_start: Some(100),
            ts_end: Some(200),
        };
        let q = build_query(&params);
        match q.key_filter {
            KeyFilter::Range { start, end } => {
                assert_eq!(start, b"a".as_slice());
                assert_eq!(end, b"z".as_slice());
            }
            _ => panic!("expected range"),
        }
        assert_eq!(q.time_range, Some((100, 200)));
    }

    #[test]
    fn test_build_query_key_time_range_no_end() {
        let params = QueryParams {
            prefix: None,
            key_start: Some("a".into()),
            key_end: None,
            ts_start: Some(100),
            ts_end: None,
        };
        let q = build_query(&params);
        match q.key_filter {
            KeyFilter::Range { start, end } => {
                assert_eq!(start, b"a".as_slice());
                assert_eq!(end, b"~".as_slice());
            }
            _ => panic!("expected range"),
        }
        assert_eq!(q.time_range, Some((100, i64::MAX)));
    }

    #[test]
    fn test_build_query_default() {
        let params = QueryParams {
            prefix: None,
            key_start: None,
            key_end: None,
            ts_start: None,
            ts_end: None,
        };
        let q = build_query(&params);
        match q.key_filter {
            KeyFilter::Prefix(k) => assert_eq!(k, b"".as_slice()),
            _ => panic!("expected prefix"),
        }
        assert!(q.time_range.is_none());
    }

    #[test]
    fn test_write_record_to_record_plain_value() {
        let wr = WriteRecord {
            key: "test".into(),
            ts: 100,
            ttl_secs: None,
            value: Some("hello".into()),
            value_base64: None,
        };
        let rec = wr.to_record().unwrap();
        assert_eq!(rec.value, b"hello");
    }

    #[test]
    fn test_write_record_to_record_empty() {
        let wr = WriteRecord {
            key: "test".into(),
            ts: 100,
            ttl_secs: None,
            value: None,
            value_base64: None,
        };
        let rec = wr.to_record().unwrap();
        assert!(rec.value.is_empty());
    }

    #[test]
    fn test_write_record_to_record_invalid_base64() {
        let wr = WriteRecord {
            key: "test".into(),
            ts: 100,
            ttl_secs: None,
            value: None,
            value_base64: Some("!!!invalid".into()),
        };
        assert!(wr.to_record().is_err());
    }

    #[test]
    fn test_write_record_to_record_base64_preferred() {
        let wr = WriteRecord {
            key: "test".into(),
            ts: 100,
            ttl_secs: None,
            value: Some("plain".into()),
            value_base64: Some(general_purpose::STANDARD.encode(b"binary")),
        };
        let rec = wr.to_record().unwrap();
        assert_eq!(rec.value, b"binary");
    }
}
