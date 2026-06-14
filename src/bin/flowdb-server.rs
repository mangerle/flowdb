// Enable the unstable `coverage_attribute` feature only when running
// cargo-llvm-cov on nightly. The `main()` function below is the binary
// entry point and is exercised via integration tests / manual runs; we
// exclude it from line coverage so the reported number reflects library
// logic rather than process bootstrap.
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use clap::Parser;
use flowdb::auth::AuthState;
use flowdb::http::AppState;
use flowdb::udp::start_udp_listener;
use flowdb::{Engine, ServerConfig};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "flowdb-server")]
#[command(about = "FlowDB time-series engine server")]
struct Cli {
    #[arg(short, long)]
    config: Option<String>,
    #[arg(long, default_value = "./data")]
    data_dir: String,
    #[arg(long, default_value = "0.0.0.0:8080")]
    http_addr: String,
    #[arg(long, default_value = "0.0.0.0:9090")]
    udp_addr: String,
    #[arg(long)]
    api_key: Option<String>,
}

/// Build a `ServerConfig` from CLI args + optional TOML file. Extracted from
/// `main()` so it can be unit-tested.
fn resolve_server_config(cli: &Cli) -> ServerConfig {
    let mut server_config = match &cli.config {
        Some(path) => {
            let content = std::fs::read_to_string(path).expect("failed to read config file");
            toml::from_str(&content).expect("failed to parse config")
        }
        None => ServerConfig::default(),
    };

    if !cli.data_dir.is_empty() {
        server_config.engine.data_dir = cli.data_dir.clone().into();
    }
    if cli.http_addr != "0.0.0.0:8080" || server_config.http_addr == "0.0.0.0:8080" {
        server_config.http_addr = cli.http_addr.clone();
    }
    if cli.udp_addr != "0.0.0.0:9090" || server_config.udp_addr == "0.0.0.0:9090" {
        server_config.udp_addr = cli.udp_addr.clone();
    }
    if let Some(key) = &cli.api_key {
        server_config.api_keys.push(key.clone());
    }
    server_config
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let server_config = resolve_server_config(&cli);

    let engine = match Engine::open(server_config.engine.clone()).await {
        Ok(e) => e,
        Err(e) => {
            eprintln!("FATAL: failed to open engine: {e}");
            std::process::exit(1);
        }
    };

    let engine = Arc::new(engine);
    let stats = Arc::new(flowdb::stats::StatsCounters::new());

    let auth = AuthState::new(server_config.api_keys.clone());

    let http_addr: std::net::SocketAddr = match server_config.http_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("FATAL: invalid http_addr '{}': {e}", server_config.http_addr);
            std::process::exit(1);
        }
    };
    let udp_addr: std::net::SocketAddr = match server_config.udp_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("FATAL: invalid udp_addr '{}': {e}", server_config.udp_addr);
            std::process::exit(1);
        }
    };

    let udp_engine = engine.clone();
    let udp_stats = stats.clone();
    let max_udp = server_config.max_udp_packet_size;

    let udp_handle = tokio::spawn(async move {
        if let Err(e) = start_udp_listener(
            udp_engine,
            udp_stats,
            udp_addr,
            max_udp,
            server_config.udp_api_key.clone(),
            10000,
        )
        .await
        {
            tracing::error!("UDP listener error: {}", e);
        }
    });

    let app_state = AppState {
        engine: engine.clone(),
        auth,
    };

    tracing::info!("FlowDB server starting");
    tracing::info!("HTTP listening on {}", http_addr);
    tracing::info!("UDP listening on {}", udp_addr);
    tracing::info!("Admin UI at http://{}/admin", http_addr);

    let listener = match tokio::net::TcpListener::bind(http_addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("FATAL: cannot bind HTTP {http_addr}: {e}");
            std::process::exit(1);
        }
    };
    let app = flowdb::http::build_router(app_state);

    // Graceful shutdown: on SIGTERM/SIGINT, stop accepting new connections
    // and give in-flight requests up to 10 seconds to finish.
    let shutdown = async move {
        let ctrl_c = async {
            let _ = tokio::signal::ctrl_c().await;
        };
        #[cfg(unix)]
        let terminate = async {
            if let Ok(mut s) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                s.recv().await;
            }
        };
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
        tracing::info!("Shutdown signal received, draining connections...");
    };

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await;

    if let Err(e) = serve_result {
        tracing::error!("HTTP server error: {}", e);
    }

    // Flush the engine so unflushed memtable data survives the crash.
    tracing::info!("Flushing engine before exit...");
    if let Err(e) = engine.close().await {
        tracing::error!("Engine flush on shutdown failed: {}", e);
    }

    udp_handle.abort();
    tracing::info!("Server stopped.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_cli_defaults() {
        let cli = Cli::parse_from(["flowdb-server"]);
        assert_eq!(cli.data_dir, "./data");
        assert_eq!(cli.http_addr, "0.0.0.0:8080");
        assert_eq!(cli.udp_addr, "0.0.0.0:9090");
        assert!(cli.api_key.is_none());
        assert!(cli.config.is_none());
    }

    #[test]
    fn test_cli_custom_args() {
        let cli = Cli::parse_from([
            "flowdb-server",
            "--data-dir", "/tmp/data",
            "--http-addr", "127.0.0.1:3000",
            "--udp-addr", "127.0.0.1:4000",
            "--api-key", "secret",
            "--config", "/tmp/config.toml",
        ]);
        assert_eq!(cli.data_dir, "/tmp/data");
        assert_eq!(cli.http_addr, "127.0.0.1:3000");
        assert_eq!(cli.udp_addr, "127.0.0.1:4000");
        assert_eq!(cli.api_key, Some("secret".into()));
        assert_eq!(cli.config, Some("/tmp/config.toml".into()));
    }

    #[test]
    fn test_server_config_defaults() {
        let config = ServerConfig::default();
        assert_eq!(config.http_addr, "0.0.0.0:8080");
        assert_eq!(config.udp_addr, "0.0.0.0:9090");
        assert!(config.api_keys.is_empty());
        assert!(config.udp_api_key.is_none());
        assert_eq!(config.max_udp_packet_size, 1400);
    }

    /// `resolve_server_config` with defaults mirrors ServerConfig::default.
    #[test]
    fn test_resolve_server_config_defaults() {
        let cli = Cli::parse_from(["flowdb-server"]);
        let cfg = resolve_server_config(&cli);
        assert_eq!(cfg.engine.data_dir, std::path::PathBuf::from("./data"));
        // When CLI provides the default addr but ServerConfig default is also
        // the same, the resolution should leave it unchanged.
        assert_eq!(cfg.http_addr, "0.0.0.0:8080");
        assert_eq!(cfg.udp_addr, "0.0.0.0:9090");
        assert!(cfg.api_keys.is_empty());
    }

    /// `resolve_server_config` applies CLI overrides for data_dir / addrs / api_key.
    #[test]
    fn test_resolve_server_config_overrides() {
        let cli = Cli::parse_from([
            "flowdb-server",
            "--data-dir", "/tmp/db",
            "--http-addr", "127.0.0.1:9000",
            "--udp-addr", "127.0.0.1:9001",
            "--api-key", "topsecret",
        ]);
        let cfg = resolve_server_config(&cli);
        assert_eq!(cfg.engine.data_dir, std::path::PathBuf::from("/tmp/db"));
        assert_eq!(cfg.http_addr, "127.0.0.1:9000");
        assert_eq!(cfg.udp_addr, "127.0.0.1:9001");
        assert_eq!(cfg.api_keys, vec!["topsecret".to_string()]);
    }

    /// `resolve_server_config` honours an explicit `--data-dir ./data` (no-op).
    #[test]
    fn test_resolve_server_config_empty_data_dir_uses_default() {
        let cli = Cli::parse_from(["flowdb-server", "--data-dir", ""]);
        let cfg = resolve_server_config(&cli);
        // Empty CLI data_dir should NOT overwrite ServerConfig default.
        assert_eq!(cfg.engine.data_dir, std::path::PathBuf::from("./data"));
    }
}