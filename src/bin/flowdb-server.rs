use clap::Parser;
use flowdb::auth::AuthState;
use flowdb::http::{start_http_server, AppState};
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let mut server_config = match &cli.config {
        Some(path) => {
            let content = std::fs::read_to_string(path).expect("failed to read config file");
            toml::from_str(&content).expect("failed to parse config")
        }
        None => ServerConfig::default(),
    };

    if !cli.data_dir.is_empty() {
        server_config.engine.data_dir = cli.data_dir.into();
    }
    if cli.http_addr != "0.0.0.0:8080" || server_config.http_addr == "0.0.0.0:8080" {
        server_config.http_addr = cli.http_addr;
    }
    if cli.udp_addr != "0.0.0.0:9090" || server_config.udp_addr == "0.0.0.0:9090" {
        server_config.udp_addr = cli.udp_addr;
    }
    if let Some(key) = cli.api_key {
        server_config.api_keys.push(key);
    }

    let engine = Engine::open(server_config.engine.clone())
        .await
        .expect("failed to open engine");

    let engine = Arc::new(engine);
    let stats = Arc::new(flowdb::stats::StatsCounters::new());

    let auth = AuthState::new(server_config.api_keys.clone());

    let http_addr: std::net::SocketAddr =
        server_config.http_addr.parse().expect("invalid http addr");
    let udp_addr: std::net::SocketAddr = server_config.udp_addr.parse().expect("invalid udp addr");

    let udp_engine = engine.clone();
    let udp_stats = stats.clone();
    let max_udp = server_config.max_udp_packet_size;

    let udp_handle = tokio::spawn(async move {
        if let Err(e) = start_udp_listener(udp_engine, udp_stats, udp_addr, max_udp).await {
            tracing::error!("UDP listener error: {}", e);
        }
    });

    let app_state = AppState { engine, auth };

    tracing::info!("FlowDB server starting");
    tracing::info!("HTTP listening on {}", http_addr);
    tracing::info!("UDP listening on {}", udp_addr);
    tracing::info!("Admin UI at http://{}/admin", http_addr);

    if let Err(e) = start_http_server(app_state, http_addr).await {
        tracing::error!("HTTP server error: {}", e);
    }

    udp_handle.abort();
}
