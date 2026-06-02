#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Corruption in {file}: {msg}")]
    Corruption { file: String, msg: String },
    #[error("Engine is closed")]
    Closed,
    #[error("Write buffer full")]
    WriteBufferFull,
    #[error("Config error: {0}")]
    Config(String),
    #[error("SSTable not found: {0}")]
    SstNotFound(u32),
    #[error("Block not found: sst={sst_id}, block={block_idx}")]
    BlockNotFound { sst_id: u32, block_idx: u32 },
    #[error("Invalid magic: expected {expected:#x}, got {actual:#x}")]
    InvalidMagic { expected: u32, actual: u32 },
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, FlowError>;
