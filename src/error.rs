/// Error type for all FlowDB operations.
///
/// Wraps I/O errors, corruption, configuration validation, JSON errors, and
/// JsonDB-layer errors into a single `FlowError` enum.
#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    /// Wraps [`std::io::Error`] from file system operations.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Data corruption detected in an SSTable or WAL segment.
    #[error("Corruption in {file}: {msg}")]
    Corruption { file: String, msg: String },
    /// Operation attempted on a closed engine.
    #[error("Engine is closed")]
    Closed,
    /// Write stalled because the write buffer is full (backpressure).
    #[error("Write buffer full")]
    WriteBufferFull,
    /// Invalid configuration value (caught by [`Config::validate`]).
    #[error("Config error: {0}")]
    Config(String),
    /// SSTable file not found by ID.
    #[error("SSTable not found: {0}")]
    SstNotFound(u32),
    /// Block index entry refers to a non-existent block.
    #[error("Block not found: sst={sst_id}, block={block_idx}")]
    BlockNotFound { sst_id: u32, block_idx: u32 },
    /// SSTable header magic number mismatch (wrong file format).
    #[error("Invalid magic: expected {expected:#x}, got {actual:#x}")]
    InvalidMagic { expected: u32, actual: u32 },
    /// JSON serialization / deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// JsonDB-level error (store not found, unique constraint violation, etc.).
    #[error("JsonDB: {0}")]
    JsonDb(String),
    /// Catch-all error for miscellaneous failures.
    #[error("{0}")]
    Other(String),
}

/// Convenience type alias for `Result<T, FlowError>`.
pub type Result<T> = std::result::Result<T, FlowError>;
