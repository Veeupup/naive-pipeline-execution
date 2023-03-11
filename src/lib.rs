mod error;
mod pipeline;
mod processor;
mod stream_block;
mod transform;

pub type Result<T> = std::result::Result<T, anyhow::Error>;

