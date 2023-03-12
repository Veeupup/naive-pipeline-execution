#![feature(get_mut_unchecked)]
mod error;
mod pipeline;
mod processor;
mod source;
mod stream_block;
mod transform;

pub type Result<T> = std::result::Result<T, anyhow::Error>;
