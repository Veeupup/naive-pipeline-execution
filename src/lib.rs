#![feature(get_mut_unchecked)]
mod error;
mod pipeline;
mod processor;
mod source;
mod transform;
mod graph;

pub type Result<T> = std::result::Result<T, anyhow::Error>;
