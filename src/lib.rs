#![feature(get_mut_unchecked)]
mod error;
mod graph;
mod pipeline;
mod processor;
mod source;
mod transform;

pub type Result<T> = std::result::Result<T, anyhow::Error>;
