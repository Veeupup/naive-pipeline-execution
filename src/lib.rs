#![feature(get_mut_unchecked)]
mod error;
mod graph;
mod pipeline;
mod processor;
mod source;
mod transform;

pub type Result<T> = std::result::Result<T, anyhow::Error>;

pub use error::*;
pub use graph::*;
pub use pipeline::*;
pub use processor::*;
pub use source::*;
pub use transform::*;
