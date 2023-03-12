use arrow::record_batch::RecordBatch;

use crate::processor::*;
use crate::Result;
use std::{
    fmt::Display,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct MemorySource {
    pub processor_context: Arc<ProcessorContext>,
    pub data: Vec<RecordBatch>,
    pub index: usize,
}

impl Display for MemorySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemorySource")
    }
}

impl MemorySource {
    pub fn new(data: Vec<RecordBatch>) -> Self {
        MemorySource {
            processor_context: Arc::new(ProcessorContext {
                processor_state: Mutex::new(ProcessorState::Ready),
                prev_processor: Mutex::new(None),
                processor_type: ProcessorType::Source,
            }),
            data,
            index: 0,
        }
    }
}

impl Processor for MemorySource {
    fn name(&self) -> &'static str {
        "MemorySource"
    }

    fn connect_from_input(&mut self, input: Arc<dyn Processor>) {
        panic!("MemorySource should not have input")
    }

    fn execute(&mut self) -> Result<()> {
        Ok(())
    }

    fn processor_context(&self) -> Arc<ProcessorContext> {
        self.processor_context.clone()
    }
}
