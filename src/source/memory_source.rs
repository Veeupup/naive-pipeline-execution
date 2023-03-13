use arrow::record_batch::RecordBatch;

use crate::graph::Index;
use crate::graph::RunningGraph;
use crate::processor::*;
use crate::Result;
use std::collections::VecDeque;
use std::{
    fmt::Display,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct MemorySource {
    pub processor_context: Arc<Context>,
    pub data: Vec<RecordBatch>,
    pub index: usize,
    pub output: SharedDataPtr,
}

impl Display for MemorySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemorySource")
    }
}

impl MemorySource {
    pub fn new(data: Vec<RecordBatch>, graph: Arc<Mutex<RunningGraph>>) -> Self {
        MemorySource {
            processor_context: Arc::new(Context::new(
                ProcessorType::Source,
                graph,
            )),
            data,
            index: 0,
            output: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl Processor for MemorySource {
    fn name(&self) -> &'static str {
        "MemorySource"
    }

    fn connect_from_input(&mut self, input: Vec<Arc<dyn Processor>>) {
        panic!("MemorySource should not have input")
    }

    fn execute(&mut self) -> Result<()> {
        if self.index < self.data.len() {
            let batch = self.data[self.index].clone();
            self.output.lock().unwrap().push_back(batch);
            self.index += 1;
        }
        if self.index == self.data.len() {
            self.context().set_state(ProcessorState::Finished);
        }
        Ok(())
    }

    fn output_port(&self) -> SharedDataPtr {
        self.output.clone()
    }

    fn context(&self) -> Arc<Context> {
        self.processor_context.clone()
    }
}
