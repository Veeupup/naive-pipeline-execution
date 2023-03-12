use arrow::record_batch::RecordBatch;

use crate::{graph::Index, Result};
use std::{
    collections::VecDeque,
    fmt::Display,
    sync::{Arc, Mutex},
};

pub type SharedData = Mutex<VecDeque<RecordBatch>>;
pub type SharedDataPtr = Arc<SharedData>;

#[derive(Debug, Clone, Copy)]
pub enum ProcessorType {
    Source,
    Transform,
    Sink,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessorState {
    Ready,    // can be scheduled
    Waiting,  // waiting for upstream processor to finish
    Running,  // is running
    Finished, // will no longer be scheduled
}

#[derive(Debug)]
pub struct Context {
    pub processor_state: Mutex<ProcessorState>,
    pub prev_processors: Mutex<Vec<Arc<dyn Processor>>>,
    pub processor_type: ProcessorType,
    // pub node_index: Index,
}

impl Context {
    pub fn new(processor_type: ProcessorType) -> Self {
        Context {
            processor_state: Mutex::new(ProcessorState::Waiting),
            prev_processors: Mutex::new(vec![]),
            processor_type,
        }
    }

    pub fn set_state(&self, state: ProcessorState) {
        let mut processor_state = self.processor_state.lock().unwrap();
        *processor_state = state;
    }

    pub fn get_state(&self) -> ProcessorState {
        let processor_state = self.processor_state.lock().unwrap();
        *processor_state
    }

    pub fn set_prev_processors(&self, processors: Vec<Arc<dyn Processor>>) {
        let mut prev_processor = self.prev_processors.lock().unwrap();
        *prev_processor = processors;
    }

    pub fn get_prev_processors(&self) -> Vec<Arc<dyn Processor>> {
        let prev_processor = self.prev_processors.lock().unwrap();
        prev_processor.clone()
    }
}

pub trait Processor: Send + Sync + std::fmt::Debug + std::fmt::Display {
    fn name(&self) -> &'static str;

    fn connect_from_input(&mut self, input: Vec<Arc<dyn Processor>>);

    fn execute(&mut self) -> Result<()>;

    fn output_port(&self) -> SharedDataPtr;

    fn context(&self) -> Arc<Context>;
}

#[derive(Debug)]
pub struct EmptyProcessor {
    name: &'static str,
    processor_context: Arc<Context>,
    output: SharedDataPtr,
}

impl Display for EmptyProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EmptyProcessor [name: {}]", self.name)
    }
}

impl EmptyProcessor {
    pub fn new(name: &'static str) -> Self {
        EmptyProcessor {
            name,
            processor_context: Arc::new(Context {
                processor_state: Mutex::new(ProcessorState::Ready),
                prev_processors: Mutex::new(vec![]),
                processor_type: ProcessorType::Source,
            }),
            output: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl Processor for EmptyProcessor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, input: Vec<Arc<dyn Processor>>) {
        self.context().set_prev_processors(input);
    }

    fn execute(&mut self) -> Result<()> {
        Ok(())
    }

    fn output_port(&self) -> SharedDataPtr {
        self.output.clone()
    }

    fn context(&self) -> Arc<Context> {
        self.processor_context.clone()
    }
}
