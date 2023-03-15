use arrow::record_batch::RecordBatch;
use petgraph::adj::NodeIndex;

use crate::{
    graph::{Index, RunningGraph},
    Result,
};
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
    pub state: Mutex<ProcessorState>,
    pub processor_type: ProcessorType,
    pub node_index: Mutex<Index>,
    pub graph: Arc<Mutex<RunningGraph>>,
}

impl Context {
    pub fn new(processor_type: ProcessorType, graph: Arc<Mutex<RunningGraph>>) -> Self {
        Context {
            processor_type,
            graph,
            node_index: Mutex::new(NodeIndex::default()),
            state: Mutex::new(ProcessorState::Waiting),
        }
    }

    pub fn set_node_index(&self, node_index: Index) {
        let mut index = self.node_index.lock().unwrap();
        *index = node_index;
    }

    pub fn get_node_index(&self) -> Index {
        *self.node_index.lock().unwrap()
    }

    pub fn set_state(&self, state: ProcessorState) {
        let mut processor_state = self.state.lock().unwrap();
        *processor_state = state;
    }

    pub fn get_state(&self) -> ProcessorState {
        let processor_state = self.state.lock().unwrap();
        *processor_state
    }

    pub fn get_prev_processors(&self) -> Vec<Arc<dyn Processor>> {
        self.graph
            .lock()
            .unwrap()
            .get_prev_processors(self.get_node_index())
    }

    pub fn get_next_processors(&self) -> Vec<Arc<dyn Processor>> {
        self.graph
            .lock()
            .unwrap()
            .get_next_processors(self.get_node_index())
    }
}

pub trait Processor: Send + Sync + std::fmt::Debug + std::fmt::Display {
    fn name(&self) -> &'static str;

    fn connect_from_input(&mut self, input: Vec<Arc<dyn Processor>>);

    fn execute(&mut self) -> Result<()>;

    fn output_port(&self) -> SharedDataPtr;

    fn context(&self) -> Arc<Context>;

    fn set_next_processor_ready(&self) {
        let next_processors = self.context().get_next_processors();
        for processor in next_processors {
            processor.context().set_state(ProcessorState::Ready);
        }
    }
}

#[derive(Debug)]
pub struct EmptyProcessor {
    name: &'static str,
    context: Arc<Context>,
    output: SharedDataPtr,
}

impl Display for EmptyProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EmptyProcessor [name: {}]", self.name)
    }
}

impl EmptyProcessor {
    pub fn new(name: &'static str, graph: Arc<Mutex<RunningGraph>>) -> Self {
        EmptyProcessor {
            name,
            context: Arc::new(Context::new(ProcessorType::Source, graph)),
            output: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl Processor for EmptyProcessor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, _input: Vec<Arc<dyn Processor>>) {
        // self.context().set_prev_processors(input);
    }

    fn execute(&mut self) -> Result<()> {
        Ok(())
    }

    fn output_port(&self) -> SharedDataPtr {
        self.output.clone()
    }

    fn context(&self) -> Arc<Context> {
        self.context.clone()
    }
}
