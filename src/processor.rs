use crate::Result;
use std::{
    fmt::Display,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Copy)]
pub enum ProcessorType {
    Source,
    Transform,
    Sink,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessorState {
    Ready,   // can be scheduled
    Waiting, // waiting for upstream processor to finish
    Running, // is running
    Stopped, // will no longer be scheduled
}

#[derive(Debug)]
pub struct ProcessorContext {
    pub processor_state: Mutex<ProcessorState>,
    pub prev_processor: Mutex<Option<Arc<dyn Processor>>>,
    pub processor_type: ProcessorType,
}

impl ProcessorContext {
    pub fn new(processor_type: ProcessorType) -> Self {
        ProcessorContext {
            processor_state: Mutex::new(ProcessorState::Waiting),
            prev_processor: Mutex::new(None),
            processor_type,
        }
    }

    pub fn set_processor_state(&self, state: ProcessorState) {
        let mut processor_state = self.processor_state.lock().unwrap();
        *processor_state = state;
    }

    pub fn set_prev_processor(&self, processor: Arc<dyn Processor>) {
        let mut prev_processor = self.prev_processor.lock().unwrap();
        *prev_processor = Some(processor);
    }

    pub fn get_prev_processor(&self) -> Option<Arc<dyn Processor>> {
        let prev_processor = self.prev_processor.lock().unwrap();
        prev_processor.clone()
    }

    pub fn get_processor_state(&self) -> ProcessorState {
        let processor_state = self.processor_state.lock().unwrap();
        *processor_state
    }
}

pub trait Processor: Send + Sync + std::fmt::Debug + std::fmt::Display {
    fn name(&self) -> &'static str;

    fn connect_from_input(&mut self, input: Arc<dyn Processor>);

    fn execute(&self) -> Result<()>;

    fn processor_context(&self) -> Arc<ProcessorContext>;
}

#[derive(Debug)]
pub struct EmptyProcessor {
    name: &'static str,
    processor_context: Arc<ProcessorContext>,
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
            processor_context: Arc::new(ProcessorContext {
                processor_state: Mutex::new(ProcessorState::Ready),
                prev_processor: Mutex::new(None),
                processor_type: ProcessorType::Source,
            }),
        }
    }
}

impl Processor for EmptyProcessor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, input: Arc<dyn Processor>) {
        self.processor_context().set_prev_processor(input);
    }

    fn execute(&self) -> Result<()> {
        Ok(())
    }

    fn processor_context(&self) -> Arc<ProcessorContext> {
        self.processor_context.clone()
    }
}

#[derive(Debug)]
pub struct MergeProcessor {
    name: &'static str,
    processor_context: Arc<ProcessorContext>,
}

impl Display for MergeProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MergeProcessor [name: {}]", self.name)
    }
}

impl MergeProcessor {
    pub fn new(name: &'static str) -> Self {
        MergeProcessor {
            name,
            processor_context: Arc::new(ProcessorContext {
                processor_state: Mutex::new(ProcessorState::Waiting),
                prev_processor: Mutex::new(None),
                processor_type: ProcessorType::Transform,
            }),
        }
    }
}

impl Processor for MergeProcessor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, input: Arc<dyn Processor>) {
        self.processor_context().set_prev_processor(input);
    }

    fn execute(&self) -> Result<()> {
        Ok(())
    }

    fn processor_context(&self) -> Arc<ProcessorContext> {
        self.processor_context.clone()
    }
}
