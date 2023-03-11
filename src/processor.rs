use crate::Result;
use std::{sync::{Arc, Mutex}, fmt::Display};

#[derive(Debug, Clone, Copy)]
pub enum ProcessorType {
    Source,
    Transform,
    Sink,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessorState {
    Ready,
    Running,
    Stopped,
}

pub trait Processor: Send + Sync + std::fmt::Debug + std::fmt::Display {
    fn name(&self) -> &'static str;

    fn connect_from_input(&mut self, input: Arc<dyn Processor>);

    // just execute the processor
    fn execute(&self) -> Result<()>;

    fn processor_type(&self) -> ProcessorType;

    // judge whether the processor is ready to execute
    fn processor_state(&self) -> ProcessorState;

    // set the processor state
    fn set_processor_state(&self, state: ProcessorState);

    // get the prev processor of the current processor
    fn prev_processor(&self) -> Option<Arc<dyn Processor>>;
}

#[derive(Debug)]
pub struct EmptyProcessor {
    name: &'static str,
    processor_type: ProcessorType,
    processor_state: Mutex<ProcessorState>,
    prev_processor: Option<Arc<dyn Processor>>,
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
            processor_type: ProcessorType::Transform,
            processor_state: Mutex::new(ProcessorState::Ready),
            prev_processor: None,
        }
    }
}

impl Processor for EmptyProcessor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, input: Arc<dyn Processor>) {
        self.prev_processor = Some(input);
    }

    fn execute(&self) -> Result<()> {
        Ok(())
    }

    fn processor_type(&self) -> ProcessorType {
        self.processor_type
    }

    fn processor_state(&self) -> ProcessorState {
        self.processor_state.lock().unwrap().clone()
    }

    fn set_processor_state(&self, state: ProcessorState) {
        *self.processor_state.lock().unwrap() = state;
    }

    fn prev_processor(&self) -> Option<Arc<dyn Processor>> {
        self.prev_processor.clone()
    }
}

#[derive(Debug)]
pub struct MergeProcessor {
    name: &'static str,
    processor_type: ProcessorType,
    processor_state: Mutex<ProcessorState>,
    prev_processor: Option<Arc<dyn Processor>>,
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
            processor_type: ProcessorType::Transform,
            processor_state: Mutex::new(ProcessorState::Ready),
            prev_processor: None,
        }
    }
}

impl Processor for MergeProcessor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, input: Arc<dyn Processor>) {
        self.prev_processor = Some(input);
    }

    fn execute(&self) -> Result<()> {
        Ok(())
    }

    fn processor_type(&self) -> ProcessorType {
        self.processor_type
    }

    fn processor_state(&self) -> ProcessorState {
        self.processor_state.lock().unwrap().clone()
    }

    fn set_processor_state(&self, state: ProcessorState) {
        *self.processor_state.lock().unwrap() = state;
    }

    fn prev_processor(&self) -> Option<Arc<dyn Processor>> {
        self.prev_processor.clone()
    }
}
