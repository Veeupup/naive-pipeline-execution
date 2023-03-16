

use crate::{
    graph::{RunningGraph},
    processor::*,
    Result,
};
use std::{
    collections::VecDeque,
    fmt::Display,
    sync::{Arc, Mutex},
};

// MergeProcessor is pipeline breaker
#[derive(Debug)]
pub struct MergeProcessor {
    name: &'static str,
    processor_context: Arc<Context>,
    input: Vec<SharedDataPtr>,
    output: SharedDataPtr,
}

impl Display for MergeProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MergeProcessor [name: {}]", self.name)
    }
}

impl MergeProcessor {
    pub fn new(name: &'static str, graph: Arc<Mutex<RunningGraph>>) -> Self {
        MergeProcessor {
            name,
            processor_context: Arc::new(Context::new(ProcessorType::Transform, graph)),
            input: vec![],
            output: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl Processor for MergeProcessor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, input: Vec<Arc<dyn Processor>>) {
        self.input = input.iter().map(|x| x.output_port()).collect::<Vec<_>>();
    }

    fn execute(&mut self) -> Result<()> {
        // check if all prev processors are finished
        let prev_processors = self.context().get_prev_processors();
        let finished = prev_processors
            .iter()
            .all(|x| x.context().get_state() == ProcessorState::Finished);

        if !finished {
            self.context().set_state(ProcessorState::Waiting);
            return Ok(());
        }

        // merge all input
        let mut outputs = self.output.lock().unwrap();
        for input in &self.input {
            let mut input = input.lock().unwrap();
            outputs.append(&mut input);
        }

        // set state to finished
        self.context().set_state(ProcessorState::Finished);

        // set next processor Ready
        self.set_next_processor_ready();

        Ok(())
    }

    fn output_port(&self) -> SharedDataPtr {
        self.output.clone()
    }

    fn context(&self) -> Arc<Context> {
        self.processor_context.clone()
    }
}
