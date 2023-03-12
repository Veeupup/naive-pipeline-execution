use arrow::record_batch::RecordBatch;

use crate::{pipeline::Index, processor::*, Result};
use std::{
    collections::VecDeque,
    fmt::Display,
    sync::{Arc, Mutex},
};

// MergeProcessor is pipeline breaker
#[derive(Debug)]
pub struct MergeProcessor {
    name: &'static str,
    processor_context: Arc<ProcessorContext>,
    input: Vec<SharedDataPtr>,
    output: SharedDataPtr,
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
                // TODO(veeupup) should be judge by prev processors
                processor_state: Mutex::new(ProcessorState::Ready),
                prev_processors: Mutex::new(vec![]),
                processor_type: ProcessorType::Transform,
            }),
            input: vec![],
            output: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn output(&self) -> Result<Vec<RecordBatch>> {
        assert_eq!(
            self.processor_context().get_processor_state(),
            ProcessorState::Finished
        );
        Ok(self.output.lock().unwrap().drain(..).collect())
    }
}

impl Processor for MergeProcessor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, input: Vec<Arc<dyn Processor>>) {
        self.input = input.iter().map(|x| x.output_port()).collect::<Vec<_>>();
        self.processor_context().set_prev_processors(input);
    }

    fn execute(&mut self) -> Result<()> {
        // check if all prev processors are finished
        let prev_processors = self.processor_context().get_prev_processors();
        let finished = prev_processors
            .iter()
            .all(|x| x.processor_context().get_processor_state() == ProcessorState::Finished);

        if !finished {
            return Ok(());
        }

        // merge all input
        let mut outputs = self.output.lock().unwrap();
        for input in &self.input {
            let mut input = input.lock().unwrap();
            println!("MergeProcessor: input: {:?}", input);
            outputs.append(&mut input);
        }

        // set state to finished
        self.processor_context()
            .set_processor_state(ProcessorState::Finished);

        Ok(())
    }

    fn output_port(&self) -> SharedDataPtr {
        self.output.clone()
    }

    fn processor_context(&self) -> Arc<ProcessorContext> {
        self.processor_context.clone()
    }
}
