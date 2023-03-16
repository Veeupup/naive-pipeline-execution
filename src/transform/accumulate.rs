use arrow::{
    array::{Int32Array, Float64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::{graph::RunningGraph, processor::*, Result};
use std::{
    collections::VecDeque,
    fmt::Display,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Copy)]
pub enum Accumulator {
    Sum,
    Count,
    Min,
    Max,
    Avg,
    Null,
}

// AccumulateProcessor is pipeline breaker
#[derive(Debug)]
pub struct AccumulateProcessor {
    name: &'static str,
    processor_context: Arc<Context>,
    accumulator: Accumulator,
    column_index: Option<usize>,
    input: Vec<SharedDataPtr>,
    output: SharedDataPtr,
}

impl Display for AccumulateProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AccumulateProcessor [name: {}]", self.name)
    }
}

impl AccumulateProcessor {
    pub fn new(
        name: &'static str,
        accumulator: Accumulator,
        column_index: Option<usize>,
        graph: Arc<Mutex<RunningGraph>>,
    ) -> Self {
        AccumulateProcessor {
            name,
            processor_context: Arc::new(Context::new(ProcessorType::Transform, graph)),
            accumulator,
            column_index,
            input: vec![],
            output: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl Processor for AccumulateProcessor {
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
        match self.accumulator {
            // Do nothing, just merge the output
            Accumulator::Null => {
                for input in &self.input {
                    let mut input = input.lock().unwrap();
                    while let Some(rb) = input.pop_front() {
                        outputs.push_back(rb);
                    }
                }
            }
            Accumulator::Sum => {
                let mut sum = 0;
                let column_index = self
                    .column_index
                    .expect("accumulate must specify column index");
                for input in &self.input {
                    let mut input = input.lock().unwrap();
                    while let Some(rb) = input.pop_front() {
                        let value = rb
                            .column(column_index)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        for val in value.iter() {
                            sum += val.unwrap_or(0);
                        }
                    }
                }
                outputs.push_back(RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("sum", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(vec![sum]))],
                )?);
            }
            Accumulator::Avg => {
                let mut sum = 0.0;
                let mut count = 0.0;
                let column_index = self
                    .column_index
                    .expect("accumulate must specify column index");
                for input in &self.input {
                    let mut input = input.lock().unwrap();
                    while let Some(rb) = input.pop_front() {
                        let value = rb
                            .column(column_index)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        for val in value.iter() {
                            if let Some(val) = val {
                                sum += val as f64;
                                count += 1.0;
                            }
                        }
                    }
                }
                outputs.push_back(RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "avg",
                        DataType::Float64,
                        false,
                    )])),
                    vec![Arc::new(Float64Array::from(vec![sum / count]))],
                )?);
            }
            _ => todo!(),
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
