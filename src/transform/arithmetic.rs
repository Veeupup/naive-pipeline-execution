use arrow::array::Array;
use arrow::array::Int32Array;
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;

use crate::processor::*;
use crate::Result;
use std::collections::VecDeque;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

// TODO(veeupup) 计算应该在两个列之间进行，而不是和一个标量
#[derive(Debug)]
pub struct ArithmeticTransform<T> {
    name: &'static str,
    processor_context: Arc<ProcessorContext>,
    operator: Operator,
    value: T,
    column_index: usize,
    input: SharedDataPtr,
    output: SharedDataPtr,
}

impl<T> ArithmeticTransform<T> {
    pub fn new(name: &'static str, operator: Operator, value: T, column_index: usize) -> Self {
        ArithmeticTransform {
            name,
            processor_context: Arc::new(ProcessorContext {
                processor_state: Mutex::new(ProcessorState::Ready),
                prev_processors: Mutex::new(vec![]),
                processor_type: ProcessorType::Transform,
            }),
            operator,
            value,
            column_index,
            input: Arc::new(Mutex::new(VecDeque::new())),
            output: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl<T> Display for ArithmeticTransform<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArithmeticTransform [name: {}]", self.name)
    }
}

// To make it simple.. Just implement i32 data type...
impl Processor for ArithmeticTransform<i32> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, input: Vec<Arc<dyn Processor>>) {
        assert_eq!(input.len(), 1);
        self.input = input[0].output_port();
        self.processor_context().set_prev_processors(input);
    }

    fn execute(&mut self) -> Result<()> {
        assert_eq!(self.processor_context().get_prev_processors().len(), 1);

        let mut input = self.input.lock().unwrap();
        let mut output = self.output.lock().unwrap();

        for rb in input.drain(..) {
            // TODO(veeupup) handle the computation
            let column = rb.column(self.column_index);
            let new_column = match rb.schema().field(self.column_index).data_type() {
                Int32Type => {
                    let item = column.as_any().downcast_ref::<Int32Array>().unwrap();
                    let mut new_column_builder = Int32Array::builder(item.len());
                    for x in item {
                        let value = self.value;
                        let result = match self.operator {
                            Operator::Add => x.map(|x| x + value),
                            Operator::Subtract => x.map(|x| x + value),
                            Operator::Multiply => x.map(|x| x * value),
                            Operator::Divide => x.map(|x| x / value),
                        };
                        new_column_builder.append_option(result);
                    }
                    let new_column = new_column_builder.finish();
                    Arc::new(new_column)
                }
                _ => todo!(),
            };

            let mut columns: Vec<Arc<dyn Array>> = vec![];
            for (i, column) in rb.columns().iter().enumerate() {
                if i == self.column_index {
                    columns.push(new_column.clone());
                } else {
                    columns.push(column.clone());
                }
            }

            output.push_back(RecordBatch::try_new(rb.schema(), columns)?);
        }

        // try to set the processor state to finished
        let prev_processor = &self.processor_context().get_prev_processors()[0];
        if prev_processor.processor_context().get_processor_state() == ProcessorState::Finished {
            self.processor_context()
                .set_processor_state(ProcessorState::Finished);
        }

        Ok(())
    }

    fn output_port(&self) -> SharedDataPtr {
        self.output.clone()
    }

    fn processor_context(&self) -> Arc<ProcessorContext> {
        self.processor_context.clone()
    }
}
