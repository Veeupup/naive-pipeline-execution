use arrow::array::Array;
use arrow::array::Int32Array;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::graph::RunningGraph;
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

#[derive(Debug)]
pub struct ArithmeticTransform {
    name: &'static str,
    processor_context: Arc<Context>,
    operator: Operator,
    l_column_index: usize,
    r_column_index: usize,
    input: SharedDataPtr,
    output: SharedDataPtr,
}

impl ArithmeticTransform {
    pub fn new(
        name: &'static str,
        operator: Operator,
        l_column_index: usize,
        r_column_index: usize,
        graph: Arc<Mutex<RunningGraph>>,
    ) -> Self {
        ArithmeticTransform {
            name,
            processor_context: Arc::new(Context::new(ProcessorType::Transform, graph)),
            operator,
            l_column_index,
            r_column_index,
            input: Arc::new(Mutex::new(VecDeque::new())),
            output: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl Display for ArithmeticTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArithmeticTransform [name: {}]", self.name)
    }
}

// To make it simple.. Just implement i32 data type...
impl Processor for ArithmeticTransform {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect_from_input(&mut self, input: Vec<Arc<dyn Processor>>) {
        assert_eq!(input.len(), 1);
        self.input = input[0].output_port();
    }

    fn execute(&mut self) -> Result<()> {
        assert_eq!(self.context().get_prev_processors().len(), 1);

        let mut input = self.input.lock().unwrap();

        for rb in input.drain(..) {
            let l_column = rb.column(self.l_column_index);
            let r_column = rb.column(self.r_column_index);

            // For simple, we just do i32 data type
            let new_column = match rb.schema().field(self.l_column_index).data_type() {
                DataType::Int32 => {
                    let l_array = l_column.as_any().downcast_ref::<Int32Array>().unwrap();
                    let r_array = r_column.as_any().downcast_ref::<Int32Array>().unwrap();

                    let mut new_column_builder = Int32Array::builder(l_array.len());
                    for i in 0..l_array.len() {
                        let l_value = l_array.value(i);
                        let r_value = r_array.value(i);
                        let result = match self.operator {
                            Operator::Add => l_value + r_value,
                            Operator::Subtract => l_value - r_value,
                            Operator::Multiply => l_value * r_value,
                            Operator::Divide => l_value / r_value,
                        };
                        new_column_builder.append_value(result);
                    }
                    let new_column = new_column_builder.finish();
                    Arc::new(new_column)
                }
                _ => todo!(),
            };

            let mut fields = vec![];
            let mut columns: Vec<Arc<dyn Array>> = vec![];

            let new_column_name = format!(
                "{} {} {}",
                rb.schema().field(self.l_column_index).name(),
                match self.operator {
                    Operator::Add => "+",
                    Operator::Subtract => "-",
                    Operator::Multiply => "*",
                    Operator::Divide => "/",
                },
                rb.schema().field(self.r_column_index).name()
            );
            let new_field = Field::new(&new_column_name, DataType::Int32, false);

            fields.push(new_field);
            columns.push(new_column);

            

            for (i, column) in rb.columns().iter().enumerate() {
                if i == self.l_column_index || i == self.r_column_index {
                    continue;
                } else {
                    fields.push(rb.schema().field(i).clone());
                    columns.push(column.clone());
                }
            }

            let schema = Arc::new(Schema::new(fields));
            let mut output = self.output.lock().unwrap();
            output.push_back(RecordBatch::try_new(schema, columns)?);
        }

        // try to set the processor state to finished
        let prev_processor = &self.context().get_prev_processors()[0];
        if prev_processor.context().get_state() == ProcessorState::Finished {
            self.context().set_state(ProcessorState::Finished);
        }

        // set next processor state to Ready
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
