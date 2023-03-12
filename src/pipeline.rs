/*
 * Created Date: Saturday, March 11th 2023, 6:43:34 pm
 * Author: Veeupup
 *
 * Inspired by Databend, Clickhouse
 * And this is just a toy project to implemeent a data processing pipeline
 */

use std::sync::{Arc, Mutex};

use crate::graph::Index;
use crate::graph::RunningGraph;
use crate::processor::Processor;
use crate::processor::ProcessorState;
use crate::transform::MergeProcessor;
use crate::Result;
use arrow::record_batch::RecordBatch;
use petgraph::stable_graph::{DefaultIx, NodeIndex, StableDiGraph};

#[derive(Debug)]
pub struct Pipeline {
    // DAG of processors
    // We can build a DAG of processors by adding edges to the graph
    // and then we can traverse the graph to execute the processors
    graph: Arc<Mutex<RunningGraph>>,

    // we will remember each levels pipe id to add transform for each pipe
    pipe_ids: Vec<Vec<Index>>,

    // which NodeIndex is ready to execute
    ready_nodes: Arc<Mutex<Vec<Index>>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline {
            graph: Arc::new(Mutex::new(RunningGraph::new())),
            pipe_ids: Vec::new(),
            ready_nodes: Arc::new(Mutex::new(vec![])),
        }
    }

    fn add_processor(&mut self, processor: Arc<dyn Processor>) -> NodeIndex {
        self.graph.lock().unwrap().add_processor(processor)
    }

    fn connect_processors(&mut self, from: NodeIndex, to: NodeIndex) {
        self.graph.lock().unwrap().add_edges(from, to);
    }

    pub fn add_source(&mut self, processor: Arc<dyn Processor>) {
        // source processor should be the first processors in the pipeline
        assert!(self.pipe_ids.len() <= 1);

        // source processor is always ready to execute
        // this is where we start to execute the pipeline
        processor.context().set_state(ProcessorState::Ready);

        let index = self.add_processor(processor);

        if self.pipe_ids.is_empty() {
            self.pipe_ids.push(vec![index]);
        } else {
            self.pipe_ids[0].push(index);
        }
    }

    /// Add a normal processor to the pipeline.
    ///
    /// processor1 --> processor1_1
    ///
    /// processor2 --> processor2_1
    ///
    /// processor3 --> processor3_1
    ///
    pub fn add_transform(&mut self, f: impl Fn() -> Arc<dyn Processor>) {
        // transform processor should never be the first processor in the pipeline
        assert!(!self.pipe_ids.is_empty());

        let last_ids = self.pipe_ids.last().unwrap().clone();
        let mut transform_ids = vec![];
        for pipe_id in last_ids {
            let mut processor = f();
            unsafe {
                let x = Arc::get_mut_unchecked(&mut processor);
                x.connect_from_input(vec![self
                    .graph
                    .lock()
                    .unwrap()
                    .get_processor_by_index(pipe_id)]);
            }

            let index = self.add_processor(processor);
            self.connect_processors(pipe_id, index);
            transform_ids.push(index);
        }

        self.pipe_ids.push(transform_ids);
    }

    /// Merge many(or one)-ways processors into one-way.
    ///
    /// processor1 --
    ///               \
    /// processor2      --> processor
    ///               /
    /// processor3 --
    ///
    pub fn merge_processor(&mut self) {
        assert!(!self.pipe_ids.is_empty());

        let last_ids = self.pipe_ids.last().unwrap().clone();
        let mut merge_processor = Arc::new(MergeProcessor::new("merge_processor"));

        let merge_processor_index = self.add_processor(merge_processor.clone());
        let mut prev_processors = vec![];
        for index in last_ids {
            self.connect_processors(index, merge_processor_index);
            prev_processors.push(self.graph.lock().unwrap().get_processor_by_index(index));
        }

        unsafe {
            let merge_processor = Arc::get_mut_unchecked(&mut merge_processor);
            merge_processor.connect_from_input(prev_processors);
        }
    }

    pub fn expand_processor() {
        todo!()
    }

    pub fn execute(&mut self) -> Result<Vec<RecordBatch>> {
        // check the graph is valid

        // traverse the graph and execute the processors
        // if the node state is Ready, execute it
        // if the node state is Running, ignore it
        // if all node state are stopped, stop the pipeline
        let mut all_processors = self.graph.lock().unwrap().get_all_processors();
        loop {
            // get the tasks that are ready to execute, we could avoid the scan of the graph
            // let ready_nodes = self.ready_nodes.lock().unwrap();
            // for node in ready_nodes.iter() {
            //     let mut processor = self.graph.lock().unwrap().node_weight_mut(*node).unwrap();
            //     unsafe {
            //         let x = Arc::get_mut_unchecked(&mut processor);
            //         x.execute()?;
            //     }
            // }

            // or we need to traverse the graph to find the ready nodes
            // FIXME: we should find a more efficient way to find the ready nodes
            let mut nodes_finished = 0;
            for processor in &mut all_processors {
                match processor.context().get_state() {
                    ProcessorState::Ready => {
                        // TODO(veeupup): run the processor in a thread pool
                        println!("execute processor: {:?})；", processor.name());
                        unsafe {
                            let x = Arc::get_mut_unchecked(processor);
                            x.execute()?;
                        }
                    }
                    ProcessorState::Running | ProcessorState::Waiting => {}
                    ProcessorState::Finished => {}
                }
            }

            // Now, all the ready nodes are stopped and we should stop the pipeline
            // break;
            if nodes_finished == all_processors.len() {
                break;
            }

            // sleep and then schedule the next round
            std::thread::sleep(std::time::Duration::from_millis(100));
            println!("try to schedule");
        }

        let last_processor = self.graph.lock().unwrap().get_last_processor();
        let output = last_processor
            .output_port()
            .lock()
            .unwrap()
            .drain(..)
            .collect();

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::{
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use petgraph::dot::Dot;

    use super::Pipeline;
    use crate::transform::*;
    use crate::{processor::EmptyProcessor, source::MemorySource, Result};

    #[test]
    pub fn test_build_pipeline() {
        let mut pipeline = Pipeline::new();

        pipeline.add_source(Arc::new(EmptyProcessor::new("source1")));
        pipeline.add_source(Arc::new(EmptyProcessor::new("source2")));
        pipeline.add_source(Arc::new(EmptyProcessor::new("source3")));
        pipeline.add_source(Arc::new(EmptyProcessor::new("source4")));

        pipeline.add_transform(|| Arc::new(EmptyProcessor::new("transform1")));

        pipeline.merge_processor();

        println!("{:#?}", pipeline.graph);

        println!("{:?}", Dot::new(&pipeline.graph));
    }

    #[test]
    pub fn test_execute_pipeline() -> Result<()> {
        let mut pipeline = Pipeline::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        pipeline.add_source(Arc::new(MemorySource::new(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )?])));

        pipeline.add_source(Arc::new(MemorySource::new(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![100, 110, 120])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )?])));

        pipeline.add_transform(|| Arc::new(ArithmeticTransform::new("add", Operator::Add, 10, 0)));

        pipeline.merge_processor();

        let output = pipeline.execute()?;

        let expected_data = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![11, 12, 13])),
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                ],
            )?,
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![110, 120, 130])),
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                ],
            )?,
        ];

        pretty_assertions::assert_eq!(output, expected_data);

        Ok(())
    }
}
