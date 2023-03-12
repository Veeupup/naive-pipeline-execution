/*
 * Created Date: Saturday, March 11th 2023, 6:43:34 pm
 * Author: Veeupup
 *
 * Inspired by Databend, Clickhouse
 * And this is just a toy project to implemeent a data processing pipeline
 */

use std::sync::{Arc, Mutex};

use crate::processor::MergeProcessor;
use crate::processor::Processor;
use crate::processor::ProcessorState;
use crate::Result;
use petgraph::stable_graph::{DefaultIx, NodeIndex, StableDiGraph};

pub type Index = NodeIndex<DefaultIx>;

#[derive(Debug)]
pub struct Pipeline {
    // DAG of processors
    // We can build a DAG of processors by adding edges to the graph
    // and then we can traverse the graph to execute the processors
    graph: StableDiGraph<Arc<dyn Processor>, i32>,

    // we will remember each levels pipe id to add transform for each pipe
    pipe_ids: Vec<Vec<Index>>,

    // which NodeIndex is ready to execute
    ready_nodes: Arc<Mutex<Vec<Index>>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline {
            graph: StableDiGraph::new(),
            pipe_ids: Vec::new(),
            ready_nodes: Arc::new(Mutex::new(vec![])),
        }
    }

    fn add_processor(&mut self, processor: Arc<dyn Processor>) -> NodeIndex {
        self.graph.add_node(processor)
    }

    fn connect_processors(&mut self, from: NodeIndex, to: NodeIndex) {
        self.graph.add_edge(from, to, 0);
    }

    pub fn add_source(&mut self, processor: Arc<dyn Processor>) {
        // source processor should be the first processors in the pipeline
        assert!(self.pipe_ids.len() <= 1);

        // source processor is always ready to execute
        // this is where we start to execute the pipeline
        processor
            .processor_context()
            .set_processor_state(ProcessorState::Ready);
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
            let processor = f();
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
        let merge_processor = Arc::new(MergeProcessor::new("merge_processor"));
        let merge_processor_index = self.add_processor(merge_processor);
        for index in last_ids {
            self.connect_processors(index, merge_processor_index);
        }
    }

    pub fn expand_processor() {
        todo!()
    }

    pub fn execute(&self) -> Result<()> {
        // traverse the graph and execute the processors
        // if the node state is Ready, execute it
        // if the node state is Running, ignore it
        // if all node state are stopped, stop the pipeline
        loop {
            // get the tasks that are ready to execute, we could avoid the scan of the graph
            let ready_nodes = self.ready_nodes.lock().unwrap();
            for node in ready_nodes.iter() {
                let processor = self.graph.node_weight(*node).unwrap();
                processor.execute();
            }

            // or we need to traverse the graph to find the ready nodes
            // FIXME: we should find a more efficient way to find the ready nodes
            for node in self.graph.node_indices() {
                let processor = self.graph.node_weight(node).unwrap();
                if processor.processor_context().get_processor_state() == ProcessorState::Ready {
                    // TODO(veeupup): run the processor in a thread pool
                    processor.execute()?;
                }
                break;
            }

            // Now, all the ready nodes are stopped and we should stop the pipeline
            break;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use petgraph::dot::Dot;

    use super::Pipeline;
    use crate::processor::EmptyProcessor;

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

        println!("{}", Dot::new(&pipeline.graph));
    }
}
