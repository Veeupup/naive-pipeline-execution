use std::sync::{Arc, Mutex};

use crate::processor::Processor;
use crate::processor::ProcessorState;
use crate::transform::MergeProcessor;
use crate::Result;
use arrow::record_batch::RecordBatch;
use petgraph::stable_graph::{DefaultIx, NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;

pub type Index = NodeIndex<DefaultIx>;

#[derive(Debug)]
pub struct RunningGraph(StableDiGraph<Arc<dyn Processor>, ()>);

impl RunningGraph {
    pub fn new() -> Self {
        RunningGraph(StableDiGraph::new())
    }

    pub fn add_processor(&mut self, processor: Arc<dyn Processor>) -> NodeIndex {
        self.0.add_node(processor)
    }

    pub fn add_edges(&mut self, from: NodeIndex, to: NodeIndex) {
        self.0.add_edge(from, to, ());
    }

    pub fn get_processor_by_index(&self, index: NodeIndex) -> Arc<dyn Processor> {
        self.0[index].clone()
    }

    pub fn get_prev_processors(&self, index: NodeIndex) -> Vec<Arc<dyn Processor>> {
        let mut prev_processors = vec![];
        for edge in self.0.edges_directed(index, petgraph::Direction::Incoming) {
            prev_processors.push(self.0[edge.source()].clone());
        }
        prev_processors
    }

    pub fn get_next_processors(&self, index: NodeIndex) -> Vec<Arc<dyn Processor>> {
        let mut next_processors = vec![];
        for edge in self.0.edges_directed(index, petgraph::Direction::Outgoing) {
            next_processors.push(self.0[edge.target()].clone());
        }
        next_processors
    }

    pub fn get_all_processors(&self) -> Vec<Arc<dyn Processor>> {
        self.0.node_weights().map(|p| p.clone()).collect()
    }

    // get the output processor of the pipeline
    pub fn get_last_processor(&self) -> Arc<dyn Processor> {
        let mut last_processor = None;
        for node in self.0.node_indices() {
            if self
                .0
                .edges_directed(node, petgraph::Direction::Outgoing)
                .count()
                == 0
            {
                last_processor = Some(self.0[node].clone());
            }
        }
        last_processor.unwrap()
    }
}
