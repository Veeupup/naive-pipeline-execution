// use crate::processor::*;
// use crate::Result;
// use std::{sync::{Arc, Mutex}, fmt::Display};

// #[derive(Debug)]
// pub struct MemorySource {
//     name: &'static str,
//     processor_type: ProcessorType,
//     processor_state: Mutex<ProcessorState>,
//     prev_processor: Option<Arc<dyn Processor>>,
// }

// impl Display for MemorySource {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "MemorySource [name: {}]", self.name)
//     }
// }

// impl MemorySource {
//     pub fn new(name: &'static str) -> Self {
//         MemorySource {
//             name,
//             processor_type: ProcessorType::Transform,
//             processor_state: Mutex::new(ProcessorState::Ready),
//             prev_processor: None,
//         }
//     }
// }

// impl Processor for MemorySource {
//     fn name(&self) -> &'static str {
//         self.name
//     }

//     fn connect_from_input(&mut self, input: Arc<dyn Processor>) {
//         self.prev_processor = Some(input);
//     }

//     fn execute(&self) -> Result<()> {
//         Ok(())
//     }

//     fn processor_type(&self) -> ProcessorType {
//         self.processor_type
//     }

//     fn processor_state(&self) -> ProcessorState {
//         self.processor_state.lock().unwrap().clone()
//     }

//     fn set_processor_state(&self, state: ProcessorState) {
//         *self.processor_state.lock().unwrap() = state;
//     }

//     fn prev_processor(&self) -> Option<Arc<dyn Processor>> {
//         self.prev_processor.clone()
//     }
// }
