use crate::ingest::decoder::EngineEvent;
use std::collections::VecDeque;

#[derive(Default)]
pub struct EventBuffer {
    queue: VecDeque<EngineEvent>,
}

impl EventBuffer {
    pub fn push(&mut self, event: EngineEvent) {
        self.queue.push_back(event);
    }

    pub fn pop(&mut self) -> Option<EngineEvent> {
        self.queue.pop_front()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}
