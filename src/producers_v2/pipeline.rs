use std::sync::Arc;

use crate::dag_v2::{BufferTransport, Produces};

/// A chain of producers wired with transports.
pub struct Pipeline {
    pub producers: Vec<Arc<dyn Produces>>,
}

impl Pipeline {
    /// Create from pre-wired producers (used by builder).
    pub fn new(producers: Vec<Arc<dyn Produces>>) -> Self {
        Self { producers }
    }

    /// Create and wire producers with transports.
    pub fn from_producers(producers: Vec<Arc<dyn Produces>>) -> Self {
        for i in 1..producers.len() {
            let transport = BufferTransport::new();
            transport.set_source(producers[i - 1].clone());
            producers[i].set_input(transport);
        }
        Self { producers }
    }

    pub fn head(&self) -> &Arc<dyn Produces> {
        &self.producers[0]
    }

    pub fn tail(&self) -> &Arc<dyn Produces> {
        self.producers.last().unwrap()
    }
}
