use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::error::ProduceError;
use super::transport::Transport;

/// A computation node. Configuration-stateless: holds immutable dependencies
/// wired at construction, no mutable state between calls.
///
/// Separate handlers per request type. Transport dispatches.
///
/// All dependencies (listener, blob store, LLM client, etc.) are
/// constructor arguments, not shared context.
pub trait Produces: Send + Sync {
    /// Handle schema request. Compute output schema, write SCHEMA_RES to output.
    fn handle_schema<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>>;

    /// Handle data request. Process input, write DATA chunks to output.
    fn handle_data<'a>(
        &'a self,
        output: Arc<dyn Transport>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>>;

    /// Wire input transport. Called once during pipeline construction.
    fn set_input(&self, transport: Arc<dyn Transport>);
}
