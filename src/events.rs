/// Workflow-level lifecycle events.
pub trait RunnerEvents: Send + Sync {
    fn workflow_done(&self);
    fn workflow_error(&self, error: &str);
}

/// Default no-op implementation.
pub struct NoopEvents;

impl RunnerEvents for NoopEvents {
    fn workflow_done(&self) {}
    fn workflow_error(&self, _: &str) {}
}

/// Events emitted by producers.
pub enum ProducerEvent {
    Started,
    Completed,
    Error,
}

/// Events emitted by tools.
pub enum ToolEvent {
    Called(String),
    Returned,
    Error(String),
    LlmRequest,
    LlmResponse,
}

/// Outbound events — producers and tools push events to observers.
/// TUI, logging, metrics subscribe to these.
pub trait Listener: Send + Sync {
    fn on_producer(&self, _event: &ProducerEvent) {}
    fn on_tool(&self, _event: &ToolEvent) {}
}

/// No-op implementations for wiring.
pub struct NoopListener;
impl Listener for NoopListener {}

/// Prints events to stderr.
pub struct StderrListener;

impl Listener for StderrListener {
    fn on_producer(&self, event: &ProducerEvent) {
        match event {
            ProducerEvent::Started => eprintln!("[step] started"),
            ProducerEvent::Completed => eprintln!("[step] done"),
            ProducerEvent::Error => eprintln!("[step] error"),
        }
    }

    fn on_tool(&self, event: &ToolEvent) {
        match event {
            ToolEvent::Called(name) => eprintln!("  [tool] {name}"),
            ToolEvent::Returned => eprintln!("  [tool] returned"),
            ToolEvent::Error(msg) => eprintln!("  [tool] error: {msg}"),
            ToolEvent::LlmRequest => eprintln!("  [llm] requesting..."),
            ToolEvent::LlmResponse => eprintln!("  [llm] responded"),
        }
    }
}
