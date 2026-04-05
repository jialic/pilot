use std::future::Future;
use std::pin::Pin;

/// How nodes interact with the user.
/// Implement this for your frontend (TUI, CLI, API, etc.).
pub trait UserIO: Send + Sync {
    fn ask(
        &self,
        question: String,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + '_>>;
}
