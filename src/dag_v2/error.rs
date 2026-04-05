use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProduceError {
    #[error("{0}")]
    Failed(Box<dyn std::error::Error + Send + Sync>),
}

impl ProduceError {
    pub fn msg(msg: impl Into<String>) -> Self {
        Self::Failed(msg.into().into())
    }
}
