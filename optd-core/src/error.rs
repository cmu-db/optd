use futures::channel::{mpsc, oneshot::Canceled};

/// all optd-core errors, defined in there respective modules,
/// but everyone uses this common Error type for simplicity.
#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    Placeholder,
    Channel(Box<dyn std::error::Error + Send + Sync>),
}

impl From<mpsc::SendError> for Error {
    fn from(value: mpsc::SendError) -> Self {
        Error::Channel(Box::new(value))
    }
}

impl From<Canceled> for Error {
    fn from(value: Canceled) -> Self {
        Error::Channel(Box::new(value))
    }
}
