use crate::{engine::error::EngineError, storage::error::StorageError};

/// all optd-core errors, defined in there respective modules,
/// but everyone uses this common Error type for simplicity.
#[derive(Clone, Debug)]
pub enum Error {
    Engine(EngineError),
    Storage(StorageError),
}
