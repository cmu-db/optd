use crate::engine::error::EngineError;

/// all optd-core errors, defined in there respective modules,
/// but everyone uses this common Error type for simplicity.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum Error {
    Engine(EngineError),
}
