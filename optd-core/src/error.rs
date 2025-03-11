use crate::engine::error::EngineError;

/// all optd-core errors, defined in there respective modules,
/// but everyone uses this common Error type for simplicity.
#[derive(Clone, Debug)]
pub(crate) enum Error {
    Engine(EngineError),
}
