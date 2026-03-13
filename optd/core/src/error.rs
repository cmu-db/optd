use snafu::prelude::*;

pub use snafu::whatever;

use crate::ir::schema::SchemaError;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(whatever, display("{message}"))]
    Whatever {
        /// The error message.
        message: String,
        /// The underlying error.
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[snafu(visibility(pub(crate)))]
    #[snafu(display("Schema error: {}", source))]
    Schema { source: SchemaError },
}

pub type Result<T> = core::result::Result<T, Error>;
