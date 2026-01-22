use snafu::prelude::*;

pub use snafu::whatever;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(whatever, display("{message}"))]
    Whatever {
        /// The error message.
        message: String,
        /// The underying error.
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[snafu(display("Connector error: {}", message))]
    Connector { message: String },
}

#[macro_export]
macro_rules! connector_err {
    ($fmt:literal$(, $($arg:expr),* $(,)?)?) => {
        return core::result::Result::Err($crate::error::Error::Connector {
            message: format!($fmt$(, $($arg),*)*),
        });
    };
    ($source:expr, $fmt:literal$(, $($arg:expr),* $(,)?)*) => {
        match $source {
            core::result::Result::Ok(v) => v,
            core::result::Result::Err(e) => {
                return core::result::Result::Err($crate::error::Error::Connector {
                    message: format!($fmt$(, $($arg),*)*),
                });
            }
        }
    };
}

pub type Result<T> = core::result::Result<T, Error>;
