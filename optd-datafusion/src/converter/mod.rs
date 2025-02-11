use std::{collections::HashMap, sync::Arc};

use datafusion::{execution::SessionState, logical_expr::TableSource};

pub mod from_optd;
pub mod into_optd;

/// A context for converting between optd and datafusion.
/// The map is used to lookup table sources when converting TableScan operators from optd to datafusion.
pub struct ConversionContext<'a> {
    /// Maps table names to table sources.
    pub tables: HashMap<String, Arc<dyn TableSource>>,
    pub session_state: &'a SessionState,
}

impl ConversionContext<'_> {
    /// Creates a new `ConversionContext` with the provided session state.
    ///
    /// # Arguments
    ///
    /// * `session_state` - A reference to the `SessionState` used for conversions.
    ///
    /// # Returns
    ///
    /// A `ConversionContext` containing an empty table map and the provided session state.
    pub fn new(session_state: &SessionState) -> ConversionContext {
        ConversionContext {
            tables: HashMap::new(),
            session_state,
        }
    }
}
