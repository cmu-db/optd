use std::{collections::HashMap, sync::Arc};

use datafusion::{execution::SessionState, logical_expr::TableSource};

pub mod from_optd;
pub mod into_optd;

/// A context for converting between optd and datafusion.
/// The map is used to lookup table sources when converting TableScan operators from optd to datafusion.
pub(crate) struct OptdDFContext<'a> {
    /// Maps table names to table sources.
    pub tables: HashMap<String, Arc<dyn TableSource>>,
    pub session_state: &'a SessionState,
}

impl OptdDFContext<'_> {
    /// Creates a new `OptdDFContext` with the provided session state.
    ///
    /// # Arguments
    ///
    /// * `session_state` - A reference to the `SessionState` used for conversions.
    ///
    /// # Returns
    ///
    /// A `OptdDFContext` containing an empty table map and the provided session state.
    pub(crate) fn new(session_state: &SessionState) -> OptdDFContext {
        OptdDFContext {
            tables: HashMap::new(),
            session_state,
        }
    }
}
