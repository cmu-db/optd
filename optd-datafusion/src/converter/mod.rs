use datafusion::{execution::SessionState, logical_expr::TableSource};
use std::{collections::HashMap, sync::Arc};

pub mod from_optd;
pub mod into_optd;

/// A context for converting between optd and datafusion.
/// The map is used to lookup table sources when converting TableScan operators from optd to datafusion.
pub(crate) struct OptdDataFusionContext<'a> {
    /// Maps table names to table sources.
    pub tables: HashMap<String, Arc<dyn TableSource>>,
    /// DataFusion session state.
    pub session_state: &'a SessionState,
}

impl OptdDataFusionContext<'_> {
    /// Creates a new empty `OptdDataFusionContext` with the provided session state.
    pub(crate) fn new(session_state: &SessionState) -> OptdDataFusionContext {
        OptdDataFusionContext {
            tables: HashMap::new(),
            session_state,
        }
    }
}
