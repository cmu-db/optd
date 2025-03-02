use datafusion::{execution::SessionState, logical_expr::TableSource};
use std::fmt::Debug;
use std::{collections::HashMap, sync::Arc};

pub mod from_optd;
pub mod into_optd;

/// A context for converting between optd and datafusion.
/// The map is used to lookup table sources when converting TableScan operators from optd to
/// datafusion.
pub(crate) struct OptdContext {
    /// Maps table names to table sources.
    tables: HashMap<String, Arc<dyn TableSource>>,
    /// DataFusion session state.
    session_state: SessionState,
}

impl OptdContext {
    /// Creates a new empty `OptdDataFusionContext` with the provided session state.
    pub(crate) fn new(session_state: &SessionState) -> OptdContext {
        OptdContext {
            tables: HashMap::new(),
            session_state: session_state.clone(),
        }
    }
}

impl Debug for OptdContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptdContext")
            .field("tables", &self.tables.keys())
            .field("session_state", &self.session_state)
            .finish()
    }
}
