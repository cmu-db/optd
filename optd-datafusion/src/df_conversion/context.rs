use datafusion::catalog::TableProvider;
use datafusion::execution::SessionState;
use std::fmt::Debug;
use std::{collections::HashMap, sync::Arc};

/// A context for converting plans and expressions between `optd` and DataFusion.
pub(crate) struct OptdDFContext {
    /// Maps table names to DataFusion [`TableProvider`]s.
    pub(crate) providers: HashMap<String, Arc<dyn TableProvider>>,
    /// DataFusion session state.
    pub(crate) session_state: SessionState,
}

impl OptdDFContext {
    /// Creates a new empty `OptdDataFusionContext` with the provided session state.
    pub(crate) fn new(session_state: &SessionState) -> OptdDFContext {
        OptdDFContext {
            providers: HashMap::new(),
            session_state: session_state.clone(),
        }
    }
}

impl Debug for OptdDFContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptdContext")
            .field("tables", &self.providers.keys())
            .field("session_state", &self.session_state)
            .finish()
    }
}
