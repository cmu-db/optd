use std::{collections::HashMap, sync::Arc};

use datafusion::{execution::SessionState, logical_expr::TableSource};

pub mod from_optd;
pub mod into_optd;

pub struct ConversionContext<'a> {
    pub tables: HashMap<String, Arc<dyn TableSource>>,
    pub session_state: &'a SessionState,
}

impl ConversionContext<'_> {
    pub fn new(session_state: &SessionState) -> ConversionContext {
        ConversionContext {
            tables: HashMap::new(),
            session_state,
        }
    }
}
