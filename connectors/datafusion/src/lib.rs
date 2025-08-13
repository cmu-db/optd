mod catalog;
mod extension;
mod planner;

use std::sync::Arc;

pub use extension::OptdExtension;
pub use planner::OptdQueryPlanner;

pub trait SessionStateBuilderOptdExt: Sized {
    fn with_optd_planner(self) -> Self;
}

impl SessionStateBuilderOptdExt for datafusion::execution::SessionStateBuilder {
    fn with_optd_planner(self) -> Self {
        self.with_query_planner(Arc::new(OptdQueryPlanner))
    }
}
