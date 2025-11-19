mod catalog;
mod extension;
mod planner;
mod table;

use std::sync::Arc;

pub use catalog::{OptdCatalogProviderList, OptdSchemaProvider};
pub use extension::{OptdExtension, OptdExtensionConfig};
pub use planner::OptdQueryPlanner;
pub use table::{OptdTable, OptdTableProvider};

pub trait SessionStateBuilderOptdExt: Sized {
    fn with_optd_planner(self) -> Self;
}

impl SessionStateBuilderOptdExt for datafusion::execution::SessionStateBuilder {
    fn with_optd_planner(self) -> Self {
        self.with_query_planner(Arc::new(OptdQueryPlanner::default()))
    }
}
