mod catalog;
mod extension;
mod planner;
mod table;

use std::sync::Arc;

pub use catalog::{OptdCatalogProvider, OptdCatalogProviderList, OptdSchemaProvider};
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

/// Extension trait for DataFusion SessionContext to integrate optd catalog
pub trait SessionContextOptdExt {
    /// Creates a new SessionContext with optd catalog provider to enable lazy-loading
    /// of external tables from the persistent catalog.
    ///
    /// # Example
    /// ```ignore
    /// let ctx = SessionContext::with_optd_catalog(catalog_handle);
    /// ctx.sql("SELECT * FROM users").await?;
    /// ```
    fn with_optd_catalog(catalog_handle: optd_catalog::CatalogServiceHandle) -> Self;
}

impl SessionContextOptdExt for datafusion::prelude::SessionContext {
    fn with_optd_catalog(catalog_handle: optd_catalog::CatalogServiceHandle) -> Self {
        use datafusion::execution::SessionStateBuilder;

        // Create a default session state first
        let default_ctx = Self::new();
        let catalog_list = default_ctx.state().catalog_list().clone();

        // Wrap it with OptdCatalogProviderList to enable catalog integration
        let optd_catalog_list = Arc::new(OptdCatalogProviderList::new(
            catalog_list,
            Some(catalog_handle),
        ));

        // Build new state with the wrapped catalog list
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_catalog_list(Arc::clone(&optd_catalog_list) as _)
            .build();

        Self::new_with_state(state)
    }
}
