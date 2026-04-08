use std::sync::Arc;

use datafusion::common::{config::ConfigExtension, extensions_options};
use optd_core::{ir::catalog::Catalog, magic::MemoryCatalog};

extensions_options! {
   /// optd configuration in datafusion.
   pub struct OptdExtensionConfig {
       /// Should try run optd optimizer instead of datafusion default.
       pub optd_enabled: bool, default = true
       /// Should fail on any unsupported features.
       pub optd_strict_mode: bool, default = false
       /// Disable DataFusion optimizers and run optd optimization only.
       pub optd_only: bool, default = false
       /// Use the advanced cardinality estimator instead of the magic estimator.
       pub optd_use_advanced_cardinality: bool, default = false
   }
}

impl ConfigExtension for OptdExtensionConfig {
    const PREFIX: &'static str = "optd";
}

/// The optd datafusion extension used to store shared state.
pub struct OptdExtension {
    catalog: Arc<dyn Catalog>,
}

impl OptdExtension {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }
}

impl Default for OptdExtension {
    fn default() -> Self {
        Self::new(Arc::new(MemoryCatalog::new("datafusion", "public")))
    }
}

impl std::fmt::Debug for OptdExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptdExtension").finish_non_exhaustive()
    }
}
