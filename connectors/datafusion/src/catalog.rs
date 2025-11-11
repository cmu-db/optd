use datafusion::{
    catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider},
    error::Result,
    execution::SessionState,
    common::DataFusionError
};
use parking_lot::RwLock;
use std::any::Any;
use std::sync::{Arc, Weak};
use async_trait::async_trait;

#[derive(Debug)]
pub struct OptdCatalogProviderList {
    inner: Arc<dyn CatalogProviderList>,
    state: Weak<RwLock<SessionState>>,
}

impl OptdCatalogProviderList {
    pub fn new(inner: Arc<dyn CatalogProviderList>, state: Weak<RwLock<SessionState>>) -> Self {
        Self { inner, state }
    }

    // pub fn new_from_location(path: &str) -> Result<Self> {
    //     let url = url::Url::parse(path)?;
    //     let state = Arc::downgrade(&SessionState::new());
    //     let inner = Arc::new();
    //     Ok(Self { inner, state })
    // }
}

impl CatalogProviderList for OptdCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.inner.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let state = self.state.clone();
        self.inner
            .catalog(name)
            .map(|catalog| Arc::new(OptdCatalogProvider::new(catalog, state)) as _)
    }
}

#[derive(Debug)]
struct OptdCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    state: Weak<RwLock<SessionState>>,
}

impl OptdCatalogProvider {
    pub fn new(inner: Arc<dyn CatalogProvider>, state: Weak<RwLock<SessionState>>) -> Self {
        Self { inner, state }
    }
}

impl CatalogProvider for OptdCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let state = self.state.clone();
        self.inner
            .schema(name)
            .map(|schema| Arc::new(OptdSchemaProvider::new(schema, state)) as _)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }
}

#[derive(Debug)]
pub struct OptdSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    state: Weak<RwLock<SessionState>>,
}

impl OptdSchemaProvider {
    pub fn new(inner: Arc<dyn SchemaProvider>, state: Weak<RwLock<SessionState>>) -> Self {
        Self { inner, state }
    }
}

#[async_trait]
impl SchemaProvider for OptdSchemaProvider {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        return self.inner.table(name).await;
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_getters() {
        // let catalog = OptdCatalogProviderList::new();
        // assert_eq!(catalog.catalog_names(), Vec::<String>::new());
    }
}