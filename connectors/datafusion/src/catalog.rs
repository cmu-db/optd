use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider},
    common::DataFusionError,
    error::Result,
};
use optd_catalog::CatalogServiceHandle;
use std::any::Any;
use std::sync::Arc;

use crate::table::OptdTableProvider;

#[derive(Debug)]
pub struct OptdCatalogProviderList {
    inner: Arc<dyn CatalogProviderList>,
    catalog_handle: Option<CatalogServiceHandle>,
}

impl OptdCatalogProviderList {
    pub fn new(
        inner: Arc<dyn CatalogProviderList>,
        catalog_handle: Option<CatalogServiceHandle>,
    ) -> Self {
        Self {
            inner,
            catalog_handle,
        }
    }
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
        let catalog_handle = self.catalog_handle.clone();
        self.inner.catalog(name).map(|catalog| {
            Arc::new(OptdCatalogProvider::new(catalog, catalog_handle)) as Arc<dyn CatalogProvider>
        })
    }
}

#[derive(Debug)]
struct OptdCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    catalog_handle: Option<CatalogServiceHandle>,
}

impl OptdCatalogProvider {
    pub fn new(
        inner: Arc<dyn CatalogProvider>,
        catalog_handle: Option<CatalogServiceHandle>,
    ) -> Self {
        Self {
            inner,
            catalog_handle,
        }
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
        let catalog_handle = self.catalog_handle.clone();
        self.inner.schema(name).map(|schema| {
            Arc::new(OptdSchemaProvider::new(schema, catalog_handle)) as Arc<dyn SchemaProvider>
        })
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
    catalog_handle: Option<CatalogServiceHandle>,
}

impl OptdSchemaProvider {
    pub fn new(
        inner: Arc<dyn SchemaProvider>,
        catalog_handle: Option<CatalogServiceHandle>,
    ) -> Self {
        Self {
            inner,
            catalog_handle,
        }
    }
}

#[async_trait]
impl SchemaProvider for OptdSchemaProvider {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table_opt = self.inner.table(name).await?;

        if let Some(table) = table_opt {
            let optd_table = Arc::new(OptdTableProvider::new(
                table,
                name.to_string(),
                self.catalog_handle.clone(),
            ));

            Ok(Some(optd_table as Arc<dyn TableProvider>))
        } else {
            Ok(None)
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}
