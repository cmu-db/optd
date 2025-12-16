use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider},
    common::DataFusionError,
    error::Result,
    prelude::SessionContext,
};
use optd_catalog::{CatalogServiceHandle, ExternalTableMetadata};
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

#[derive(Debug, Clone)]
pub struct OptdCatalogProvider {
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

    pub fn catalog_handle(&self) -> Option<&CatalogServiceHandle> {
        self.catalog_handle.as_ref()
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
        self.inner.schema(name).map(|schema| {
            Arc::new(OptdSchemaProvider::new(schema, self.catalog_handle.clone()))
                as Arc<dyn SchemaProvider>
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

    /// Creates a `TableProvider` from external table metadata.
    ///
    /// Reconstructs the appropriate DataFusion table based on file format (CSV, Parquet, JSON).
    /// DataFusion uses lazy schema inference - schemas are populated during query execution.
    async fn create_table_from_metadata(
        &self,
        metadata: &ExternalTableMetadata,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let temp_ctx = SessionContext::new();
        match metadata.file_format.to_uppercase().as_str() {
            "CSV" => {
                temp_ctx
                    .register_csv("temp_table", &metadata.location, Default::default())
                    .await?;
            }
            "PARQUET" => {
                temp_ctx
                    .register_parquet("temp_table", &metadata.location, Default::default())
                    .await?;
            }
            "JSON" | "NDJSON" => {
                temp_ctx
                    .register_json("temp_table", &metadata.location, Default::default())
                    .await?;
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported file format: {}. Supported formats: PARQUET, CSV, JSON",
                    metadata.file_format
                )));
            }
        }

        let _ = temp_ctx.sql("SELECT * FROM temp_table LIMIT 0").await?;
        let catalog = temp_ctx
            .catalog("datafusion")
            .ok_or_else(|| DataFusionError::Plan("Default catalog not found".to_string()))?;
        let schema = catalog
            .schema("public")
            .ok_or_else(|| DataFusionError::Plan("Default schema not found".to_string()))?;
        let table = schema.table("temp_table").await?.ok_or_else(|| {
            DataFusionError::Plan("Table not found after registration".to_string())
        })?;

        Ok(table)
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
            let optd_table = Arc::new(OptdTableProvider::new(table, name.to_string()));
            return Ok(Some(optd_table as Arc<dyn TableProvider>));
        }

        if let Some(catalog_handle) = &self.catalog_handle
            && let Some(metadata) = catalog_handle
                .get_external_table(None, name)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            let table_provider = self.create_table_from_metadata(&metadata).await?;

            self.inner
                .register_table(name.to_string(), table_provider.clone())?;

            let optd_table = Arc::new(OptdTableProvider::new(table_provider, name.to_string()));
            return Ok(Some(optd_table as Arc<dyn TableProvider>));
        }

        Ok(None)
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
