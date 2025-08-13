use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};

use crate::OptdExtension;

#[derive(Debug)]
pub struct OptdCatalog {
    ext: Arc<OptdExtension>,
}

#[derive(Debug)]
pub struct OptdSchema {
    ext: Arc<OptdExtension>,
}

#[derive(Debug)]
pub struct OptdTable {
    ext: Arc<OptdExtension>,
}

impl CatalogProvider for OptdCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self as &dyn std::any::Any
    }

    fn schema_names(&self) -> Vec<String> {
        // self.ext.get_schema_names()
        todo!()
    }

    fn schema(
        &self,
        name: &str,
    ) -> Option<std::sync::Arc<dyn datafusion::catalog::SchemaProvider>> {
        todo!()
    }
}

impl SchemaProvider for OptdSchema {
    fn owner_name(&self) -> Option<&str> {
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn table_names(&self) -> Vec<String> {
        todo!()
    }

    fn table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        name: &'life1 str,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = datafusion::common::Result<
                        Option<Arc<dyn datafusion::catalog::TableProvider>>,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    fn table_exist(&self, name: &str) -> bool {
        todo!()
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
        todo!()
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
        todo!()
    }
}

impl datafusion::catalog::TableProvider for OptdTable {
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    #[doc = " Get a reference to the schema for this table"]
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        todo!()
    }

    #[doc = " Get the type of this table for metadata/catalog purposes."]
    fn table_type(&self) -> datafusion::datasource::TableType {
        todo!()
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn datafusion::catalog::Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = datafusion::common::Result<
                        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }
}
