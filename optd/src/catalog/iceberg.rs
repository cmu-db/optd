use super::{Catalog as OptdCatalog, CatalogError};
use async_trait::async_trait;
use iceberg::{Catalog as IcebergCatalog, NamespaceIdent, TableIdent, table::Table};
use std::collections::HashMap;

/// The default namespace for the Iceberg catalog.
///
/// TODO(connor): For now, keep everything in the default namespace for simplicity.
static DEFAULT_NAMESPACE: &str = "default";

/// A wrapper around an arbitrary Iceberg catalog.
#[derive(Debug)]
pub struct OptdIcebergCatalog<C: IcebergCatalog>(C);

impl<C: IcebergCatalog> OptdIcebergCatalog<C> {
    /// Creates a new catalog.
    pub fn new(catalog: C) -> Self {
        Self(catalog)
    }

    /// Retrieves a [`Table`] from the catalog.
    async fn get_table(&self, table_name: &str) -> Result<Table, CatalogError> {
        let namespace_ident = NamespaceIdent::new(DEFAULT_NAMESPACE.to_string());
        let table_ident = TableIdent::new(namespace_ident, table_name.to_string());

        // TODO(connor): FIX ERROR HANDLING.
        self.0
            .load_table(&table_ident)
            .await
            .map_err(|e| CatalogError::Unknown(e.to_string()))
    }
}

#[async_trait]
impl<C: IcebergCatalog> OptdCatalog for OptdIcebergCatalog<C> {
    async fn get_table_properties(
        &self,
        table_name: &str,
    ) -> Result<HashMap<String, String>, CatalogError> {
        let table = self.get_table(table_name).await?;

        Ok(table.metadata().properties().clone())
    }

    async fn get_table_columns(&self, table_name: &str) -> Result<Vec<String>, CatalogError> {
        let table = self.get_table(table_name).await?;

        let metadata = table.metadata();
        let schema = metadata.current_schema();

        Ok(schema
            .identifier_field_ids()
            .map(|id| {
                let field = schema.field_by_id(id).expect("schema id is corrupted");

                // TODO(connor): There is a lot of other information that can be extracted here.
                field.name.clone()
            })
            .collect())
    }
}
