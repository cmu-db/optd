use crate::storage::memo::SqliteMemo;
use iceberg::{spec::Schema, Catalog, Result, TableIdent};
use std::sync::Arc;

#[derive(Debug)]
pub struct OptdCatalog<C> {
    // TODO(connor): Do we even need this if `SqliteMemo` is going to implement `Catalog`?
    _memo: Arc<SqliteMemo>,
    catalog: C,
}

impl<C: Catalog> OptdCatalog<C> {
    pub fn new(memo: Arc<SqliteMemo>, catalog: C) -> Self {
        Self {
            _memo: memo,
            catalog,
        }
    }

    pub fn catalog(&self) -> &C {
        &self.catalog
    }

    pub async fn get_current_table_schema(&self, table_id: &TableIdent) -> Result<Arc<Schema>> {
        let table = self.catalog.load_table(table_id).await?;
        let table_metadata = table.metadata();

        Ok(table_metadata.current_schema().clone())
    }

    pub async fn num_columns(&self, table_id: &TableIdent) -> Result<usize> {
        let schema = self.get_current_table_schema(table_id).await?;
        let field_ids = schema.identifier_field_ids();

        Ok(field_ids.len())
    }
}
