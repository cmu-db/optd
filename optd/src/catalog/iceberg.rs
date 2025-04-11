use async_trait::async_trait;
use iceberg::{Catalog as IcebergCatalog, NamespaceIdent, TableIdent};
use optd_dsl::Catalog as OptdCatalog;

/// The default namespace for the Iceberg catalog.
///
/// TODO(connor): For now, keep everything in the default namespace for simplicity.
static DEFAULT_NAMESPACE: &str = "default";

/// A wrapper around an arbitrary Iceberg catalog.
#[derive(Debug)]
pub struct OptdIcebergCatalog<C: IcebergCatalog>(C);

impl<C: IcebergCatalog> OptdIcebergCatalog<C> {
    /// Create a new catalog.
    pub fn new(catalog: C) -> Self {
        Self(catalog)
    }
}

#[async_trait]
impl<C: IcebergCatalog> OptdCatalog for OptdIcebergCatalog<C> {
    async fn get_table_columns(&self, table_name: &str) -> Result<Vec<String>, String> {
        let namespace_ident = NamespaceIdent::new(DEFAULT_NAMESPACE.to_string());
        let table_ident = TableIdent::new(namespace_ident, table_name.to_string());

        // TODO(connor): FIX ERROR HANDLING.
        let table = self
            .0
            .load_table(&table_ident)
            .await
            .map_err(|e| e.to_string())?;

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
