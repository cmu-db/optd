use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{Arc, RwLock},
};

use tracing::info;

use crate::ir::{
    catalog::*,
    statistics::TableStatistics,
    table_ref::{ResolvedTableRef, TableRef},
};

pub struct MagicCatalog {
    inner: RwLock<MagicCatalogInner>,
    default_catalog: String,
    default_schema: String,
}

pub struct MagicCatalogInner {
    tables: HashMap<DataSourceId, TableMetadata>,
    table_to_id: HashMap<ResolvedTableRef, DataSourceId>,
    next_table_id: i64,
}

impl MagicCatalog {
    pub fn new(default_catalog: &str, default_schema: &str) -> Self {
        Self {
            inner: RwLock::new(MagicCatalogInner {
                tables: HashMap::new(),
                table_to_id: HashMap::new(),
                next_table_id: 1,
            }),
            default_catalog: default_catalog.into(),
            default_schema: default_schema.into(),
        }
    }

    fn resolve_table_ref(&self, table: TableRef) -> ResolvedTableRef {
        table.resolve(&self.default_catalog, &self.default_schema)
    }
}

impl Catalog for MagicCatalog {
    fn create_table(&self, table: TableRef, schema: Arc<Schema>) -> Result<DataSourceId> {
        let mut writer = self.inner.write().unwrap();
        let id = DataSourceId(writer.next_table_id);
        let table = self.resolve_table_ref(table);
        match writer.table_to_id.entry(table.clone()) {
            Entry::Occupied(occupied) => {
                return Err(CatalogError::TableAlreadyExists {
                    table,
                    existing_id: *occupied.get(),
                });
            }
            Entry::Vacant(vacant) => vacant.insert(id),
        };
        writer.tables.insert(
            id,
            TableMetadata {
                id,
                table,
                schema,
                stats: None,
            },
        );
        writer.next_table_id += 1;
        Ok(id)
    }

    fn create_table_with_stats(
        &self,
        table: TableRef,
        schema: SchemaRef,
        stats: TableStatistics,
    ) -> Result<DataSourceId> {
        let mut writer = self.inner.write().unwrap();
        let id = DataSourceId(writer.next_table_id);
        let table = self.resolve_table_ref(table);
        match writer.table_to_id.entry(table.clone()) {
            Entry::Occupied(occupied) => {
                return Err(CatalogError::TableAlreadyExists {
                    table,
                    existing_id: *occupied.get(),
                });
            }
            Entry::Vacant(vacant) => vacant.insert(id),
        };
        writer.tables.insert(
            id,
            TableMetadata {
                id,
                table,
                schema,
                stats: Some(stats),
            },
        );
        writer.next_table_id += 1;
        Ok(id)
    }

    fn table(&self, table_id: DataSourceId) -> Result<TableMetadata> {
        info!(?table_id, "describe");
        let reader = self.inner.read().unwrap();
        reader
            .tables
            .get(&table_id)
            .cloned()
            .ok_or(CatalogError::DataSourceNotFound {
                data_source_id: table_id,
            })
    }

    fn table_by_ref(&self, table: &TableRef) -> Result<TableMetadata> {
        let reader = self.inner.read().unwrap();
        let resolved = table
            .clone()
            .resolve(&self.default_catalog, &self.default_schema);
        let Some(table_id) = reader.table_to_id.get(&resolved) else {
            return Err(CatalogError::TableNotFound {
                table: table.clone(),
            });
        };
        reader
            .tables
            .get(table_id)
            .cloned()
            .ok_or(CatalogError::DanglingTableReference {
                table: resolved,
                data_source_id: *table_id,
            })
    }

    fn drop_table(&self, table: TableRef) -> Result<()> {
        let mut writer = self.inner.write().unwrap();
        let resolved = self.resolve_table_ref(table.clone());
        let Some(table_id) = writer.table_to_id.get(&resolved).copied() else {
            return Err(CatalogError::TableNotFound { table });
        };

        writer
            .tables
            .remove(&table_id)
            .ok_or(CatalogError::DanglingTableReference {
                table: resolved.clone(),
                data_source_id: table_id,
            })?;
        writer.table_to_id.remove(&resolved);
        Ok(())
    }

    fn set_table_stats(&self, table_id: DataSourceId, stats: TableStatistics) -> Result<()> {
        let mut writer = self.inner.write().unwrap();
        let table = writer
            .tables
            .get_mut(&table_id)
            .ok_or(CatalogError::DataSourceNotFound {
                data_source_id: table_id,
            })?;
        table.stats = Some(stats);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::DataType;

    use super::*;

    fn mock_table_schema(name: &str) -> Schema {
        Schema::new(vec![
            Field::new(format!("{name}.v1"), DataType::Int32, false),
            Field::new(format!("{name}.v2"), DataType::Int32, false),
            Field::new(format!("{name}.v3"), DataType::Boolean, false),
        ])
    }

    #[test]
    fn create_table() {
        let cat = MagicCatalog::new("optd", "public");
        let schema = Arc::new(mock_table_schema("t1"));
        let t1 = cat
            .create_table(TableRef::bare("t1"), schema.clone())
            .unwrap();
        let another_t1 = cat.create_table(TableRef::bare("t1"), schema.clone());
        assert!(another_t1.is_err());

        let output = cat.table(t1).unwrap();
        assert_eq!(output.schema, schema);
    }

    #[test]
    fn describe_table_with_name() {
        let cat = MagicCatalog::new("optd", "public");
        let schema = Arc::new(mock_table_schema("t1"));
        cat.create_table(TableRef::bare("t1"), schema.clone())
            .unwrap();

        let output = cat.table_by_ref(&TableRef::bare("t1")).unwrap();
        assert_eq!(output.schema, schema);
        assert_eq!(output.table.table.as_ref(), "t1");
    }

    #[test]
    fn describe_missing_table_with_name() {
        let cat = MagicCatalog::new("optd", "public");

        let err = cat.table_by_ref(&TableRef::bare("missing")).unwrap_err();
        assert!(err.to_string().contains("Table 'missing' not found"));
    }

    #[test]
    fn drop_table_removes_lookup_and_allows_recreate() {
        let cat = MagicCatalog::new("optd", "public");
        let schema = Arc::new(mock_table_schema("t1"));
        let first_id = cat
            .create_table(TableRef::bare("t1"), schema.clone())
            .unwrap();

        cat.drop_table(TableRef::bare("t1")).unwrap();

        assert!(matches!(
            cat.table(first_id),
            Err(CatalogError::DataSourceNotFound { data_source_id }) if data_source_id == first_id
        ));
        assert!(matches!(
            cat.table_by_ref(&TableRef::bare("t1")),
            Err(CatalogError::TableNotFound { .. })
        ));

        let second_id = cat.create_table(TableRef::bare("t1"), schema).unwrap();
        assert_ne!(first_id, second_id);
    }

    #[test]
    fn drop_missing_table_with_name() {
        let cat = MagicCatalog::new("optd", "public");

        assert!(matches!(
            cat.drop_table(TableRef::bare("missing")),
            Err(CatalogError::TableNotFound { .. })
        ));
    }
}
