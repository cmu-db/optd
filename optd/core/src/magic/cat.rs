use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{Arc, RwLock},
};

use tracing::info;

use crate::error::{Result as OptdResult, whatever};
use crate::ir::{catalog::*, statistics::TableStatistics};

pub struct MagicCatalog(RwLock<MagicCatalogInner>);

pub struct MagicCatalogInner {
    tables: HashMap<DataSourceId, TableMetadata>,
    name_to_id: HashMap<String, DataSourceId>,
    next_table_id: i64,
}

impl Default for MagicCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MagicCatalog {
    pub fn new() -> Self {
        Self(RwLock::new(MagicCatalogInner {
            tables: HashMap::new(),
            name_to_id: HashMap::new(),
            next_table_id: 1,
        }))
    }
}

impl Catalog for MagicCatalog {
    fn try_create_table(
        &self,
        table_name: String,
        schema: Arc<Schema>,
    ) -> Result<DataSourceId, DataSourceId> {
        let mut writer = self.0.write().unwrap();
        let id = DataSourceId(writer.next_table_id);
        match writer.name_to_id.entry(table_name.clone()) {
            Entry::Occupied(occupied) => return Err(*occupied.get()),
            Entry::Vacant(vacant) => vacant.insert(id),
        };
        writer.tables.insert(
            id,
            TableMetadata {
                id,
                name: table_name,
                schema,
                stats: None,
            },
        );
        writer.next_table_id += 1;
        Ok(id)
    }

    fn try_create_table_with_stats(
        &self,
        table_name: String,
        schema: SchemaRef,
        stats: TableStatistics,
    ) -> Result<DataSourceId, DataSourceId> {
        let mut writer = self.0.write().unwrap();
        let id = DataSourceId(writer.next_table_id);
        match writer.name_to_id.entry(table_name.clone()) {
            Entry::Occupied(occupied) => return Err(*occupied.get()),
            Entry::Vacant(vacant) => vacant.insert(id),
        };
        writer.tables.insert(
            id,
            TableMetadata {
                id,
                name: table_name,
                schema,
                stats: Some(stats),
            },
        );
        writer.next_table_id += 1;
        Ok(id)
    }

    fn describe_table(&self, table_id: DataSourceId) -> TableMetadata {
        info!(?table_id, "describe");
        let reader = self.0.read().unwrap();
        reader.tables.get(&table_id).cloned().unwrap()
    }

    fn try_describe_table_with_name(&self, table_name: &str) -> OptdResult<TableMetadata> {
        let reader = self.0.read().unwrap();
        let Some(table_id) = reader.name_to_id.get(table_name) else {
            whatever!("Table {} not found", table_name);
        };
        Ok(reader.tables.get(table_id).cloned().unwrap())
    }

    fn set_table_stats(&self, table_id: DataSourceId, stats: TableStatistics) {
        let mut writer = self.0.write().unwrap();
        let table = writer.tables.get_mut(&table_id).unwrap();
        table.stats = Some(stats);
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
        let cat = MagicCatalog::new();
        let schema = Arc::new(mock_table_schema("t1"));
        let t1 = cat
            .try_create_table("t1".to_string(), schema.clone())
            .unwrap();
        let another_t1 = cat.try_create_table("t1".to_string(), schema.clone());
        assert!(another_t1.is_err());

        let output = cat.describe_table(t1);
        assert_eq!(output.schema, schema);
    }

    #[test]
    fn describe_table_with_name() {
        let cat = MagicCatalog::new();
        let schema = Arc::new(mock_table_schema("t1"));
        cat.try_create_table("t1".to_string(), schema.clone()).unwrap();

        let output = cat.try_describe_table_with_name("t1").unwrap();
        assert_eq!(output.schema, schema);
        assert_eq!(output.name, "t1");
    }

    #[test]
    fn describe_missing_table_with_name() {
        let cat = MagicCatalog::new();

        let err = cat.try_describe_table_with_name("missing").unwrap_err();
        assert!(err.to_string().contains("Table missing not found"));
    }
}
