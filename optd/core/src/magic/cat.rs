use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use anyhow::bail;

use crate::ir::catalog::*;

pub struct MagicCatalog {
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
        MagicCatalog {
            tables: HashMap::new(),
            name_to_id: HashMap::new(),
            next_table_id: 1,
        }
    }
}

impl Catalog for MagicCatalog {
    fn try_create_table(
        &mut self,
        table_name: String,
        schema: Arc<SchemaDescription>,
    ) -> anyhow::Result<DataSourceId> {
        let id = DataSourceId(self.next_table_id);
        match self.name_to_id.entry(table_name) {
            Entry::Occupied(occupied) => {
                bail!(
                    "Table with name {} already exists: {:?}",
                    occupied.key(),
                    occupied.get()
                )
            }
            Entry::Vacant(vacant) => vacant.insert(id),
        };
        self.tables.insert(
            id,
            TableMetadata {
                id,
                schema,
                row_count: 0,
            },
        );
        self.next_table_id += 1;
        Ok(id)
    }

    fn describe_table(&self, table_id: DataSourceId) -> &TableMetadata {
        self.tables.get(&table_id).unwrap()
    }

    fn try_describe_table_with_name(&self, table_name: &str) -> anyhow::Result<&TableMetadata> {
        let Some(table_id) = self.name_to_id.get(table_name) else {
            bail!("Table {} not found", table_name);
        };
        Ok(self.describe_table(*table_id))
    }

    fn set_table_row_count(&mut self, table_id: DataSourceId, row_count: usize) {
        let table = self.tables.get_mut(&table_id).unwrap();
        table.row_count = row_count;
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::DataType;

    use super::*;

    fn mock_table_schema(name: &str) -> SchemaDescription {
        SchemaDescription::from_iter([
            (format!("{name}.v1"), DataType::Int32),
            (format!("{name}.v2"), DataType::Int32),
            (format!("{name}.v3"), DataType::Boolean),
        ])
    }

    #[test]
    fn create_table() -> anyhow::Result<()> {
        let mut cat = MagicCatalog::new();
        let schema = Arc::new(mock_table_schema("t1"));
        let t1 = cat.try_create_table("t1".to_string(), schema.clone())?;
        let another_t1 = cat.try_create_table("t1".to_string(), schema.clone());
        assert!(another_t1.is_err());

        let output = cat.describe_table(t1);
        assert_eq!(output.schema, schema);

        Ok(())
    }
}
