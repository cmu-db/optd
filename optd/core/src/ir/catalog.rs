use std::{collections::HashMap, sync::Arc};

use crate::ir::{data_type::DataType, statistics::ValueHistogram};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataSourceId(pub i64);

/// Describes schema information about a column.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// The name of the column, e.g. `my_column`.
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub histogram: Option<ValueHistogram>,
}

impl Field {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Field {
            name,
            data_type,
            nullable,
            histogram: None,
        }
    }
}

impl<S> From<(S, DataType, bool)> for Field
where
    S: Into<String>,
{
    fn from(value: (S, DataType, bool)) -> Self {
        let (name, data_type, nullable) = value;

        Field::new(name.into(), data_type, nullable)
    }
}

/// Describe schema information for a relation.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    /// Description of the columns.
    columns: Arc<[Arc<Field>]>,
}

impl Schema {
    pub fn new(columns: Vec<Arc<Field>>) -> Self {
        Schema {
            columns: columns.into(),
        }
    }

    pub fn columns(&self) -> &[Arc<Field>] {
        &self.columns
    }
}

impl<S> FromIterator<S> for Schema
where
    S: Into<Field>,
{
    fn from_iter<T: IntoIterator<Item = S>>(columns: T) -> Self {
        let columns = columns.into_iter().map(|x| Arc::new(x.into())).collect();
        Self::new(columns)
    }
}

impl FromIterator<Arc<Field>> for Schema {
    fn from_iter<T: IntoIterator<Item = Arc<Field>>>(iter: T) -> Self {
        let columns = iter.into_iter().collect();
        Self::new(columns)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableMetadata {
    pub id: DataSourceId,
    pub name: String,
    pub schema: Arc<Schema>,
    pub histograms: HashMap<String, ValueHistogram>,
    pub row_count: usize,
}

pub trait Catalog: Send + Sync + 'static {
    /// Creates a table.
    fn try_create_table(
        &self,
        table_name: String,
        schema: Arc<Schema>,
    ) -> Result<DataSourceId, DataSourceId>;
    /// Describes the schema of a table with identifier `table_id`.
    fn describe_table(&self, table_id: DataSourceId) -> TableMetadata;
    /// Describes the schema of a table with name `table_name`.
    fn try_describe_table_with_name(&self, table_name: &str) -> anyhow::Result<TableMetadata>;

    /// TODO(yuchen): This is a mock.
    fn set_table_row_count(&self, table_id: DataSourceId, row_count: usize);
}
