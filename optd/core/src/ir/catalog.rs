use std::sync::Arc;

use crate::ir::data_type::DataType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataSourceId(pub i64);

/// Describes schema information about a column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDescription {
    /// The name of the column, e.g. `my_column`.
    pub name: String,
    pub data_type: DataType,
}

impl ColumnDescription {
    pub fn new(name: String, data_type: DataType) -> Self {
        ColumnDescription { name, data_type }
    }
}

impl<S> From<(S, DataType)> for ColumnDescription
where
    S: Into<String>,
{
    fn from(value: (S, DataType)) -> Self {
        let (name, data_type) = value;

        ColumnDescription::new(name.into(), data_type)
    }
}

/// Describe schema information for a relation.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaDescription {
    /// Description of the columns.
    columns: Vec<Arc<ColumnDescription>>,
}

impl SchemaDescription {
    pub fn new(columns: Vec<Arc<ColumnDescription>>) -> Self {
        SchemaDescription { columns }
    }

    pub fn columns(&self) -> &[Arc<ColumnDescription>] {
        &self.columns
    }

    pub fn extend<I>(&mut self, columns: I)
    where
        I: IntoIterator<Item = Arc<ColumnDescription>>,
    {
        self.columns.extend(columns);
    }
}

impl<S> FromIterator<S> for SchemaDescription
where
    S: Into<ColumnDescription>,
{
    fn from_iter<T: IntoIterator<Item = S>>(columns: T) -> Self {
        let columns = columns.into_iter().map(|x| Arc::new(x.into())).collect();
        Self::new(columns)
    }
}

impl FromIterator<Arc<ColumnDescription>> for SchemaDescription {
    fn from_iter<T: IntoIterator<Item = Arc<ColumnDescription>>>(iter: T) -> Self {
        let columns = iter.into_iter().collect();
        Self::new(columns)
    }
}

impl IntoIterator for SchemaDescription {
    type Item = Arc<ColumnDescription>;

    type IntoIter = std::vec::IntoIter<Arc<ColumnDescription>>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.into_iter()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableMetadata {
    pub id: DataSourceId,
    pub schema: Arc<SchemaDescription>,
    pub row_count: usize,
}

pub trait Catalog: Send + Sync + 'static {
    /// Creates a table.
    fn try_create_table(
        &mut self,
        table_name: String,
        schema: Arc<SchemaDescription>,
    ) -> anyhow::Result<DataSourceId>;
    /// Describes the schema of a table with identifier `table_id`.
    fn describe_table(&self, table_id: DataSourceId) -> &TableMetadata;
    /// Describes the schema of a table with name `table_name`.
    fn try_describe_table_with_name(&self, table_name: &str) -> anyhow::Result<&TableMetadata>;

    /// TODO(yuchen): This is a mock.
    fn set_table_row_count(&mut self, table_id: DataSourceId, row_count: usize);
}
