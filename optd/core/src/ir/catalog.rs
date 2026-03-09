//! Definitions for the catalog interface.
//! optd uses the catalog interface to get schema information about tables.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

pub use arrow_schema::Field;
pub use arrow_schema::Schema;
pub use arrow_schema::SchemaRef;
use snafu::ensure;

use crate::error::Error;
use crate::ir::statistics::TableStatistics;
use crate::ir::table_ref::TableRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataSourceId(pub i64);

/// Contains metadata information about a table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableMetadata {
    pub id: DataSourceId,
    pub name: String,
    pub schema: SchemaRef,
    pub stats: Option<TableStatistics>,
}

pub trait Catalog: Send + Sync + 'static {
    /// Creates a table.
    fn try_create_table(
        &self,
        table_name: String,
        schema: SchemaRef,
    ) -> Result<DataSourceId, DataSourceId>;

    /// Creates a table with stats.
    fn try_create_table_with_stats(
        &self,
        table_name: String,
        schema: SchemaRef,
        stats: TableStatistics,
    ) -> Result<DataSourceId, DataSourceId>;

    /// Describes the schema of a table with identifier `table_id`.
    fn describe_table(&self, table_id: DataSourceId) -> TableMetadata;
    /// Describes the schema of a table with name `table_name`.
    fn try_describe_table_with_name(&self, table_name: &str) -> anyhow::Result<TableMetadata>;

    /// TODO(yuchen): This is a mock.
    fn set_table_stats(&self, table_id: DataSourceId, stats: TableStatistics);
}
