//! Definitions for the catalog interface.
//! optd uses the catalog interface to get schema information about tables.

pub use arrow_schema::Field;
pub use arrow_schema::Schema;
pub use arrow_schema::SchemaRef;
use snafu::prelude::*;

use crate::ir::{
    statistics::TableStatistics,
    table_ref::{ResolvedTableRef, TableRef},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataSourceId(pub i64);

#[derive(Debug, Snafu)]
pub enum CatalogError {
    #[snafu(display("Table '{}' already exists with id {}", table, existing_id.0))]
    TableAlreadyExists {
        table: ResolvedTableRef,
        existing_id: DataSourceId,
    },
    #[snafu(display("Table '{}' not found", table))]
    TableNotFound { table: TableRef },
    #[snafu(display("Data source {} not found", data_source_id.0))]
    DataSourceNotFound { data_source_id: DataSourceId },
}

pub type Result<T> = core::result::Result<T, CatalogError>;

/// Contains metadata information about a table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableMetadata {
    pub id: DataSourceId,
    pub table: ResolvedTableRef,
    pub schema: SchemaRef,
    pub stats: Option<TableStatistics>,
}

pub trait Catalog: Send + Sync + 'static {
    /// Creates a table.
    fn try_create_table(&self, table: TableRef, schema: SchemaRef) -> Result<DataSourceId>;

    /// Creates a table with stats.
    fn try_create_table_with_stats(
        &self,
        table: TableRef,
        schema: SchemaRef,
        stats: TableStatistics,
    ) -> Result<DataSourceId>;

    /// Describes the schema of a table with identifier `table_id`.
    fn table(&self, table_id: DataSourceId) -> Result<TableMetadata>;
    /// Describes the schema of a table with reference `table`.
    fn table_by_ref(&self, table: &TableRef) -> Result<TableMetadata>;

    fn set_table_stats(&self, table_id: DataSourceId, stats: TableStatistics) -> Result<()>;
}
