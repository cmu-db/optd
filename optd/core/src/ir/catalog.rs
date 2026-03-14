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
    #[snafu(display(
        "Catalog entry for table '{}' points to missing data source {}",
        table,
        data_source_id.0
    ))]
    DanglingTableReference {
        table: ResolvedTableRef,
        data_source_id: DataSourceId,
    },
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
    /// Registers `table` with the provided `schema` and returns its stable data source id.
    ///
    /// Returns [`CatalogError::TableAlreadyExists`] if the resolved table reference has
    /// already been registered.
    fn create_table(&self, table: TableRef, schema: SchemaRef) -> Result<DataSourceId>;

    /// Registers `table` with the provided `schema` and initial `stats`.
    ///
    /// Returns the allocated data source id, or [`CatalogError::TableAlreadyExists`] if
    /// the resolved table reference already exists.
    fn create_table_with_stats(
        &self,
        table: TableRef,
        schema: SchemaRef,
        stats: TableStatistics,
    ) -> Result<DataSourceId>;

    /// Returns the metadata associated with `table_id`.
    ///
    /// Returns [`CatalogError::DataSourceNotFound`] when the id is unknown.
    fn table(&self, table_id: DataSourceId) -> Result<TableMetadata>;
    /// Returns the metadata associated with `table`.
    ///
    /// Implementations may resolve partially qualified references using their default
    /// catalog and schema. Returns [`CatalogError::TableNotFound`] when the table is
    /// unknown.
    fn table_by_ref(&self, table: &TableRef) -> Result<TableMetadata>;

    /// Removes `table` from the catalog.
    ///
    /// Implementations may resolve partially qualified references using their default
    /// catalog and schema. Returns [`CatalogError::TableNotFound`] when the table is
    /// unknown.
    fn drop_table(&self, table: TableRef) -> Result<()>;

    /// Replaces the stored statistics for `table_id`.
    ///
    /// Returns [`CatalogError::DataSourceNotFound`] when the id is unknown.
    fn set_table_stats(&self, table_id: DataSourceId, stats: TableStatistics) -> Result<()>;
}
