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

/// A stable identifier for a catalog-registered data source.
///
/// This id remains the canonical handle for a table even when name-based
/// lookups are resolved separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataSourceId(pub i64);

/// Errors related to the catalog functionalities.
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
    #[snafu(display("Catalog backend error: {}", message))]
    Backend { message: String },
}

pub type Result<T> = core::result::Result<T, CatalogError>;

/// Metadata recorded for a table known to the catalog.
///
/// Returned by catalog lookup APIs to provide the table's stable id, resolved
/// name, schema, and any available statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct TableMetadata {
    /// Stable identifier assigned by the catalog for this table.
    pub id: DataSourceId,
    /// Fully resolved catalog, schema, and table name.
    pub table: ResolvedTableRef,
    /// Arrow schema describing the table's columns.
    pub schema: SchemaRef,
    /// Optional statistics recorded for the table.
    pub statistics: Option<TableStatistics>,
    /// Optional SQL definition for the table.
    pub definition: Option<String>,
}

/// Catalog interface for registering and resolving table metadata.
///
/// optd uses this abstraction to inspect schemas, stable table identities, and
/// optional statistics during planning and execution.
pub trait Catalog: Send + Sync + 'static {
    fn kind(&self) -> &str {
        "unknown"
    }

    /// Registers `table` with the provided `schema` and returns its stable data source id.
    ///
    /// Returns [`CatalogError::TableAlreadyExists`] if the resolved table reference has
    /// already been registered.
    fn create_table(
        &self,
        table: TableRef,
        schema: SchemaRef,
        definition: Option<String>,
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

    /// Replaces the stored statistics for `table`.
    ///
    /// Implementations may resolve partially qualified references using their default
    /// catalog and schema. Returns [`CatalogError::TableNotFound`] when the table is
    /// unknown.
    fn set_table_statistics(&self, table: TableRef, stats: TableStatistics) -> Result<()>;
}
