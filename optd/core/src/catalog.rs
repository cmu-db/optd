use std::collections::{BTreeMap, btree_map::Entry};
use std::fmt;
use std::sync::{Arc, RwLock};

pub use arrow_schema::{Schema, SchemaRef};

use crate::ScalarValue;

/// Stable identifier for a catalog-registered table.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub usize);

/// Fully qualified table reference.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResolvedTableRef {
    pub catalog: Arc<str>,
    pub schema: Arc<str>,
    pub table: Arc<str>,
}

impl fmt::Display for ResolvedTableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Possibly qualified table reference used by scans before catalog resolution.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TableRef {
    /// Unqualified table name, such as `lineitem`.
    Bare { table: Arc<str> },
    /// Schema-qualified table name, such as `public.lineitem`.
    Partial { schema: Arc<str>, table: Arc<str> },
    /// Fully-qualified table name, such as `memory.public.lineitem`.
    Full {
        catalog: Arc<str>,
        schema: Arc<str>,
        table: Arc<str>,
    },
}

impl TableRef {
    /// Creates an unqualified table reference.
    pub fn bare(table: impl Into<Arc<str>>) -> Self {
        Self::Bare {
            table: table.into(),
        }
    }

    /// Creates a schema-qualified table reference.
    pub fn partial(schema: impl Into<Arc<str>>, table: impl Into<Arc<str>>) -> Self {
        Self::Partial {
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// Creates a fully-qualified table reference.
    pub fn full(
        catalog: impl Into<Arc<str>>,
        schema: impl Into<Arc<str>>,
        table: impl Into<Arc<str>>,
    ) -> Self {
        Self::Full {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// Returns the table name regardless of qualification.
    pub fn table(&self) -> &str {
        match self {
            Self::Bare { table } | Self::Partial { table, .. } | Self::Full { table, .. } => table,
        }
    }

    /// Returns the schema name when present.
    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::Partial { schema, .. } | Self::Full { schema, .. } => Some(schema),
            Self::Bare { .. } => None,
        }
    }

    /// Returns the catalog name when present.
    pub fn catalog(&self) -> Option<&str> {
        match self {
            Self::Full { catalog, .. } => Some(catalog),
            Self::Bare { .. } | Self::Partial { .. } => None,
        }
    }

    /// Resolves this table using default catalog and schema names.
    pub fn resolve(self, default_catalog: &str, default_schema: &str) -> ResolvedTableRef {
        match self {
            Self::Bare { table } => ResolvedTableRef {
                catalog: default_catalog.into(),
                schema: default_schema.into(),
                table,
            },
            Self::Partial { schema, table } => ResolvedTableRef {
                catalog: default_catalog.into(),
                schema,
                table,
            },
            Self::Full {
                catalog,
                schema,
                table,
            } => ResolvedTableRef {
                catalog,
                schema,
                table,
            },
        }
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bare { table } => write!(f, "{table}"),
            Self::Partial { schema, table } => write!(f, "{schema}.{table}"),
            Self::Full {
                catalog,
                schema,
                table,
            } => write!(f, "{catalog}.{schema}.{table}"),
        }
    }
}

impl From<ResolvedTableRef> for TableRef {
    fn from(value: ResolvedTableRef) -> Self {
        Self::Full {
            catalog: value.catalog,
            schema: value.schema,
            table: value.table,
        }
    }
}

impl From<&ResolvedTableRef> for TableRef {
    fn from(value: &ResolvedTableRef) -> Self {
        Self::Full {
            catalog: value.catalog.clone(),
            schema: value.schema.clone(),
            table: value.table.clone(),
        }
    }
}

/// Table-level statistics that a catalog may provide to optimizer analyses.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct TableStatistics {
    pub row_count: Option<usize>,
    pub size_bytes: Option<usize>,
    pub column_statistics: BTreeMap<String, ColumnStatistics>,
}

/// Column-level statistics that a catalog may provide to optimizer analyses.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    pub lower_bound: Option<ScalarValue>,
    pub upper_bound: Option<ScalarValue>,
    pub frequency: Option<usize>,
    pub distinct: Option<usize>,
}

/// Metadata recorded for a catalog table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableMetadata {
    pub id: TableId,
    pub table: ResolvedTableRef,
    pub schema: SchemaRef,
    pub statistics: Option<TableStatistics>,
    pub definition: Option<String>,
}

/// Error produced by catalog lookup or mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    TableAlreadyExists {
        table: ResolvedTableRef,
        existing_id: TableId,
    },
    TableNotFound {
        table: TableRef,
    },
    TableIdNotFound {
        id: TableId,
    },
    DanglingTableReference {
        table: ResolvedTableRef,
        id: TableId,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TableAlreadyExists { table, existing_id } => {
                write!(
                    f,
                    "table '{table}' already exists with id {}",
                    existing_id.0
                )
            }
            Self::TableNotFound { table } => write!(f, "table '{table}' not found"),
            Self::TableIdNotFound { id } => write!(f, "table id {} not found", id.0),
            Self::DanglingTableReference { table, id } => {
                write!(f, "table '{table}' points to missing table id {}", id.0)
            }
        }
    }
}

impl std::error::Error for CatalogError {}

pub type CatalogResult<T> = Result<T, CatalogError>;

/// Catalog interface for resolving table references into schema-bearing metadata.
pub trait Catalog: Send + Sync + 'static {
    /// Returns a short implementation name.
    fn kind(&self) -> &str {
        "unknown"
    }

    /// Registers a table and returns its stable id.
    fn create_table(
        &self,
        table: TableRef,
        schema: SchemaRef,
        definition: Option<String>,
    ) -> CatalogResult<TableId>;

    /// Returns metadata for a stable table id.
    fn table(&self, id: TableId) -> CatalogResult<TableMetadata>;

    /// Resolves a table reference and returns its metadata.
    fn table_by_ref(&self, table: &TableRef) -> CatalogResult<TableMetadata>;

    /// Removes a table from the catalog.
    fn drop_table(&self, table: TableRef) -> CatalogResult<()>;

    /// Replaces stored statistics for a table.
    fn set_table_statistics(
        &self,
        table: TableRef,
        statistics: TableStatistics,
    ) -> CatalogResult<()>;
}

/// Deterministic in-memory catalog implementation useful for tests and local planning.
#[derive(Debug)]
pub struct MemoryCatalog {
    inner: RwLock<MemoryCatalogInner>,
    default_catalog: String,
    default_schema: String,
}

#[derive(Debug)]
struct MemoryCatalogInner {
    tables: BTreeMap<TableId, TableMetadata>,
    table_to_id: BTreeMap<ResolvedTableRef, TableId>,
    next_table_id: usize,
}

impl MemoryCatalog {
    /// Creates an empty catalog with the defaults used to resolve partial table references.
    pub fn new(default_catalog: impl Into<String>, default_schema: impl Into<String>) -> Self {
        Self {
            inner: RwLock::new(MemoryCatalogInner {
                tables: BTreeMap::new(),
                table_to_id: BTreeMap::new(),
                next_table_id: 0,
            }),
            default_catalog: default_catalog.into(),
            default_schema: default_schema.into(),
        }
    }

    fn resolve_table_ref(&self, table: TableRef) -> ResolvedTableRef {
        table.resolve(&self.default_catalog, &self.default_schema)
    }
}

impl Catalog for MemoryCatalog {
    fn kind(&self) -> &str {
        "memory"
    }

    fn create_table(
        &self,
        table: TableRef,
        schema: SchemaRef,
        definition: Option<String>,
    ) -> CatalogResult<TableId> {
        let mut inner = self.inner.write().expect("memory catalog lock poisoned");
        let table = self.resolve_table_ref(table);
        let id = TableId(inner.next_table_id);

        match inner.table_to_id.entry(table.clone()) {
            Entry::Occupied(entry) => {
                return Err(CatalogError::TableAlreadyExists {
                    table,
                    existing_id: *entry.get(),
                });
            }
            Entry::Vacant(entry) => entry.insert(id),
        };

        inner.tables.insert(
            id,
            TableMetadata {
                id,
                table,
                schema,
                statistics: None,
                definition,
            },
        );
        inner.next_table_id += 1;
        Ok(id)
    }

    fn table(&self, id: TableId) -> CatalogResult<TableMetadata> {
        let inner = self.inner.read().expect("memory catalog lock poisoned");
        inner
            .tables
            .get(&id)
            .cloned()
            .ok_or(CatalogError::TableIdNotFound { id })
    }

    fn table_by_ref(&self, table: &TableRef) -> CatalogResult<TableMetadata> {
        let inner = self.inner.read().expect("memory catalog lock poisoned");
        let resolved = self.resolve_table_ref(table.clone());
        let Some(id) = inner.table_to_id.get(&resolved).copied() else {
            return Err(CatalogError::TableNotFound {
                table: table.clone(),
            });
        };

        inner
            .tables
            .get(&id)
            .cloned()
            .ok_or(CatalogError::DanglingTableReference {
                table: resolved,
                id,
            })
    }

    fn drop_table(&self, table: TableRef) -> CatalogResult<()> {
        let mut inner = self.inner.write().expect("memory catalog lock poisoned");
        let resolved = self.resolve_table_ref(table.clone());
        let Some(id) = inner.table_to_id.remove(&resolved) else {
            return Err(CatalogError::TableNotFound { table });
        };

        inner
            .tables
            .remove(&id)
            .ok_or(CatalogError::DanglingTableReference {
                table: resolved,
                id,
            })?;
        Ok(())
    }

    fn set_table_statistics(
        &self,
        table: TableRef,
        statistics: TableStatistics,
    ) -> CatalogResult<()> {
        let mut inner = self.inner.write().expect("memory catalog lock poisoned");
        let resolved = self.resolve_table_ref(table.clone());
        let Some(id) = inner.table_to_id.get(&resolved).copied() else {
            return Err(CatalogError::TableNotFound { table });
        };

        let metadata = inner
            .tables
            .get_mut(&id)
            .ok_or(CatalogError::DanglingTableReference {
                table: resolved,
                id,
            })?;
        metadata.statistics = Some(statistics);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn table_ref_resolves_with_defaults() {
        assert_eq!(
            TableRef::bare("users").resolve("memory", "public"),
            ResolvedTableRef {
                catalog: "memory".into(),
                schema: "public".into(),
                table: "users".into(),
            }
        );
        assert_eq!(
            TableRef::partial("analytics", "events").resolve("memory", "public"),
            ResolvedTableRef {
                catalog: "memory".into(),
                schema: "analytics".into(),
                table: "events".into(),
            }
        );
    }

    #[test]
    fn memory_catalog_registers_and_resolves_tables() {
        let catalog = MemoryCatalog::new("memory", "public");
        let schema = schema();
        let id = catalog
            .create_table(TableRef::bare("users"), schema.clone(), None)
            .unwrap();

        let by_id = catalog.table(id).unwrap();
        assert_eq!(by_id.schema, schema);
        assert_eq!(by_id.table.to_string(), "memory.public.users");

        let by_name = catalog
            .table_by_ref(&TableRef::partial("public", "users"))
            .unwrap();
        assert_eq!(by_name.id, id);
    }

    #[test]
    fn memory_catalog_rejects_duplicate_table_names() {
        let catalog = MemoryCatalog::new("memory", "public");
        let first = catalog
            .create_table(TableRef::bare("users"), schema(), None)
            .unwrap();
        let duplicate = catalog
            .create_table(TableRef::full("memory", "public", "users"), schema(), None)
            .unwrap_err();

        assert!(matches!(
            duplicate,
            CatalogError::TableAlreadyExists { existing_id, .. } if existing_id == first
        ));
    }

    #[test]
    fn memory_catalog_drops_tables() {
        let catalog = MemoryCatalog::new("memory", "public");
        let id = catalog
            .create_table(TableRef::bare("users"), schema(), None)
            .unwrap();

        catalog.drop_table(TableRef::bare("users")).unwrap();

        assert!(matches!(
            catalog.table(id),
            Err(CatalogError::TableIdNotFound { id: missing }) if missing == id
        ));
        assert!(matches!(
            catalog.table_by_ref(&TableRef::bare("users")),
            Err(CatalogError::TableNotFound { .. })
        ));
    }
}
