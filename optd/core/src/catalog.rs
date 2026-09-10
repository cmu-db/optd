use std::collections::{BTreeMap, btree_map::Entry};
use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

pub use arrow_schema::{Schema, SchemaRef};

use crate::ScalarValue;

/// Maximum number of most-common-value entries retained for one column.
pub const MAX_MOST_COMMON_VALUES: usize = 128;

/// Maximum number of histogram buckets retained for one column.
pub const MAX_HISTOGRAM_BUCKETS: usize = 32;

/// Maximum number of columns participating in one key relationship.
pub const MAX_KEY_COLUMNS: usize = 16;

/// Maximum number of unique or foreign keys retained for one table.
pub const MAX_TABLE_KEYS: usize = 64;

/// Error returned when a bounded catalog collection exceeds its contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectionTooLarge {
    pub maximum: usize,
    pub actual: usize,
}

impl fmt::Display for CollectionTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "collection contains {} entries, but at most {} are allowed",
            self.actual, self.maximum
        )
    }
}

impl std::error::Error for CollectionTooLarge {}

/// A collection whose size is validated at construction and deserialization.
///
/// The inner allocation is private so safe callers cannot grow it past `MAX`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoundedVec<T, const MAX: usize>(Vec<T>);

impl<T, const MAX: usize> BoundedVec<T, MAX> {
    /// Validates and wraps an existing vector.
    pub fn try_new(values: Vec<T>) -> Result<Self, CollectionTooLarge> {
        if values.len() > MAX {
            return Err(CollectionTooLarge {
                maximum: MAX,
                actual: values.len(),
            });
        }
        Ok(Self(values))
    }

    /// Returns the configured maximum length.
    pub const fn maximum_len() -> usize {
        MAX
    }

    /// Consumes the wrapper and returns its contents.
    pub fn into_vec(self) -> Vec<T> {
        self.0
    }
}

impl<T, const MAX: usize> Default for BoundedVec<T, MAX> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T, const MAX: usize> Deref for BoundedVec<T, MAX> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, const MAX: usize> TryFrom<Vec<T>> for BoundedVec<T, MAX> {
    type Error = CollectionTooLarge;

    fn try_from(values: Vec<T>) -> Result<Self, Self::Error> {
        Self::try_new(values)
    }
}

impl<'a, T, const MAX: usize> IntoIterator for &'a BoundedVec<T, MAX> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(feature = "serde")]
impl<T: serde::Serialize, const MAX: usize> serde::Serialize for BoundedVec<T, MAX> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.0, serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, T: serde::Deserialize<'de>, const MAX: usize> serde::Deserialize<'de>
    for BoundedVec<T, MAX>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BoundedVecVisitor<T, const MAX: usize>(std::marker::PhantomData<T>);

        impl<'de, T: serde::Deserialize<'de>, const MAX: usize> serde::de::Visitor<'de>
            for BoundedVecVisitor<T, MAX>
        {
            type Value = BoundedVec<T, MAX>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a sequence containing at most {MAX} entries")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                use serde::de::Error;

                if sequence.size_hint().is_some_and(|length| length > MAX) {
                    return Err(A::Error::custom(format_args!(
                        "collection exceeds maximum length {MAX}"
                    )));
                }
                let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX));
                while let Some(value) = sequence.next_element()? {
                    if values.len() == MAX {
                        return Err(A::Error::custom(format_args!(
                            "collection exceeds maximum length {MAX}"
                        )));
                    }
                    values.push(value);
                }
                Ok(BoundedVec(values))
            }
        }

        deserializer.deserialize_seq(BoundedVecVisitor(std::marker::PhantomData))
    }
}

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
    /// Number of rows in the table, when known.
    pub row_count: Option<usize>,
    /// Physical size of the table in bytes, when known.
    pub size_bytes: Option<usize>,
    /// Per-column statistics keyed by catalog column name.
    pub column_statistics: BTreeMap<String, ColumnStatistics>,
    /// Structural key relationships supplied independently of observed data.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "TableConstraints::is_empty")
    )]
    pub constraints: TableConstraints,
}

/// Column-level statistics that a catalog may provide to optimizer analyses.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    /// Smallest observed non-null value, when known.
    pub lower_bound: Option<ScalarValue>,
    /// Largest observed non-null value, when known.
    pub upper_bound: Option<ScalarValue>,
    /// Number of non-null values, when known.
    pub frequency: Option<usize>,
    /// Number of distinct non-null values, when known.
    pub distinct: Option<usize>,
    /// Optional bounded distribution statistics. Existing estimators do not consume this field.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub distribution: Option<ColumnDistributionStatistics>,
}

/// Whether a statistic describes the entire population represented by its source.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StatisticsCompleteness {
    /// The provider did not state whether the statistic is complete.
    #[default]
    Unknown,
    /// The statistic accounts for the entire source population.
    Complete,
    /// The statistic accounts for only part of the source population.
    Partial,
}

/// Backend-neutral description of how a statistic was produced.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum StatisticsProvenance {
    /// The provider did not expose a collection method.
    #[default]
    Unknown,
    /// Statistics were supplied directly by a catalog or storage backend.
    Catalog,
    /// Statistics were computed by scanning the full source population.
    FullScan,
    /// Statistics were computed from a bounded sample of source rows.
    Sampled { rows: usize },
    /// Statistics were supplied synthetically, for example by a benchmark fixture.
    Synthetic,
}

/// Null count annotated with its completeness and collection provenance.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullCountStatistics {
    /// Number of null values in the population described by this statistic.
    pub count: usize,
    pub completeness: StatisticsCompleteness,
    pub provenance: StatisticsProvenance,
}

/// Frequency of one non-null value in a column distribution.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct MostCommonValue {
    pub value: ScalarValue,
    pub frequency: usize,
}

/// A bounded most-common-value distribution.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MostCommonValues {
    pub entries: BoundedVec<MostCommonValue, MAX_MOST_COMMON_VALUES>,
    pub completeness: StatisticsCompleteness,
    pub provenance: StatisticsProvenance,
}

/// One ordered value interval in a column histogram.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucket {
    /// Inclusive lower endpoint.
    pub lower_bound: ScalarValue,
    /// Inclusive upper endpoint.
    pub upper_bound: ScalarValue,
    /// Number of non-null values represented by the bucket.
    pub frequency: usize,
}

/// A bounded ordered histogram.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Histogram {
    pub buckets: BoundedVec<HistogramBucket, MAX_HISTOGRAM_BUCKETS>,
    pub completeness: StatisticsCompleteness,
    pub provenance: StatisticsProvenance,
}

/// Optional richer column statistics carried by the catalog contract.
///
/// Each component records completeness and provenance separately because a backend may, for
/// example, expose an exact null count alongside a sampled histogram.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnDistributionStatistics {
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub null_count: Option<NullCountStatistics>,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub most_common_values: Option<MostCommonValues>,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub histogram: Option<Histogram>,
}

/// One ordered set of columns known to uniquely identify a table row.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UniqueKey {
    columns: BoundedVec<String, MAX_KEY_COLUMNS>,
}

impl UniqueKey {
    /// Creates a bounded unique key.
    pub fn try_new(columns: Vec<String>) -> Result<Self, CollectionTooLarge> {
        Ok(Self {
            columns: BoundedVec::try_new(columns)?,
        })
    }

    /// Returns the ordered key columns.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

/// An enforced relationship from local columns to a referenced unique key.
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    columns: BoundedVec<String, MAX_KEY_COLUMNS>,
    pub referenced_table: TableRef,
    referenced_columns: BoundedVec<String, MAX_KEY_COLUMNS>,
}

impl ForeignKey {
    /// Creates a bounded key relationship when both ordered column sets have equal arity.
    pub fn try_new(
        columns: Vec<String>,
        referenced_table: TableRef,
        referenced_columns: Vec<String>,
    ) -> Result<Self, ForeignKeyError> {
        if columns.len() != referenced_columns.len() {
            return Err(ForeignKeyError::ArityMismatch {
                columns: columns.len(),
                referenced_columns: referenced_columns.len(),
            });
        }
        Ok(Self {
            columns: BoundedVec::try_new(columns)?,
            referenced_table,
            referenced_columns: BoundedVec::try_new(referenced_columns)?,
        })
    }

    /// Returns the ordered local columns.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the ordered referenced columns.
    pub fn referenced_columns(&self) -> &[String] {
        &self.referenced_columns
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for ForeignKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(serde::Deserialize)]
        struct SerializedForeignKey {
            columns: BoundedVec<String, MAX_KEY_COLUMNS>,
            referenced_table: TableRef,
            referenced_columns: BoundedVec<String, MAX_KEY_COLUMNS>,
        }

        let value = SerializedForeignKey::deserialize(deserializer)?;
        ForeignKey::try_new(
            value.columns.into_vec(),
            value.referenced_table,
            value.referenced_columns.into_vec(),
        )
        .map_err(D::Error::custom)
    }
}

/// Error returned while validating a foreign-key relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyError {
    TooManyColumns(CollectionTooLarge),
    ArityMismatch {
        columns: usize,
        referenced_columns: usize,
    },
}

impl From<CollectionTooLarge> for ForeignKeyError {
    fn from(error: CollectionTooLarge) -> Self {
        Self::TooManyColumns(error)
    }
}

impl fmt::Display for ForeignKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyColumns(error) => error.fmt(f),
            Self::ArityMismatch {
                columns,
                referenced_columns,
            } => write!(
                f,
                "foreign key has {columns} local columns but {referenced_columns} referenced columns"
            ),
        }
    }
}

impl std::error::Error for ForeignKeyError {}

/// Bounded structural constraints supplied independently from observed statistics.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableConstraints {
    pub unique_keys: BoundedVec<UniqueKey, MAX_TABLE_KEYS>,
    pub foreign_keys: BoundedVec<ForeignKey, MAX_TABLE_KEYS>,
}

impl TableConstraints {
    /// Returns true when no key relationships are present.
    pub fn is_empty(&self) -> bool {
        self.unique_keys.is_empty() && self.foreign_keys.is_empty()
    }
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

    #[test]
    fn bounded_collections_accept_the_limit_and_reject_one_more() {
        let entries = (0..MAX_MOST_COMMON_VALUES)
            .map(|value| MostCommonValue {
                value: ScalarValue::Int64(value as i64),
                frequency: 1,
            })
            .collect();
        let bounded = BoundedVec::<_, MAX_MOST_COMMON_VALUES>::try_new(entries).unwrap();
        assert_eq!(bounded.len(), MAX_MOST_COMMON_VALUES);

        let too_many = vec![ScalarValue::Int64(0); MAX_HISTOGRAM_BUCKETS + 1];
        assert_eq!(
            BoundedVec::<_, MAX_HISTOGRAM_BUCKETS>::try_new(too_many),
            Err(CollectionTooLarge {
                maximum: MAX_HISTOGRAM_BUCKETS,
                actual: MAX_HISTOGRAM_BUCKETS + 1,
            })
        );
    }

    #[test]
    fn foreign_key_validates_arity_and_column_bound() {
        let mismatch = ForeignKey::try_new(
            vec!["tenant_id".to_string(), "user_id".to_string()],
            TableRef::bare("users"),
            vec!["id".to_string()],
        );
        assert!(matches!(
            mismatch,
            Err(ForeignKeyError::ArityMismatch {
                columns: 2,
                referenced_columns: 1,
            })
        ));

        let too_many = vec!["key".to_string(); MAX_KEY_COLUMNS + 1];
        let error =
            ForeignKey::try_new(too_many.clone(), TableRef::bare("parent"), too_many).unwrap_err();
        assert!(matches!(
            error,
            ForeignKeyError::TooManyColumns(CollectionTooLarge {
                maximum: MAX_KEY_COLUMNS,
                actual,
            }) if actual == MAX_KEY_COLUMNS + 1
        ));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn rich_statistics_round_trip_without_losing_contract_metadata() {
        let statistics = TableStatistics {
            row_count: Some(100),
            size_bytes: Some(4096),
            column_statistics: [(
                "status".to_string(),
                ColumnStatistics {
                    lower_bound: None,
                    upper_bound: None,
                    frequency: Some(95),
                    distinct: Some(3),
                    distribution: Some(ColumnDistributionStatistics {
                        null_count: Some(NullCountStatistics {
                            count: 5,
                            completeness: StatisticsCompleteness::Complete,
                            provenance: StatisticsProvenance::FullScan,
                        }),
                        most_common_values: Some(MostCommonValues {
                            entries: BoundedVec::try_new(vec![MostCommonValue {
                                value: ScalarValue::Utf8("active".to_string()),
                                frequency: 80,
                            }])
                            .unwrap(),
                            completeness: StatisticsCompleteness::Partial,
                            provenance: StatisticsProvenance::Sampled { rows: 100 },
                        }),
                        histogram: Some(Histogram {
                            buckets: BoundedVec::try_new(vec![HistogramBucket {
                                lower_bound: ScalarValue::Int64(1),
                                upper_bound: ScalarValue::Int64(10),
                                frequency: 10,
                            }])
                            .unwrap(),
                            completeness: StatisticsCompleteness::Complete,
                            provenance: StatisticsProvenance::Catalog,
                        }),
                    }),
                },
            )]
            .into_iter()
            .collect(),
            constraints: TableConstraints {
                unique_keys: BoundedVec::try_new(vec![
                    UniqueKey::try_new(vec!["id".to_string()]).unwrap(),
                ])
                .unwrap(),
                foreign_keys: BoundedVec::default(),
            },
        };

        let json = serde_json::to_string(&statistics).unwrap();
        let decoded: TableStatistics = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, statistics);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn deserialization_rejects_oversized_collections() {
        let json = serde_json::to_string(&vec![0; MAX_HISTOGRAM_BUCKETS + 1]).unwrap();
        let error =
            serde_json::from_str::<BoundedVec<usize, MAX_HISTOGRAM_BUCKETS>>(&json).unwrap_err();
        assert!(error.to_string().contains("maximum length 32"));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn deserialization_rejects_mismatched_foreign_key_arity() {
        let json = r#"{
            "columns":["tenant_id","user_id"],
            "referenced_table":{"Bare":{"table":"users"}},
            "referenced_columns":["id"]
        }"#;
        let error = serde_json::from_str::<ForeignKey>(json).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("2 local columns but 1 referenced")
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn legacy_statistics_json_defaults_new_contract_fields() {
        let json = r#"{
            "row_count":10,
            "size_bytes":null,
            "column_statistics":{
                "id":{
                    "lower_bound":null,
                    "upper_bound":null,
                    "frequency":10,
                    "distinct":10
                }
            }
        }"#;
        let statistics: TableStatistics = serde_json::from_str(json).unwrap();
        assert!(statistics.constraints.is_empty());
        assert!(statistics.column_statistics["id"].distribution.is_none());
    }
}
