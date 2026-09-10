use std::cmp::Ordering;
use std::collections::{BTreeMap, btree_map::Entry};
use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

pub use arrow_schema::{Schema, SchemaRef};

use crate::ScalarValue;

/// Maximum number of most-common-value entries retained for one column.
///
/// This is an entry-count limit, not a byte limit: scalar payloads such as strings have their own
/// allocation sizes.
pub const MAX_MOST_COMMON_VALUES: usize = 128;

/// Maximum number of histogram buckets retained for one column (not a byte limit).
pub const MAX_HISTOGRAM_BUCKETS: usize = 32;

/// Maximum number of columns participating in one key relationship (not a byte limit).
pub const MAX_KEY_COLUMNS: usize = 16;

/// Maximum number of unique or foreign keys retained for one table (not a byte limit).
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

/// A collection whose entry count is validated at construction and deserialization.
///
/// The inner allocation is private so safe callers cannot grow it past `MAX` entries. `MAX` does
/// not cap allocated bytes or the size of an individual `T`.
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

/// Whether a stored representation covers every value in its documented population.
///
/// Completeness describes coverage, not accuracy. For example, an MCV list is `Complete` only if
/// every distinct non-null value appears in it, while a histogram is `Complete` only if its
/// buckets cover all non-null rows not represented by the MCV list. Frequencies remain estimates
/// of full-table row counts even when their provenance is [`StatisticsProvenance::Sampled`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StatisticsCompleteness {
    /// The provider did not state whether the statistic is complete.
    #[default]
    Unknown,
    /// The representation covers its entire documented population.
    Complete,
    /// The representation deliberately omits part of its documented population.
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
    /// Statistics were estimated from `rows` source rows selected for inspection.
    ///
    /// `rows` is the unscaled sample size, not a table row-count estimate. Counts stored in the
    /// statistics remain estimates in full-table row units.
    Sampled { rows: usize },
    /// Statistics were supplied synthetically, for example by a benchmark fixture.
    Synthetic,
}

/// Null count annotated with its completeness and collection provenance.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullCountStatistics {
    /// Estimated number of null values in the full table.
    ///
    /// A sampled collector scales its observed nulls to table-row units before storing this value.
    pub count: usize,
    pub completeness: StatisticsCompleteness,
    pub provenance: StatisticsProvenance,
}

/// Frequency of one non-null value in a column distribution.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct MostCommonValue {
    /// A non-null value. [`MostCommonValues::try_new`] rejects nulls and duplicates.
    pub value: ScalarValue,
    /// Estimated number of full-table rows equal to `value`.
    pub frequency: usize,
}

/// A bounded most-common-value distribution with unique, non-null entries.
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MostCommonValues {
    entries: BoundedVec<MostCommonValue, MAX_MOST_COMMON_VALUES>,
    completeness: StatisticsCompleteness,
    provenance: StatisticsProvenance,
}

impl MostCommonValues {
    /// Validates and constructs an MCV list.
    pub fn try_new(
        entries: Vec<MostCommonValue>,
        completeness: StatisticsCompleteness,
        provenance: StatisticsProvenance,
    ) -> Result<Self, DistributionError> {
        let entries = BoundedVec::try_new(entries)?;
        for (index, entry) in entries.iter().enumerate() {
            if matches!(entry.value, ScalarValue::Null(_)) {
                return Err(DistributionError::NullMostCommonValue { index });
            }
            if scalar_order(&entry.value, &entry.value).is_none() {
                return Err(DistributionError::IncomparableMostCommonValue { index });
            }
            if let Some(first_index) = entries[..index]
                .iter()
                .position(|previous| previous.value == entry.value)
            {
                return Err(DistributionError::DuplicateMostCommonValue { first_index, index });
            }
        }
        Ok(Self {
            entries,
            completeness,
            provenance,
        })
    }

    /// Returns the validated MCV entries.
    pub fn entries(&self) -> &[MostCommonValue] {
        &self.entries
    }

    /// Returns the distribution coverage declaration.
    pub fn completeness(&self) -> StatisticsCompleteness {
        self.completeness
    }

    /// Returns how the distribution was produced.
    pub fn provenance(&self) -> &StatisticsProvenance {
        &self.provenance
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for MostCommonValues {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(serde::Deserialize)]
        struct SerializedMostCommonValues {
            entries: BoundedVec<MostCommonValue, MAX_MOST_COMMON_VALUES>,
            completeness: StatisticsCompleteness,
            provenance: StatisticsProvenance,
        }

        let value = SerializedMostCommonValues::deserialize(deserializer)?;
        Self::try_new(
            value.entries.into_vec(),
            value.completeness,
            value.provenance,
        )
        .map_err(D::Error::custom)
    }
}

/// One ordered value interval in a column histogram.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucket {
    /// Inclusive lower endpoint.
    pub lower_bound: ScalarValue,
    /// Inclusive upper endpoint.
    pub upper_bound: ScalarValue,
    /// Estimated number of full-table rows represented by the bucket.
    ///
    /// Rows equal to an MCV entry are excluded even when the value lies between these endpoints.
    pub frequency: usize,
}

/// A bounded ordered histogram of the non-null, non-MCV residual population.
///
/// Buckets use inclusive endpoints, have compatible endpoint types, and must be strictly ordered
/// and non-overlapping. Intervals may contain an MCV value, but their frequencies exclude rows for
/// all MCV entries. `Complete` means every residual row belongs to a bucket.
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Histogram {
    buckets: BoundedVec<HistogramBucket, MAX_HISTOGRAM_BUCKETS>,
    completeness: StatisticsCompleteness,
    provenance: StatisticsProvenance,
}

impl Histogram {
    /// Validates and constructs a residual histogram.
    pub fn try_new(
        buckets: Vec<HistogramBucket>,
        completeness: StatisticsCompleteness,
        provenance: StatisticsProvenance,
    ) -> Result<Self, DistributionError> {
        let buckets = BoundedVec::try_new(buckets)?;
        for (index, bucket) in buckets.iter().enumerate() {
            let Some(interval_order) = scalar_order(&bucket.lower_bound, &bucket.upper_bound)
            else {
                return Err(DistributionError::IncompatibleHistogramEndpoints { index });
            };
            if interval_order == Ordering::Greater {
                return Err(DistributionError::ReversedHistogramInterval { index });
            }
            if index > 0 {
                let previous = &buckets[index - 1];
                let Some(bucket_order) = scalar_order(&previous.upper_bound, &bucket.lower_bound)
                else {
                    return Err(DistributionError::IncompatibleHistogramBuckets {
                        previous: index - 1,
                        index,
                    });
                };
                if bucket_order != Ordering::Less {
                    return Err(DistributionError::UnorderedHistogramBuckets {
                        previous: index - 1,
                        index,
                    });
                }
            }
        }
        Ok(Self {
            buckets,
            completeness,
            provenance,
        })
    }

    /// Returns the validated ordered buckets.
    pub fn buckets(&self) -> &[HistogramBucket] {
        &self.buckets
    }

    /// Returns the residual-population coverage declaration.
    pub fn completeness(&self) -> StatisticsCompleteness {
        self.completeness
    }

    /// Returns how the histogram was produced.
    pub fn provenance(&self) -> &StatisticsProvenance {
        &self.provenance
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Histogram {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(serde::Deserialize)]
        struct SerializedHistogram {
            buckets: BoundedVec<HistogramBucket, MAX_HISTOGRAM_BUCKETS>,
            completeness: StatisticsCompleteness,
            provenance: StatisticsProvenance,
        }

        let value = SerializedHistogram::deserialize(deserializer)?;
        Self::try_new(
            value.buckets.into_vec(),
            value.completeness,
            value.provenance,
        )
        .map_err(D::Error::custom)
    }
}

fn scalar_order(left: &ScalarValue, right: &ScalarValue) -> Option<Ordering> {
    match (left, right) {
        (ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => Some(left.cmp(right)),
        (ScalarValue::Int32(left), ScalarValue::Int32(right)) => Some(left.cmp(right)),
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => Some(left.cmp(right)),
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => left.partial_cmp(right),
        (
            ScalarValue::Decimal128 {
                value: left,
                precision: left_precision,
                scale: left_scale,
            },
            ScalarValue::Decimal128 {
                value: right,
                precision: right_precision,
                scale: right_scale,
            },
        ) if left_precision == right_precision && left_scale == right_scale => {
            Some(left.cmp(right))
        }
        (ScalarValue::Date32(left), ScalarValue::Date32(right)) => Some(left.cmp(right)),
        (ScalarValue::Utf8(left), ScalarValue::Utf8(right)) => Some(left.cmp(right)),
        (
            ScalarValue::IntervalMonthDayNano {
                months: left_months,
                days: left_days,
                nanoseconds: left_nanoseconds,
            },
            ScalarValue::IntervalMonthDayNano {
                months: right_months,
                days: right_days,
                nanoseconds: right_nanoseconds,
            },
        ) => Some((left_months, left_days, left_nanoseconds).cmp(&(
            right_months,
            right_days,
            right_nanoseconds,
        ))),
        (
            ScalarValue::IntervalDayTime {
                days: left_days,
                milliseconds: left_milliseconds,
            },
            ScalarValue::IntervalDayTime {
                days: right_days,
                milliseconds: right_milliseconds,
            },
        ) => Some((left_days, left_milliseconds).cmp(&(right_days, right_milliseconds))),
        _ => None,
    }
}

/// Error returned while validating an MCV list or histogram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistributionError {
    TooManyEntries(CollectionTooLarge),
    NullMostCommonValue { index: usize },
    IncomparableMostCommonValue { index: usize },
    DuplicateMostCommonValue { first_index: usize, index: usize },
    IncompatibleHistogramEndpoints { index: usize },
    IncompatibleHistogramBuckets { previous: usize, index: usize },
    ReversedHistogramInterval { index: usize },
    UnorderedHistogramBuckets { previous: usize, index: usize },
}

impl From<CollectionTooLarge> for DistributionError {
    fn from(error: CollectionTooLarge) -> Self {
        Self::TooManyEntries(error)
    }
}

impl fmt::Display for DistributionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyEntries(error) => error.fmt(f),
            Self::NullMostCommonValue { index } => {
                write!(f, "MCV entry {index} is null")
            }
            Self::IncomparableMostCommonValue { index } => {
                write!(f, "MCV entry {index} is not equal to itself")
            }
            Self::DuplicateMostCommonValue { first_index, index } => {
                write!(f, "MCV entry {index} duplicates entry {first_index}")
            }
            Self::IncompatibleHistogramEndpoints { index } => write!(
                f,
                "histogram bucket {index} has null, incomparable, or incompatible endpoints"
            ),
            Self::IncompatibleHistogramBuckets { previous, index } => write!(
                f,
                "histogram bucket {index} has an endpoint type incompatible with bucket {previous}"
            ),
            Self::ReversedHistogramInterval { index } => {
                write!(
                    f,
                    "histogram bucket {index} has a lower endpoint above its upper endpoint"
                )
            }
            Self::UnorderedHistogramBuckets { previous, index } => write!(
                f,
                "histogram bucket {index} overlaps, touches, or precedes bucket {previous}"
            ),
        }
    }
}

impl std::error::Error for DistributionError {}

/// Optional richer column statistics carried by the catalog contract.
///
/// Each component records completeness and provenance separately because a backend may, for
/// example, expose an exact null count alongside a sampled histogram. All stored counts and
/// frequencies are estimates in full-table row units. Providers must reconcile cross-component
/// totals with `ColumnStatistics::frequency` and `TableStatistics::row_count`; the core can check
/// the shape of each component but cannot infer a backend's sampling or rounding policy.
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

/// Provider-supplied assertion that an ordered column set forms a unique key.
///
/// The core validates only that the set is non-empty, bounded, and duplicate-free. It does not
/// inspect the table schema or data, distinguish declared from enforced constraints, or assign SQL
/// semantics to nullable key columns. Providers must include a key only when its enforcement and
/// null treatment make the uniqueness claim safe for downstream optimizer use.
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueKey {
    columns: BoundedVec<String, MAX_KEY_COLUMNS>,
}

impl UniqueKey {
    /// Creates a structurally valid bounded unique-key assertion.
    pub fn try_new(columns: Vec<String>) -> Result<Self, UniqueKeyError> {
        let columns = BoundedVec::try_new(columns)?;
        if columns.is_empty() {
            return Err(UniqueKeyError::Empty);
        }
        if let Some(column) = duplicate_column(&columns) {
            return Err(UniqueKeyError::DuplicateColumn { column });
        }
        Ok(Self { columns })
    }

    /// Returns the ordered key columns.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for UniqueKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(serde::Deserialize)]
        struct SerializedUniqueKey {
            columns: BoundedVec<String, MAX_KEY_COLUMNS>,
        }

        let value = SerializedUniqueKey::deserialize(deserializer)?;
        Self::try_new(value.columns.into_vec()).map_err(D::Error::custom)
    }
}

/// Error returned while validating a unique key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UniqueKeyError {
    TooManyColumns(CollectionTooLarge),
    Empty,
    DuplicateColumn { column: String },
}

impl From<CollectionTooLarge> for UniqueKeyError {
    fn from(error: CollectionTooLarge) -> Self {
        Self::TooManyColumns(error)
    }
}

impl fmt::Display for UniqueKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyColumns(error) => error.fmt(f),
            Self::Empty => f.write_str("unique key must contain at least one column"),
            Self::DuplicateColumn { column } => {
                write!(f, "unique key contains duplicate column '{column}'")
            }
        }
    }
}

impl std::error::Error for UniqueKeyError {}

/// Provider-supplied mapping from local columns to a referenced unique-key column set.
///
/// The core validates only local shape: both sets are non-empty, bounded, duplicate-free, and have
/// equal arity. It does not resolve the referenced table or columns, prove referenced uniqueness,
/// verify enforcement, or define how nullable local columns participate. In particular, SQL
/// foreign-key null behavior depends on the provider's dialect and match mode. Providers must
/// expose only relationships whose enforcement and null semantics are safe for the intended
/// optimizer inference.
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    columns: BoundedVec<String, MAX_KEY_COLUMNS>,
    /// Table containing the provider-asserted referenced unique key.
    pub referenced_table: TableRef,
    referenced_columns: BoundedVec<String, MAX_KEY_COLUMNS>,
}

impl ForeignKey {
    /// Creates a structurally valid bounded key relationship.
    pub fn try_new(
        columns: Vec<String>,
        referenced_table: TableRef,
        referenced_columns: Vec<String>,
    ) -> Result<Self, ForeignKeyError> {
        if columns.is_empty() || referenced_columns.is_empty() {
            return Err(ForeignKeyError::Empty);
        }
        if columns.len() != referenced_columns.len() {
            return Err(ForeignKeyError::ArityMismatch {
                columns: columns.len(),
                referenced_columns: referenced_columns.len(),
            });
        }
        let columns = BoundedVec::try_new(columns)?;
        let referenced_columns = BoundedVec::try_new(referenced_columns)?;
        if let Some(column) = duplicate_column(&columns) {
            return Err(ForeignKeyError::DuplicateLocalColumn { column });
        }
        if let Some(column) = duplicate_column(&referenced_columns) {
            return Err(ForeignKeyError::DuplicateReferencedColumn { column });
        }
        Ok(Self {
            columns,
            referenced_table,
            referenced_columns,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForeignKeyError {
    TooManyColumns(CollectionTooLarge),
    Empty,
    ArityMismatch {
        columns: usize,
        referenced_columns: usize,
    },
    DuplicateLocalColumn {
        column: String,
    },
    DuplicateReferencedColumn {
        column: String,
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
            Self::Empty => f.write_str("foreign key must contain at least one column pair"),
            Self::ArityMismatch {
                columns,
                referenced_columns,
            } => write!(
                f,
                "foreign key has {columns} local columns but {referenced_columns} referenced columns"
            ),
            Self::DuplicateLocalColumn { column } => {
                write!(f, "foreign key contains duplicate local column '{column}'")
            }
            Self::DuplicateReferencedColumn { column } => write!(
                f,
                "foreign key contains duplicate referenced column '{column}'"
            ),
        }
    }
}

impl std::error::Error for ForeignKeyError {}

fn duplicate_column(columns: &[String]) -> Option<String> {
    columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| columns[..index].contains(column).then(|| column.clone()))
}

/// Bounded provider-supplied structural metadata, independent from observed statistics.
///
/// These values are assertions rather than constraints enforced by the in-memory catalog. The
/// provider is responsible for validating table and column existence, referenced-key uniqueness,
/// enforcement state, and dialect-specific nullable-key semantics before publishing them.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableConstraints {
    /// Provider-asserted unique keys for this table.
    pub unique_keys: BoundedVec<UniqueKey, MAX_TABLE_KEYS>,
    /// Provider-asserted relationships from this table to referenced unique keys.
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
    fn most_common_values_reject_nulls_and_duplicates() {
        let null = MostCommonValues::try_new(
            vec![MostCommonValue {
                value: ScalarValue::Null(DataType::Int64),
                frequency: 1,
            }],
            StatisticsCompleteness::Partial,
            StatisticsProvenance::Catalog,
        );
        assert_eq!(
            null,
            Err(DistributionError::NullMostCommonValue { index: 0 })
        );

        let duplicate = MostCommonValues::try_new(
            vec![
                MostCommonValue {
                    value: ScalarValue::Int64(7),
                    frequency: 4,
                },
                MostCommonValue {
                    value: ScalarValue::Int64(7),
                    frequency: 3,
                },
            ],
            StatisticsCompleteness::Partial,
            StatisticsProvenance::Sampled { rows: 10 },
        );
        assert_eq!(
            duplicate,
            Err(DistributionError::DuplicateMostCommonValue {
                first_index: 0,
                index: 1,
            })
        );
    }

    #[test]
    fn histogram_rejects_invalid_intervals_and_ordering() {
        let incompatible = Histogram::try_new(
            vec![HistogramBucket {
                lower_bound: ScalarValue::Int64(1),
                upper_bound: ScalarValue::Utf8("10".to_string()),
                frequency: 5,
            }],
            StatisticsCompleteness::Complete,
            StatisticsProvenance::Catalog,
        );
        assert_eq!(
            incompatible,
            Err(DistributionError::IncompatibleHistogramEndpoints { index: 0 })
        );

        let reversed = Histogram::try_new(
            vec![HistogramBucket {
                lower_bound: ScalarValue::Int64(10),
                upper_bound: ScalarValue::Int64(1),
                frequency: 5,
            }],
            StatisticsCompleteness::Complete,
            StatisticsProvenance::Catalog,
        );
        assert_eq!(
            reversed,
            Err(DistributionError::ReversedHistogramInterval { index: 0 })
        );

        let overlapping = Histogram::try_new(
            vec![
                HistogramBucket {
                    lower_bound: ScalarValue::Int64(1),
                    upper_bound: ScalarValue::Int64(5),
                    frequency: 5,
                },
                HistogramBucket {
                    lower_bound: ScalarValue::Int64(5),
                    upper_bound: ScalarValue::Int64(9),
                    frequency: 4,
                },
            ],
            StatisticsCompleteness::Complete,
            StatisticsProvenance::Catalog,
        );
        assert_eq!(
            overlapping,
            Err(DistributionError::UnorderedHistogramBuckets {
                previous: 0,
                index: 1,
            })
        );
    }

    #[test]
    fn keys_reject_empty_and_duplicate_column_sets() {
        assert_eq!(UniqueKey::try_new(Vec::new()), Err(UniqueKeyError::Empty));
        assert_eq!(
            UniqueKey::try_new(vec!["id".to_string(), "id".to_string()]),
            Err(UniqueKeyError::DuplicateColumn {
                column: "id".to_string(),
            })
        );
        assert_eq!(
            ForeignKey::try_new(
                vec!["id".to_string(), "id".to_string()],
                TableRef::bare("parent"),
                vec!["tenant_id".to_string(), "id".to_string()],
            ),
            Err(ForeignKeyError::DuplicateLocalColumn {
                column: "id".to_string(),
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
                    frequency: Some(90),
                    distinct: Some(3),
                    distribution: Some(ColumnDistributionStatistics {
                        null_count: Some(NullCountStatistics {
                            count: 10,
                            completeness: StatisticsCompleteness::Complete,
                            provenance: StatisticsProvenance::FullScan,
                        }),
                        most_common_values: Some(
                            MostCommonValues::try_new(
                                vec![MostCommonValue {
                                    value: ScalarValue::Int64(5),
                                    frequency: 40,
                                }],
                                StatisticsCompleteness::Partial,
                                StatisticsProvenance::Sampled { rows: 25 },
                            )
                            .unwrap(),
                        ),
                        histogram: Some(
                            Histogram::try_new(
                                vec![HistogramBucket {
                                    lower_bound: ScalarValue::Int64(1),
                                    upper_bound: ScalarValue::Int64(10),
                                    // Full-table residual frequency: rows equal to MCV 5 are
                                    // excluded even though 5 lies inside the interval.
                                    frequency: 50,
                                }],
                                StatisticsCompleteness::Complete,
                                StatisticsProvenance::Sampled { rows: 25 },
                            )
                            .unwrap(),
                        ),
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
        let distribution = decoded.column_statistics["status"]
            .distribution
            .as_ref()
            .unwrap();
        let mcv_frequency: usize = distribution
            .most_common_values
            .as_ref()
            .unwrap()
            .entries()
            .iter()
            .map(|entry| entry.frequency)
            .sum();
        assert_eq!(
            distribution
                .most_common_values
                .as_ref()
                .unwrap()
                .provenance(),
            &StatisticsProvenance::Sampled { rows: 25 }
        );
        let residual_frequency: usize = distribution
            .histogram
            .as_ref()
            .unwrap()
            .buckets()
            .iter()
            .map(|bucket| bucket.frequency)
            .sum();
        assert_eq!(mcv_frequency + residual_frequency, 90);
        assert_eq!(
            mcv_frequency + residual_frequency,
            decoded.column_statistics["status"].frequency.unwrap()
        );
        assert_eq!(
            mcv_frequency + residual_frequency + distribution.null_count.as_ref().unwrap().count,
            decoded.row_count.unwrap()
        );
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
    fn deserialization_revalidates_distribution_shapes() {
        let entry = serde_json::to_value(MostCommonValue {
            value: ScalarValue::Int64(7),
            frequency: 10,
        })
        .unwrap();
        let value = serde_json::json!({
            "entries": [entry.clone(), entry],
            "completeness": "Partial",
            "provenance": { "Sampled": { "rows": 20 } }
        });
        let error = serde_json::from_value::<MostCommonValues>(value).unwrap_err();
        assert!(error.to_string().contains("duplicates entry 0"));
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
