use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{
    DataFusionError, Result as DFResult, ScalarValue as DFScalarValue, TableReference,
};
use datafusion::prelude::SessionContext;
use optd_core::{
    Catalog, ColumnDistributionStatistics, ColumnStatistics, Histogram, HistogramBucket,
    MAX_HISTOGRAM_BUCKETS, MAX_MOST_COMMON_VALUES, MostCommonValue, MostCommonValues,
    NullCountStatistics, ScalarValue, StatisticsCompleteness, StatisticsProvenance, TableRef,
    TableStatistics,
};

/// Default maximum number of exact most-common values retained per eligible column.
pub const DEFAULT_MCV_LIMIT: usize = MAX_MOST_COMMON_VALUES;
/// Default maximum number of residual histogram buckets retained per eligible column.
pub const DEFAULT_HISTOGRAM_BUCKETS: usize = MAX_HISTOGRAM_BUCKETS;
/// Collect distributions for columns below this absolute NDV threshold.
pub const DEFAULT_DISTRIBUTION_MAX_DISTINCT: usize = 100_000;
/// Also collect distributions when NDV is at most this fraction of non-null rows.
pub const DEFAULT_DISTRIBUTION_MAX_DISTINCT_FRACTION: f64 = 0.75;

/// Policy for the explicit, full-scan bounded distribution collector.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundedStatisticsCollectionConfig {
    pub mcv_limit: usize,
    pub histogram_buckets: usize,
    pub max_distinct: usize,
    pub max_distinct_fraction: f64,
}

impl Default for BoundedStatisticsCollectionConfig {
    fn default() -> Self {
        Self {
            mcv_limit: DEFAULT_MCV_LIMIT,
            histogram_buckets: DEFAULT_HISTOGRAM_BUCKETS,
            max_distinct: DEFAULT_DISTRIBUTION_MAX_DISTINCT,
            max_distinct_fraction: DEFAULT_DISTRIBUTION_MAX_DISTINCT_FRACTION,
        }
    }
}

/// Collection-cost counters for one explicit bounded statistics collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedStatisticsCollectionMetrics {
    /// One aggregate query produces the base row-count, NDV, null, and bound profile.
    pub base_profile_queries: usize,
    /// One grouped query is issued for each distribution-eligible column.
    pub distribution_queries: usize,
    /// Number of value/frequency groups returned across eligible columns.
    pub grouped_values: usize,
    pub eligible_columns: usize,
    pub skipped_columns: usize,
    /// Columns whose grouped values cannot be represented by the core contract.
    pub unsupported_columns: usize,
    pub elapsed: Duration,
}

/// Result of an explicit bounded collection, including lightweight cost evidence.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundedStatisticsCollection {
    pub statistics: TableStatistics,
    pub metrics: BoundedStatisticsCollectionMetrics,
}

/// Collects table and column statistics by running local aggregate SQL.
///
/// The helper is intentionally connector-side: core optd consumes catalog
/// statistics but does not execute SQL during optimization.
pub async fn collect_table_statistics(
    session: &SessionContext,
    table_name: &str,
    columns: &[&str],
) -> DFResult<TableStatistics> {
    let sql = statistics_sql(&quote_ident(table_name), columns);
    collect_table_statistics_from_sql(session, &sql, columns).await
}

/// Collects table and column statistics for a resolved DataFusion table reference.
pub async fn collect_table_statistics_for_table_ref(
    session: &SessionContext,
    table: &TableReference,
    columns: &[&str],
) -> DFResult<TableStatistics> {
    let sql = statistics_sql(&quote_table_reference(table), columns);
    collect_table_statistics_from_sql(session, &sql, columns).await
}

/// Explicitly collects exact base profiles and bounded distributions by scanning a table.
///
/// This function is deliberately not used by query-time planning. Callers opt into its cost for
/// offline preparation, then persist or install the returned statistics themselves. Eligible
/// columns use the experiment policy: bounded distributions are collected when the non-null NDV
/// is below either the absolute threshold or the configured fraction of non-null rows.
///
/// Collection issues multiple scans. The source must remain stable for the duration of the call,
/// or the session/provider must supply a consistent snapshot. Detectable contradictions between
/// the base profile and a grouped scan are rejected rather than published as exact statistics.
pub async fn collect_bounded_table_statistics_for_table_ref(
    session: &SessionContext,
    table: &TableReference,
    columns: &[&str],
    config: &BoundedStatisticsCollectionConfig,
) -> DFResult<BoundedStatisticsCollection> {
    validate_bounded_collection_config(config)?;
    let started = Instant::now();
    let table_sql = quote_table_reference(table);
    let sql = statistics_sql(&table_sql, columns);
    let mut statistics = collect_table_statistics_from_sql(session, &sql, columns).await?;
    let row_count = statistics.row_count;
    let mut distribution_queries = 0;
    let mut grouped_values = 0;
    let mut eligible_columns = 0;
    let mut skipped_columns = 0;
    let mut unsupported_columns = 0;

    for column in columns {
        let mut normalized_distinct = None;
        let column_statistics = statistics
            .column_statistics
            .get(*column)
            .expect("the base statistics query creates every requested column profile");
        let null_count = match (row_count, column_statistics.frequency) {
            (Some(rows), Some(non_null)) => Some(NullCountStatistics {
                count: rows.checked_sub(non_null).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "column '{column}' non-null frequency {non_null} exceeds row count {rows}"
                    ))
                })?,
                completeness: StatisticsCompleteness::Complete,
                provenance: StatisticsProvenance::FullScan,
            }),
            _ => None,
        };
        let expected_frequency = column_statistics.frequency;
        let expected_distinct = column_statistics.distinct;
        let eligible = distribution_eligible(column_statistics, config);
        let distribution = if eligible {
            distribution_queries += 1;
            let collected = collect_value_frequencies(session, &table_sql, column).await?;
            grouped_values += collected.raw_groups;
            if let Some(groups) = collected.groups {
                eligible_columns += 1;
                normalized_distinct = Some(groups.len());
                build_distribution(
                    groups,
                    expected_distinct.expect("eligibility requires NDV"),
                    collected.raw_groups,
                    expected_frequency.expect("eligibility requires frequency"),
                    null_count,
                    histogram_eligible(column_statistics),
                    config,
                )?
            } else {
                skipped_columns += 1;
                unsupported_columns += 1;
                ColumnDistributionStatistics {
                    null_count,
                    ..Default::default()
                }
            }
        } else {
            skipped_columns += 1;
            ColumnDistributionStatistics {
                null_count,
                ..Default::default()
            }
        };
        let profile = statistics
            .column_statistics
            .get_mut(*column)
            .expect("column exists");
        if has_unrepresentable_float_bound(profile) {
            profile.lower_bound = None;
            profile.upper_bound = None;
        }
        if let Some(distinct) = normalized_distinct {
            profile.distinct = Some(distinct);
        }
        profile.distribution = Some(distribution);
    }

    Ok(BoundedStatisticsCollection {
        statistics,
        metrics: BoundedStatisticsCollectionMetrics {
            base_profile_queries: 1,
            distribution_queries,
            grouped_values,
            eligible_columns,
            skipped_columns,
            unsupported_columns,
            elapsed: started.elapsed(),
        },
    })
}

fn validate_bounded_collection_config(config: &BoundedStatisticsCollectionConfig) -> DFResult<()> {
    if config.mcv_limit == 0 || config.mcv_limit > MAX_MOST_COMMON_VALUES {
        return Err(DataFusionError::Plan(format!(
            "MCV limit {} must be between 1 and {MAX_MOST_COMMON_VALUES}",
            config.mcv_limit
        )));
    }
    if config.histogram_buckets == 0 || config.histogram_buckets > MAX_HISTOGRAM_BUCKETS {
        return Err(DataFusionError::Plan(format!(
            "histogram bucket count {} must be between 1 and {MAX_HISTOGRAM_BUCKETS}",
            config.histogram_buckets
        )));
    }
    if !config.max_distinct_fraction.is_finite()
        || !(0.0..=1.0).contains(&config.max_distinct_fraction)
    {
        return Err(DataFusionError::Plan(
            "distribution NDV fraction must be finite and between 0 and 1".to_string(),
        ));
    }
    Ok(())
}

fn distribution_eligible(
    statistics: &ColumnStatistics,
    config: &BoundedStatisticsCollectionConfig,
) -> bool {
    let (Some(distinct), Some(frequency)) = (statistics.distinct, statistics.frequency) else {
        return false;
    };
    distinct > 0
        && frequency > 0
        && statistics.lower_bound.is_some()
        && (distinct <= config.max_distinct
            || distinct as f64 / frequency as f64 <= config.max_distinct_fraction)
}

fn build_distribution(
    groups: Vec<MostCommonValue>,
    expected_distinct: usize,
    raw_group_count: usize,
    expected_frequency: usize,
    null_count: Option<NullCountStatistics>,
    collect_histogram: bool,
    config: &BoundedStatisticsCollectionConfig,
) -> DFResult<ColumnDistributionStatistics> {
    let distinct = groups.len();
    if raw_group_count != expected_distinct {
        return Err(DataFusionError::Internal(format!(
            "statistics grouped {raw_group_count} raw values but the base profile reported NDV {expected_distinct}; the source may have changed between scans"
        )));
    }
    let grouped_frequency = checked_frequency_sum(&groups, "grouped values")?;
    if grouped_frequency != expected_frequency {
        return Err(DataFusionError::Internal(format!(
            "statistics grouped frequency {grouped_frequency} but the base profile reported non-null frequency {expected_frequency}; the source may have changed between scans"
        )));
    }
    let mut ranked = (0..distinct).collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        groups[*right]
            .frequency
            .cmp(&groups[*left].frequency)
            .then_with(|| left.cmp(right))
    });
    ranked.truncate(config.mcv_limit);
    let mcv_indices = ranked.iter().copied().collect::<BTreeSet<_>>();
    let mcv_entries = ranked
        .into_iter()
        .map(|index| groups[index].clone())
        .collect::<Vec<_>>();
    let mcv_frequency = checked_frequency_sum(&mcv_entries, "MCV entries")?;
    if mcv_frequency > expected_frequency {
        return Err(DataFusionError::Internal(format!(
            "MCV frequency {mcv_frequency} exceeds non-null frequency {expected_frequency}"
        )));
    }
    let mcv_completeness = if mcv_entries.len() == distinct {
        StatisticsCompleteness::Complete
    } else {
        StatisticsCompleteness::Partial
    };
    let most_common_values = MostCommonValues::try_new(
        mcv_entries,
        mcv_completeness,
        StatisticsProvenance::FullScan,
    )
    .map_err(|error| DataFusionError::Internal(error.to_string()))?;

    let histogram = if collect_histogram {
        let residual = groups
            .into_iter()
            .enumerate()
            .filter_map(|(index, value)| (!mcv_indices.contains(&index)).then_some(value))
            .collect::<Vec<_>>();
        let expected_residual = expected_frequency - mcv_frequency;
        let residual_frequency = checked_frequency_sum(&residual, "histogram residual")?;
        if residual_frequency != expected_residual {
            return Err(DataFusionError::Internal(format!(
                "histogram residual frequency {residual_frequency} does not match non-MCV frequency {expected_residual}"
            )));
        }
        let buckets = equi_depth_residual_buckets(&residual, config.histogram_buckets)?;
        let histogram_frequency = buckets.iter().try_fold(0usize, |total, bucket| {
            total.checked_add(bucket.frequency).ok_or_else(|| {
                DataFusionError::Internal(
                    "histogram bucket frequency sum overflowed usize".to_string(),
                )
            })
        })?;
        if histogram_frequency != expected_residual {
            return Err(DataFusionError::Internal(format!(
                "histogram frequency {histogram_frequency} does not match non-MCV frequency {expected_residual}"
            )));
        }
        Some(
            Histogram::try_new(
                buckets,
                StatisticsCompleteness::Complete,
                StatisticsProvenance::FullScan,
            )
            .map_err(|error| DataFusionError::Internal(error.to_string()))?,
        )
    } else {
        None
    };

    Ok(ColumnDistributionStatistics {
        null_count,
        most_common_values: Some(most_common_values),
        histogram,
    })
}

fn checked_frequency_sum(entries: &[MostCommonValue], label: &str) -> DFResult<usize> {
    entries.iter().try_fold(0usize, |total, entry| {
        total.checked_add(entry.frequency).ok_or_else(|| {
            DataFusionError::Internal(format!("{label} frequency sum overflowed usize"))
        })
    })
}

fn has_unrepresentable_float_bound(statistics: &ColumnStatistics) -> bool {
    [&statistics.lower_bound, &statistics.upper_bound]
        .into_iter()
        .flatten()
        .any(|bound| matches!(bound, ScalarValue::Float64(value) if value.is_nan()))
}

fn histogram_eligible(statistics: &ColumnStatistics) -> bool {
    matches!(
        statistics.lower_bound,
        Some(
            ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Date32(_)
                | ScalarValue::Decimal128 { .. }
        )
    )
}

/// Forms weighted equi-depth buckets without splitting equal values across boundaries.
fn equi_depth_residual_buckets(
    residual: &[MostCommonValue],
    bucket_count: usize,
) -> DFResult<Vec<HistogramBucket>> {
    if residual.is_empty() || bucket_count == 0 {
        return Ok(Vec::new());
    }
    let total = checked_frequency_sum(residual, "histogram residual")?;
    if total == 0 {
        return Ok(Vec::new());
    }
    let mut buckets = Vec::<HistogramBucket>::new();
    let mut cumulative = 0usize;
    let mut current_bucket = None;
    for entry in residual {
        let bucket = ((cumulative as u128 * bucket_count as u128) / total as u128)
            .min((bucket_count - 1) as u128) as usize;
        if current_bucket != Some(bucket) {
            buckets.push(HistogramBucket {
                lower_bound: entry.value.clone(),
                upper_bound: entry.value.clone(),
                frequency: entry.frequency,
            });
            current_bucket = Some(bucket);
        } else {
            let current = buckets.last_mut().expect("a histogram bucket exists");
            current.upper_bound = entry.value.clone();
            current.frequency =
                current
                    .frequency
                    .checked_add(entry.frequency)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "histogram bucket frequency overflowed usize".to_string(),
                        )
                    })?;
        }
        cumulative = cumulative.checked_add(entry.frequency).ok_or_else(|| {
            DataFusionError::Internal("histogram cumulative frequency overflowed usize".to_string())
        })?;
    }
    Ok(buckets)
}

struct CollectedValueFrequencies {
    groups: Option<Vec<MostCommonValue>>,
    raw_groups: usize,
}

async fn collect_value_frequencies(
    session: &SessionContext,
    table_sql: &str,
    column: &str,
) -> DFResult<CollectedValueFrequencies> {
    let quoted = quote_ident(column);
    let sql = format!(
        "SELECT {quoted} AS \"value\", COUNT(*) AS \"frequency\" \
         FROM {table_sql} WHERE {quoted} IS NOT NULL \
         GROUP BY {quoted} ORDER BY \"value\" ASC"
    );
    let batches = session.sql(&sql).await?.collect().await?;
    let mut groups = Vec::<MostCommonValue>::new();
    let mut raw_groups = 0;
    let mut unsupported = false;
    for batch in &batches {
        let value_index = batch
            .schema()
            .index_of("value")
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        let frequency_index = batch
            .schema()
            .index_of("frequency")
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        for row in 0..batch.num_rows() {
            raw_groups += 1;
            let value = DFScalarValue::try_from_array(batch.column(value_index), row)?;
            let frequency = DFScalarValue::try_from_array(batch.column(frequency_index), row)?;
            let Some(value) = normalize_distribution_scalar(value) else {
                unsupported = true;
                continue;
            };
            let Some(frequency) = scalar_to_usize(frequency) else {
                return Err(DataFusionError::Internal(
                    "grouped statistics frequency was not a non-negative integer".to_string(),
                ));
            };
            if matches!(value, ScalarValue::Float64(value) if value == 0.0)
                && let Some(zero) = groups.iter_mut().find(
                    |entry| matches!(entry.value, ScalarValue::Float64(value) if value == 0.0),
                )
            {
                zero.frequency = zero.frequency.checked_add(frequency).ok_or_else(|| {
                    DataFusionError::Internal("signed-zero frequency overflowed usize".to_string())
                })?;
            } else {
                groups.push(MostCommonValue { value, frequency });
            }
        }
    }
    Ok(CollectedValueFrequencies {
        groups: (!unsupported).then_some(groups),
        raw_groups,
    })
}

fn normalize_distribution_scalar(value: DFScalarValue) -> Option<ScalarValue> {
    match value {
        DFScalarValue::Float64(Some(value)) if value.is_nan() => None,
        DFScalarValue::Float64(Some(value)) => {
            Some(ScalarValue::Float64(if value == 0.0 { 0.0 } else { value }))
        }
        value => convert_scalar(value),
    }
}

async fn collect_table_statistics_from_sql(
    session: &SessionContext,
    sql: &str,
    columns: &[&str],
) -> DFResult<TableStatistics> {
    let batches = session.sql(sql).await?.collect().await?;
    let batch = first_batch(&batches)?;

    let row_count = scalar_usize(batch, "row_count")?;
    let mut column_statistics = BTreeMap::new();
    for column in columns {
        let frequency = scalar_usize(batch, &format!("{column}__frequency"))?;
        let distinct = scalar_usize(batch, &format!("{column}__distinct"))?;
        let lower_bound =
            scalar_value(batch, &format!("{column}__lower"))?.and_then(convert_scalar);
        let upper_bound =
            scalar_value(batch, &format!("{column}__upper"))?.and_then(convert_scalar);
        column_statistics.insert(
            (*column).to_string(),
            ColumnStatistics {
                lower_bound,
                upper_bound,
                frequency,
                distinct,
                distribution: None,
            },
        );
    }

    Ok(TableStatistics {
        row_count,
        size_bytes: None,
        column_statistics,
        constraints: Default::default(),
    })
}

/// Collects statistics and writes them into an optd catalog.
pub async fn collect_and_set_table_statistics(
    session: &SessionContext,
    catalog: &dyn Catalog,
    table_ref: TableRef,
    table_name: &str,
    columns: &[&str],
) -> DFResult<()> {
    let constraints = catalog
        .table_by_ref(&table_ref)
        .map_err(|err| DataFusionError::External(Box::new(err)))?
        .statistics
        .map(|statistics| statistics.constraints)
        .unwrap_or_default();
    let mut statistics = collect_table_statistics(session, table_name, columns).await?;
    statistics.constraints = constraints;
    catalog
        .set_table_statistics(table_ref, statistics)
        .map_err(|err| DataFusionError::External(Box::new(err)))
}

fn statistics_sql(table_sql: &str, columns: &[&str]) -> String {
    let mut exprs = vec!["COUNT(*) AS \"row_count\"".to_string()];
    for column in columns {
        let quoted = quote_ident(column);
        exprs.push(format!("COUNT({quoted}) AS \"{column}__frequency\""));
        exprs.push(format!(
            "COUNT(DISTINCT {quoted}) AS \"{column}__distinct\""
        ));
        exprs.push(format!("MIN({quoted}) AS \"{column}__lower\""));
        exprs.push(format!("MAX({quoted}) AS \"{column}__upper\""));
    }
    format!("SELECT {} FROM {table_sql}", exprs.join(", "))
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn quote_table_reference(table: &TableReference) -> String {
    match (table.catalog(), table.schema()) {
        (Some(catalog), Some(schema)) => {
            format!(
                "{}.{}.{}",
                quote_ident(catalog),
                quote_ident(schema),
                quote_ident(table.table())
            )
        }
        (None, Some(schema)) => format!("{}.{}", quote_ident(schema), quote_ident(table.table())),
        _ => quote_ident(table.table()),
    }
}

fn first_batch(batches: &[RecordBatch]) -> DFResult<&RecordBatch> {
    batches.first().ok_or_else(|| {
        DataFusionError::Internal("statistics query returned no batches".to_string())
    })
}

fn scalar_value(batch: &RecordBatch, column_name: &str) -> DFResult<Option<DFScalarValue>> {
    let index = batch
        .schema()
        .index_of(column_name)
        .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))?;
    let scalar = DFScalarValue::try_from_array(batch.column(index), 0)?;
    Ok(if scalar.is_null() { None } else { Some(scalar) })
}

fn scalar_usize(batch: &RecordBatch, column_name: &str) -> DFResult<Option<usize>> {
    scalar_value(batch, column_name).map(|value| value.and_then(scalar_to_usize))
}

fn scalar_to_usize(value: DFScalarValue) -> Option<usize> {
    match value {
        DFScalarValue::Int64(Some(value)) => usize::try_from(value).ok(),
        DFScalarValue::UInt64(Some(value)) => usize::try_from(value).ok(),
        DFScalarValue::Int32(Some(value)) => usize::try_from(value).ok(),
        DFScalarValue::UInt32(Some(value)) => usize::try_from(value).ok(),
        _ => None,
    }
}

fn convert_scalar(value: DFScalarValue) -> Option<ScalarValue> {
    match value {
        DFScalarValue::Boolean(Some(value)) => Some(ScalarValue::Boolean(value)),
        DFScalarValue::Int32(Some(value)) => Some(ScalarValue::Int32(value)),
        DFScalarValue::Int64(Some(value)) => Some(ScalarValue::Int64(value)),
        DFScalarValue::Float64(Some(value)) => {
            Some(ScalarValue::Float64(if value == 0.0 { 0.0 } else { value }))
        }
        DFScalarValue::Utf8(Some(value))
        | DFScalarValue::Utf8View(Some(value))
        | DFScalarValue::LargeUtf8(Some(value)) => Some(ScalarValue::Utf8(value)),
        DFScalarValue::Date32(Some(value)) => Some(ScalarValue::Date32(value)),
        DFScalarValue::Decimal128(Some(value), precision, scale) => Some(ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use datafusion::arrow::array::{Decimal128Array, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::{Expr, TableType};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_catalog::Session;
    use datafusion_datasource::memory::MemorySourceConfig;
    use optd_core::{BoundedVec, MemoryCatalog, TableConstraints, UniqueKey};
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct ChangingTableProvider {
        schema: Arc<Schema>,
        batches: [RecordBatch; 2],
        scans: AtomicUsize,
    }

    #[async_trait]
    impl TableProvider for ChangingTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            let scan = self.scans.fetch_add(1, Ordering::SeqCst);
            let batch = self.batches[usize::from(scan > 0)].clone();
            Ok(MemorySourceConfig::try_new_exec(
                &[vec![batch]],
                self.schema.clone(),
                projection.cloned(),
            )?)
        }
    }

    #[tokio::test]
    async fn collect_table_statistics_extracts_column_profiles() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("b"),
                    None,
                ])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        session.register_table("t", Arc::new(table)).unwrap();

        let stats = collect_table_statistics(&session, "t", &["id", "name"])
            .await
            .unwrap();

        assert_eq!(stats.row_count, Some(4));
        assert_eq!(stats.column_statistics["id"].frequency, Some(4));
        assert_eq!(stats.column_statistics["id"].distinct, Some(3));
        assert_eq!(
            stats.column_statistics["id"].lower_bound,
            Some(ScalarValue::Int64(1))
        );
        assert_eq!(stats.column_statistics["name"].frequency, Some(3));
        assert_eq!(stats.column_statistics["name"].distinct, Some(2));
    }

    #[tokio::test]
    async fn bounded_collection_is_repeatable_bounded_and_records_exact_metadata() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let values = (0..200)
            .map(|index| match index {
                0..60 => Some(0),
                60..190 => Some(index - 59),
                _ => None,
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..200)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();
        session
            .register_table(
                "profiles",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        let config = BoundedStatisticsCollectionConfig {
            max_distinct: 100,
            ..Default::default()
        };

        let first = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("profiles"),
            &["id", "value"],
            &config,
        )
        .await
        .unwrap();
        let second = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("profiles"),
            &["id", "value"],
            &config,
        )
        .await
        .unwrap();

        assert_eq!(first.statistics, second.statistics);
        assert_eq!(first.metrics.base_profile_queries, 1);
        assert_eq!(first.metrics.distribution_queries, 1);
        assert_eq!(first.metrics.eligible_columns, 1);
        assert_eq!(first.metrics.skipped_columns, 1);
        assert_eq!(first.metrics.grouped_values, 131);

        let id_distribution = first.statistics.column_statistics["id"]
            .distribution
            .as_ref()
            .unwrap();
        assert!(id_distribution.most_common_values.is_none());
        assert!(id_distribution.histogram.is_none());
        assert_eq!(
            id_distribution.null_count,
            Some(NullCountStatistics {
                count: 0,
                completeness: StatisticsCompleteness::Complete,
                provenance: StatisticsProvenance::FullScan,
            })
        );

        let value_distribution = first.statistics.column_statistics["value"]
            .distribution
            .as_ref()
            .unwrap();
        let mcvs = value_distribution.most_common_values.as_ref().unwrap();
        assert_eq!(mcvs.entries().len(), MAX_MOST_COMMON_VALUES);
        assert_eq!(mcvs.entries()[0].value, ScalarValue::Int64(0));
        assert_eq!(mcvs.entries()[0].frequency, 60);
        assert_eq!(mcvs.completeness(), StatisticsCompleteness::Partial);
        assert_eq!(mcvs.provenance(), &StatisticsProvenance::FullScan);
        let histogram = value_distribution.histogram.as_ref().unwrap();
        assert!(histogram.buckets().len() <= MAX_HISTOGRAM_BUCKETS);
        assert_eq!(
            histogram
                .buckets()
                .iter()
                .map(|bucket| bucket.frequency)
                .sum::<usize>(),
            3
        );
        assert_eq!(histogram.completeness(), StatisticsCompleteness::Complete);
        assert_eq!(histogram.provenance(), &StatisticsProvenance::FullScan);
        assert_eq!(
            value_distribution.null_count,
            Some(NullCountStatistics {
                count: 10,
                completeness: StatisticsCompleteness::Complete,
                provenance: StatisticsProvenance::FullScan,
            })
        );
    }

    #[tokio::test]
    async fn bounded_collection_rejects_limits_above_the_catalog_contract() {
        let session = SessionContext::new();
        let too_many_mcvs = BoundedStatisticsCollectionConfig {
            mcv_limit: MAX_MOST_COMMON_VALUES + 1,
            ..Default::default()
        };
        let too_many_buckets = BoundedStatisticsCollectionConfig {
            histogram_buckets: MAX_HISTOGRAM_BUCKETS + 1,
            ..Default::default()
        };
        let zero_mcvs = BoundedStatisticsCollectionConfig {
            mcv_limit: 0,
            ..Default::default()
        };
        let zero_buckets = BoundedStatisticsCollectionConfig {
            histogram_buckets: 0,
            ..Default::default()
        };
        let invalid_fraction = BoundedStatisticsCollectionConfig {
            max_distinct_fraction: f64::NAN,
            ..Default::default()
        };
        let oversized_fraction = BoundedStatisticsCollectionConfig {
            max_distinct_fraction: 1.01,
            ..Default::default()
        };

        let mcv_error = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("unused"),
            &[],
            &too_many_mcvs,
        )
        .await
        .unwrap_err();
        let histogram_error = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("unused"),
            &[],
            &too_many_buckets,
        )
        .await
        .unwrap_err();
        let zero_mcv_error = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("unused"),
            &[],
            &zero_mcvs,
        )
        .await
        .unwrap_err();
        let zero_bucket_error = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("unused"),
            &[],
            &zero_buckets,
        )
        .await
        .unwrap_err();
        let fraction_error = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("unused"),
            &[],
            &invalid_fraction,
        )
        .await
        .unwrap_err();
        let oversized_fraction_error = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("unused"),
            &[],
            &oversized_fraction,
        )
        .await
        .unwrap_err();

        assert!(mcv_error.to_string().contains("MCV limit"));
        assert!(
            histogram_error
                .to_string()
                .contains("histogram bucket count")
        );
        assert!(zero_mcv_error.to_string().contains("MCV limit 0"));
        assert!(
            zero_bucket_error
                .to_string()
                .contains("histogram bucket count 0")
        );
        assert!(fraction_error.to_string().contains("NDV fraction"));
        assert!(
            oversized_fraction_error
                .to_string()
                .contains("NDV fraction")
        );
    }

    #[tokio::test]
    async fn bounded_collection_marks_complete_text_mcvs_without_a_histogram() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("b"),
                None,
            ]))],
        )
        .unwrap();
        session
            .register_table(
                "names",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();

        let collected = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("names"),
            &["name"],
            &BoundedStatisticsCollectionConfig::default(),
        )
        .await
        .unwrap();
        let distribution = collected.statistics.column_statistics["name"]
            .distribution
            .as_ref()
            .unwrap();
        let mcvs = distribution.most_common_values.as_ref().unwrap();

        assert_eq!(mcvs.completeness(), StatisticsCompleteness::Complete);
        assert_eq!(mcvs.entries().len(), 2);
        assert_eq!(mcvs.entries()[0].value, ScalarValue::Utf8("b".into()));
        assert!(distribution.histogram.is_none());
        assert_eq!(collected.metrics.distribution_queries, 1);
    }

    #[tokio::test]
    async fn bounded_collection_merges_signed_float_zero() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![-0.0, 0.0, 1.0]))],
        )
        .unwrap();
        session
            .register_table(
                "signed_zero",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();

        let collected = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("signed_zero"),
            &["value"],
            &BoundedStatisticsCollectionConfig::default(),
        )
        .await
        .unwrap();
        let mcvs = collected.statistics.column_statistics["value"]
            .distribution
            .as_ref()
            .unwrap()
            .most_common_values
            .as_ref()
            .unwrap();

        assert_eq!(
            collected.statistics.column_statistics["value"].distinct,
            Some(2)
        );
        assert_eq!(
            collected.statistics.column_statistics["value"].lower_bound,
            Some(ScalarValue::Float64(0.0))
        );
        assert_eq!(mcvs.entries().len(), 2);
        assert_eq!(mcvs.entries()[0].value, ScalarValue::Float64(0.0));
        assert_eq!(mcvs.entries()[0].frequency, 2);
    }

    #[tokio::test]
    async fn bounded_collection_skips_nan_distribution_without_losing_safe_counts() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(1.0),
                Some(1.0),
                None,
            ]))],
        )
        .unwrap();
        session
            .register_table(
                "nan_values",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();

        let collected = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("nan_values"),
            &["value"],
            &BoundedStatisticsCollectionConfig::default(),
        )
        .await
        .unwrap();
        let profile = &collected.statistics.column_statistics["value"];
        let distribution = profile.distribution.as_ref().unwrap();

        assert_eq!(profile.frequency, Some(3));
        assert_eq!(profile.distinct, Some(2));
        assert_eq!(profile.lower_bound, None);
        assert_eq!(profile.upper_bound, None);
        assert_eq!(
            distribution.null_count,
            Some(NullCountStatistics {
                count: 1,
                completeness: StatisticsCompleteness::Complete,
                provenance: StatisticsProvenance::FullScan,
            })
        );
        assert!(distribution.most_common_values.is_none());
        assert!(distribution.histogram.is_none());
        assert_eq!(collected.metrics.unsupported_columns, 1);
    }

    #[tokio::test]
    async fn bounded_collection_accepts_float_infinities() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![
                f64::NEG_INFINITY,
                0.0,
                f64::INFINITY,
            ]))],
        )
        .unwrap();
        session
            .register_table(
                "infinities",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();

        let collected = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("infinities"),
            &["value"],
            &BoundedStatisticsCollectionConfig {
                mcv_limit: 1,
                histogram_buckets: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let histogram = collected.statistics.column_statistics["value"]
            .distribution
            .as_ref()
            .unwrap()
            .histogram
            .as_ref()
            .unwrap();

        assert_eq!(histogram.buckets().len(), 2);
        assert_eq!(
            histogram.buckets()[1].upper_bound,
            ScalarValue::Float64(f64::INFINITY)
        );
    }

    #[tokio::test]
    async fn bounded_collection_rejects_a_source_that_changes_between_scans() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let first =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let second = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 1, 1, 2]))],
        )
        .unwrap();
        session
            .register_table(
                "changing",
                Arc::new(ChangingTableProvider {
                    schema,
                    batches: [first, second],
                    scans: AtomicUsize::new(0),
                }),
            )
            .unwrap();

        let error = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("changing"),
            &["value"],
            &BoundedStatisticsCollectionConfig::default(),
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("source may have changed between scans")
        );
        assert!(error.to_string().contains("grouped frequency 4"));
        assert!(error.to_string().contains("non-null frequency 2"));
    }

    #[test]
    fn bounded_weighted_histogram_keeps_value_groups_whole_at_bucket_boundaries() {
        let residual = [(1, 1), (2, 8), (3, 1), (4, 8), (5, 1)]
            .into_iter()
            .map(|(value, frequency)| MostCommonValue {
                value: ScalarValue::Int64(value),
                frequency,
            })
            .collect::<Vec<_>>();

        let buckets = equi_depth_residual_buckets(&residual, 2).unwrap();

        assert_eq!(
            buckets,
            vec![
                HistogramBucket {
                    lower_bound: ScalarValue::Int64(1),
                    upper_bound: ScalarValue::Int64(3),
                    frequency: 10,
                },
                HistogramBucket {
                    lower_bound: ScalarValue::Int64(4),
                    upper_bound: ScalarValue::Int64(5),
                    frequency: 9,
                },
            ]
        );
    }

    #[tokio::test]
    async fn bounded_collection_builds_decimal_histograms() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let values = Decimal128Array::from_iter_values([100, 200, 300, 400])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();
        session
            .register_table(
                "decimals",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();

        let collected = collect_bounded_table_statistics_for_table_ref(
            &session,
            &TableReference::bare("decimals"),
            &["value"],
            &BoundedStatisticsCollectionConfig {
                mcv_limit: 1,
                histogram_buckets: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let histogram = collected.statistics.column_statistics["value"]
            .distribution
            .as_ref()
            .unwrap()
            .histogram
            .as_ref()
            .unwrap();

        assert_eq!(histogram.buckets().len(), 2);
        assert!(histogram.buckets().iter().all(|bucket| matches!(
            (&bucket.lower_bound, &bucket.upper_bound),
            (
                ScalarValue::Decimal128 {
                    precision: 10,
                    scale: 2,
                    ..
                },
                ScalarValue::Decimal128 {
                    precision: 10,
                    scale: 2,
                    ..
                }
            )
        )));
    }

    #[tokio::test]
    async fn bounded_collection_handles_empty_and_all_null_columns() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let empty = RecordBatch::new_empty(schema.clone());
        let nulls = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![None, None, None]))],
        )
        .unwrap();
        session
            .register_table(
                "empty_values",
                Arc::new(MemTable::try_new(schema.clone(), vec![vec![empty]]).unwrap()),
            )
            .unwrap();
        session
            .register_table(
                "null_values",
                Arc::new(MemTable::try_new(schema, vec![vec![nulls]]).unwrap()),
            )
            .unwrap();

        for (table, rows, nulls) in [("empty_values", 0, 0), ("null_values", 3, 3)] {
            let collected = collect_bounded_table_statistics_for_table_ref(
                &session,
                &TableReference::bare(table),
                &["value"],
                &BoundedStatisticsCollectionConfig::default(),
            )
            .await
            .unwrap();
            let profile = &collected.statistics.column_statistics["value"];
            let distribution = profile.distribution.as_ref().unwrap();

            assert_eq!(collected.statistics.row_count, Some(rows));
            assert_eq!(profile.frequency, Some(0));
            assert_eq!(profile.distinct, Some(0));
            assert_eq!(distribution.null_count.as_ref().unwrap().count, nulls);
            assert!(distribution.most_common_values.is_none());
            assert!(distribution.histogram.is_none());
            assert_eq!(collected.metrics.distribution_queries, 0);
        }
    }

    #[tokio::test]
    async fn collect_table_statistics_handles_schema_qualified_tables() {
        use datafusion::common::TableReference;

        let session = SessionContext::new();
        session
            .sql("CREATE SCHEMA s")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 2, 4]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        session
            .register_table(TableReference::partial("s", "t"), Arc::new(table))
            .unwrap();

        let table_ref = TableReference::partial("s", "t");
        let stats = collect_table_statistics_for_table_ref(&session, &table_ref, &["id"])
            .await
            .unwrap();

        assert_eq!(stats.row_count, Some(4));
        assert_eq!(stats.column_statistics["id"].distinct, Some(3));
        assert_eq!(
            stats.column_statistics["id"].upper_bound,
            Some(ScalarValue::Int64(4))
        );
    }

    #[tokio::test]
    async fn refreshing_statistics_preserves_catalog_constraints() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        session.register_table("t", Arc::new(table)).unwrap();

        let catalog = MemoryCatalog::new("memory", "public");
        catalog
            .create_table(TableRef::bare("t"), schema, None)
            .unwrap();
        let constraints = TableConstraints {
            unique_keys: BoundedVec::try_new(vec![
                UniqueKey::try_new(vec!["id".to_string()]).unwrap(),
            ])
            .unwrap(),
            foreign_keys: BoundedVec::default(),
        };
        catalog
            .set_table_statistics(
                TableRef::bare("t"),
                TableStatistics {
                    row_count: None,
                    size_bytes: None,
                    column_statistics: BTreeMap::new(),
                    constraints: constraints.clone(),
                },
            )
            .unwrap();

        collect_and_set_table_statistics(&session, &catalog, TableRef::bare("t"), "t", &["id"])
            .await
            .unwrap();

        let refreshed = catalog
            .table_by_ref(&TableRef::bare("t"))
            .unwrap()
            .statistics
            .unwrap();
        assert_eq!(refreshed.row_count, Some(3));
        assert_eq!(refreshed.constraints, constraints);
    }
}
