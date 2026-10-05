use std::collections::{BTreeMap, HashSet};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{
    DataFusionError, Result as DFResult, ScalarValue as DFScalarValue, TableReference,
};
use datafusion::prelude::SessionContext;
use optd_core::{
    Catalog, ColumnSketches, ColumnStatistics, EncodedScalarValue, ScalarValue, TableRef,
    TableStatistics,
};

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
                sketches: None,
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
    scalar_value(batch, column_name).map(|value| match value {
        Some(DFScalarValue::Int64(Some(value))) => usize::try_from(value).ok(),
        Some(DFScalarValue::UInt64(Some(value))) => usize::try_from(value).ok(),
        Some(DFScalarValue::Int32(Some(value))) => usize::try_from(value).ok(),
        Some(DFScalarValue::UInt32(Some(value))) => usize::try_from(value).ok(),
        _ => None,
    })
}

fn convert_scalar(value: DFScalarValue) -> Option<ScalarValue> {
    match value {
        DFScalarValue::Boolean(Some(value)) => Some(ScalarValue::Boolean(value)),
        DFScalarValue::Int32(Some(value)) => Some(ScalarValue::Int32(value)),
        DFScalarValue::Int64(Some(value)) => Some(ScalarValue::Int64(value)),
        DFScalarValue::Float64(Some(value)) => Some(ScalarValue::Float64(value)),
        DFScalarValue::Utf8(Some(value))
        | DFScalarValue::LargeUtf8(Some(value))
        | DFScalarValue::Utf8View(Some(value)) => Some(ScalarValue::Utf8(value)),
        DFScalarValue::Date32(Some(value)) => Some(ScalarValue::Date32(value)),
        DFScalarValue::Decimal128(Some(value), precision, scale) => Some(ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        }),
        _ => None,
    }
}

/// Exact full-scan statistics for explicit test fixtures and regression runs.
///
/// This deliberately executes `SELECT *` and materializes the table. Normal query planning must
/// use the bounded aggregate collector or persisted catalog statistics instead.
pub(crate) async fn collect_full_scan_statistics(
    session: &SessionContext,
    table_name: &str,
    columns: &[&str],
) -> DFResult<TableStatistics> {
    let sql = format!("SELECT * FROM {}", quote_ident(table_name));
    let batches = session.sql(&sql).await?.collect().await?;
    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let mut column_statistics = BTreeMap::new();

    for column_name in columns {
        let mut values = Vec::new();
        for batch in &batches {
            let index = batch
                .schema()
                .index_of(column_name)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
            for row in 0..batch.num_rows() {
                let value = DFScalarValue::try_from_array(batch.column(index), row)?;
                if value.is_null() {
                    continue;
                }
                let value = convert_scalar(value).ok_or_else(|| {
                    DataFusionError::NotImplemented(format!(
                        "full-scan statistics do not support column '{column_name}'"
                    ))
                })?;
                values.push(value);
            }
        }

        let mut exact_distinct = HashSet::new();
        let mut lower_bound: Option<ScalarValue> = None;
        let mut upper_bound: Option<ScalarValue> = None;
        let mut column_sketches = ColumnSketches::new(row_count as u64)
            .hll()
            .space_saving(128)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        for value in &values {
            if let Some(encoded) = EncodedScalarValue::from_scalar(value) {
                exact_distinct.insert(encoded);
            }
            update_test_bounds(&mut lower_bound, &mut upper_bound, value);
            column_sketches.observe(value);
        }

        column_statistics.insert(
            (*column_name).to_string(),
            ColumnStatistics {
                lower_bound,
                upper_bound,
                frequency: Some(values.len()),
                distinct: Some(exact_distinct.len()),
                distribution: None,
                sketches: Some(column_sketches),
            },
        );
    }

    Ok(TableStatistics {
        row_count: Some(row_count),
        size_bytes: None,
        column_statistics,
        constraints: Default::default(),
    })
}

fn update_test_bounds(
    lower: &mut Option<ScalarValue>,
    upper: &mut Option<ScalarValue>,
    value: &ScalarValue,
) {
    if lower
        .as_ref()
        .is_none_or(|current| test_scalar_order(value, current).is_some_and(|order| order.is_lt()))
    {
        *lower = Some(value.clone());
    }
    if upper
        .as_ref()
        .is_none_or(|current| test_scalar_order(value, current).is_some_and(|order| order.is_gt()))
    {
        *upper = Some(value.clone());
    }
}

fn test_scalar_order(left: &ScalarValue, right: &ScalarValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => Some(left.cmp(right)),
        (ScalarValue::Int32(left), ScalarValue::Int32(right)) => Some(left.cmp(right)),
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => Some(left.cmp(right)),
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => left.partial_cmp(right),
        (ScalarValue::Date32(left), ScalarValue::Date32(right)) => Some(left.cmp(right)),
        (ScalarValue::Utf8(left), ScalarValue::Utf8(right)) => Some(left.cmp(right)),
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
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use optd_core::{BoundedVec, MemoryCatalog, TableConstraints, UniqueKey};
    use std::sync::Arc;

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
    async fn full_scan_test_collector_builds_exact_stats_and_sketches_from_values() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 2, 4])),
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

        let collected = collect_full_scan_statistics(&session, "t", &["id", "name"])
            .await
            .unwrap();

        assert_eq!(collected.row_count, Some(4));
        assert_eq!(collected.column_statistics["id"].frequency, Some(4));
        assert_eq!(collected.column_statistics["id"].distinct, Some(3));
        assert_eq!(
            collected.column_statistics["id"].lower_bound,
            Some(ScalarValue::Int64(1))
        );
        assert_eq!(
            collected.column_statistics["id"].upper_bound,
            Some(ScalarValue::Int64(4))
        );
        assert_eq!(collected.column_statistics["name"].frequency, Some(3));

        let id_sketches = collected.column_statistics["id"].sketches.as_ref().unwrap();
        let hll_estimate = id_sketches.distinct_values.as_ref().unwrap().estimate();
        assert!((hll_estimate - 3.0).abs() < 1.0);
        let two = EncodedScalarValue::from_scalar(&ScalarValue::Int64(2)).unwrap();
        let frequent_two = id_sketches
            .frequent_values
            .as_ref()
            .unwrap()
            .estimate(&two)
            .unwrap();
        assert_eq!(frequent_two.frequency, 2);
        assert_eq!(frequent_two.error, 0);
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
