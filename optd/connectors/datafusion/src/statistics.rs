use std::collections::BTreeMap;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{
    DataFusionError, Result as DFResult, ScalarValue as DFScalarValue, TableReference,
};
use datafusion::prelude::SessionContext;
use optd_core::{Catalog, ColumnStatistics, ScalarValue, TableRef, TableStatistics};

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
            },
        );
    }

    Ok(TableStatistics {
        row_count,
        size_bytes: None,
        column_statistics,
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
    let statistics = collect_table_statistics(session, table_name, columns).await?;
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
        DFScalarValue::Utf8(Some(value)) | DFScalarValue::LargeUtf8(Some(value)) => {
            Some(ScalarValue::Utf8(value))
        }
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
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
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
}
