//! Query-local runtime statistics for optd planning.
//!
//! This module collects table and column statistics from DataFusion and stores
//! them in an optd catalog. It does not collect selectivities directly:
//! cardinality analysis derives filter and join selectivities later from row
//! count, non-null frequency, NDV, min, and max.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use optd_core::{Catalog, MemoryCatalog, TableRef, TableStatistics};

use crate::statistics::collect_table_statistics_for_table_ref;

type RuntimeStatisticsResult<T> = Result<T, String>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct StatsCacheKey {
    table: String,
    columns: Vec<String>,
}

#[derive(Debug, Clone)]
struct ReferencedScan {
    table: TableReference,
    columns: BTreeSet<String>,
}

/// Builds an optd catalog with runtime-collected stats for one DataFusion plan.
pub(crate) struct RuntimeStatisticsCatalogBuilder {
    session: SessionContext,
    cache: tokio::sync::Mutex<HashMap<StatsCacheKey, TableStatistics>>,
}

impl RuntimeStatisticsCatalogBuilder {
    pub(crate) fn new(session: SessionContext) -> Self {
        Self {
            session,
            cache: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn build_for_plan(
        &self,
        plan: &LogicalPlan,
    ) -> RuntimeStatisticsResult<Arc<dyn Catalog>> {
        let scans = referenced_scan_columns(plan);
        let catalog = Arc::new(MemoryCatalog::new("datafusion", "public"));
        for scan in scans {
            let provider = self
                .session
                .table_provider(scan.table.clone())
                .await
                .map_err(|e| e.to_string())?;
            let table_ref = table_ref_from_df(&scan.table);
            catalog
                .create_table(table_ref.clone(), provider.schema(), None)
                .map_err(|e| e.to_string())?;
            let statistics = self
                .cached_table_statistics(&scan.table, provider.as_ref(), &scan.columns)
                .await?;
            catalog
                .set_table_statistics(table_ref, statistics)
                .map_err(|e| e.to_string())?;
        }
        Ok(catalog)
    }

    #[cfg(test)]
    pub(crate) async fn cache_len(&self) -> usize {
        self.cache.lock().await.len()
    }

    async fn cached_table_statistics(
        &self,
        table: &TableReference,
        provider: &dyn TableProvider,
        columns: &BTreeSet<String>,
    ) -> RuntimeStatisticsResult<TableStatistics> {
        let columns = columns.iter().cloned().collect::<Vec<_>>();
        let key = StatsCacheKey {
            table: table.to_string(),
            columns: columns.clone(),
        };
        if let Some(statistics) = self.cache.lock().await.get(&key).cloned() {
            return Ok(statistics);
        }

        let schema = provider.schema();
        let available_columns = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<BTreeSet<_>>();
        let columns = columns
            .iter()
            .filter(|column| available_columns.contains(column.as_str()))
            .map(String::as_str)
            .collect::<Vec<_>>();
        let statistics = collect_table_statistics_for_table_ref(&self.session, table, &columns)
            .await
            .map_err(|e| e.to_string())?;
        self.cache.lock().await.insert(key, statistics.clone());
        Ok(statistics)
    }
}

fn referenced_scan_columns(plan: &LogicalPlan) -> Vec<ReferencedScan> {
    let mut scans = BTreeMap::<String, ReferencedScan>::new();
    collect_referenced_scan_columns(plan, &mut scans);
    scans.into_values().collect()
}

fn collect_referenced_scan_columns(plan: &LogicalPlan, out: &mut BTreeMap<String, ReferencedScan>) {
    if let LogicalPlan::TableScan(scan) = plan {
        let key = scan.table_name.to_string();
        let entry = out.entry(key).or_insert_with(|| ReferencedScan {
            table: scan.table_name.clone(),
            columns: BTreeSet::new(),
        });
        for field in scan.projected_schema.fields() {
            entry.columns.insert(field.name().clone());
        }
    }
    for input in plan.inputs() {
        collect_referenced_scan_columns(input, out);
    }
}

fn table_ref_from_df(table: &TableReference) -> TableRef {
    match (table.catalog(), table.schema()) {
        (Some(catalog), Some(schema)) => TableRef::full(catalog, schema, table.table()),
        (None, Some(schema)) => TableRef::partial(schema, table.table()),
        _ => TableRef::bare(table.table()),
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeStatisticsCatalogBuilder;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use optd_core::{ScalarValue, TableRef};
    use std::sync::Arc;

    #[tokio::test]
    async fn runtime_statistics_catalog_populates_and_caches_scan_stats() {
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
        let builder = RuntimeStatisticsCatalogBuilder::new(session.clone());
        let plan = session
            .state()
            .create_logical_plan("SELECT id FROM t WHERE id >= 2")
            .await
            .unwrap();

        let catalog = builder.build_for_plan(&plan).await.unwrap();
        let stats = catalog
            .table_by_ref(&TableRef::bare("t"))
            .unwrap()
            .statistics
            .unwrap();

        assert_eq!(stats.row_count, Some(4));
        assert_eq!(stats.column_statistics["id"].distinct, Some(3));
        assert_eq!(
            stats.column_statistics["id"].lower_bound,
            Some(ScalarValue::Int64(1))
        );
        assert_eq!(builder.cache_len().await, 1);

        let _ = builder.build_for_plan(&plan).await.unwrap();
        assert_eq!(builder.cache_len().await, 1);
    }

    #[tokio::test]
    async fn runtime_statistics_catalog_uses_qualified_table_reference() {
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
            vec![Arc::new(Int64Array::from(vec![10, 20, 20]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        session
            .register_table(TableReference::partial("s", "t"), Arc::new(table))
            .unwrap();
        let builder = RuntimeStatisticsCatalogBuilder::new(session.clone());
        let plan = session
            .state()
            .create_logical_plan("SELECT id FROM s.t")
            .await
            .unwrap();

        let catalog = builder.build_for_plan(&plan).await.unwrap();
        let stats = catalog
            .table_by_ref(&TableRef::partial("s", "t"))
            .unwrap()
            .statistics
            .unwrap();

        assert_eq!(stats.row_count, Some(3));
        assert_eq!(stats.column_statistics["id"].distinct, Some(2));
    }
}
