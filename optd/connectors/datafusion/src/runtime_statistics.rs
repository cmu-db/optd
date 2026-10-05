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
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use optd_core::{Catalog, MemoryCatalog, TableRef, TableStatistics};

use crate::statistics::{collect_full_scan_statistics, collect_table_statistics_for_table_ref};
#[cfg(test)]
use optd_core::TableConstraints;

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
    #[cfg(test)]
    constraints: HashMap<TableRef, TableConstraints>,
    full_scan_sketches: bool,
}

impl RuntimeStatisticsCatalogBuilder {
    pub(crate) fn new(session: SessionContext) -> Self {
        Self {
            session,
            cache: tokio::sync::Mutex::new(HashMap::new()),
            #[cfg(test)]
            constraints: HashMap::new(),
            full_scan_sketches: false,
        }
    }

    /// Adds provider-asserted constraints for an explicit catalog fixture.
    ///
    /// These assertions are never derived from runtime statistics, sampled values, or NDV.
    #[cfg(test)]
    pub(crate) fn with_table_constraints(
        mut self,
        table: TableRef,
        constraints: TableConstraints,
    ) -> Self {
        self.constraints.insert(table, constraints);
        self
    }

    /// Enables unbounded full-table sketch collection for an explicit regression or test run.
    ///
    /// Normal query planning remains aggregate-only. This mode deliberately materializes each
    /// referenced table, so callers must opt into its benchmark-only cost.
    pub(crate) fn with_full_scan_sketches(mut self) -> Self {
        self.full_scan_sketches = true;
        self
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

            let statistics = if self.full_scan_sketches {
                let columns = scan.columns.iter().map(String::as_str).collect::<Vec<_>>();
                let statistics = collect_full_scan_statistics(&self.session, &scan.table, &columns)
                    .await
                    .map_err(|e| e.to_string())?;
                let hll_columns = statistics
                    .column_statistics
                    .values()
                    .filter(|column| {
                        column
                            .sketches
                            .as_ref()
                            .is_some_and(|sketch| sketch.distinct_values.is_some())
                    })
                    .count();
                let space_saving_columns = statistics
                    .column_statistics
                    .values()
                    .filter(|column| {
                        column
                            .sketches
                            .as_ref()
                            .is_some_and(|sketch| sketch.frequent_values.is_some())
                    })
                    .count();
                eprintln!(
                    "runtime statistics: full scan table={table_ref:?} rows={:?} columns={} hll_columns={hll_columns} spacesaving_columns={space_saving_columns}",
                    statistics.row_count,
                    statistics.column_statistics.len(),
                );
                statistics
            } else {
                self.cached_table_statistics(&scan.table, provider.as_ref(), &scan.columns)
                    .await?
            };

            #[cfg(test)]
            let statistics = {
                let mut statistics = statistics;
                statistics.constraints = self
                    .constraints
                    .get(&table_ref)
                    .cloned()
                    .unwrap_or_default();
                statistics
            };
            for (column, column_statistics) in &statistics.column_statistics {
                if let Some(sketch) = &column_statistics.sketches {
                    eprintln!(
                        "runtime statistics: catalog sketch table={table_ref:?} column={column} population_rows={} hll={} spacesaving={}",
                        sketch.population_rows,
                        sketch.distinct_values.is_some(),
                        sketch.frequent_values.is_some(),
                    );
                }
            }
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
    plan.apply_with_subqueries(|plan| {
        collect_referenced_scan(plan, &mut scans);
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("collecting table scans cannot fail");
    scans.into_values().collect()
}

fn collect_referenced_scan(plan: &LogicalPlan, out: &mut BTreeMap<String, ReferencedScan>) {
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
    use crate::runner::optimizer_context_from_logical_plan;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use optd_core::{
        BoundedVec, CardinalityEstimationV1, ForeignKey, ScalarValue, TableConstraints, TableRef,
        UniqueKey,
    };
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
    async fn runtime_statistics_installs_real_hll_with_hll() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 2, 4]))],
        )
        .unwrap();
        session
            .register_table(
                "t",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        let plan = session
            .state()
            .create_logical_plan("SELECT id FROM t")
            .await
            .unwrap();
        let builder =
            RuntimeStatisticsCatalogBuilder::new(session.clone()).with_full_scan_sketches();

        let mut optimizer = optimizer_context_from_logical_plan(&session, &builder, &plan)
            .await
            .unwrap();
        let installed = optimizer
            .analyses
            .catalog()
            .table_by_ref(&TableRef::bare("t"))
            .unwrap()
            .statistics
            .unwrap()
            .column_statistics["id"]
            .sketches
            .clone()
            .unwrap();
        assert_eq!(installed.population_rows, 4);
        assert!((installed.distinct_values.as_ref().unwrap().estimate() - 3.0).abs() < 1.0);
        let root = optimizer.query.root().unwrap();
        let profile = optimizer
            .analyses
            .get::<CardinalityEstimationV1>(&optimizer.query, root)
            .unwrap();
        assert!(
            profile
                .columns
                .values()
                .any(|column| column.sketches.is_some())
        );
        assert!(
            profile
                .columns
                .values()
                .any(|column| { column.distinct.source == optd_core::EstimateSource::Sketch })
        );
    }

    #[tokio::test]
    async fn runtime_statistics_installs_real_spacesaving_with_spacesaving() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(2),
                Some(2),
                None,
            ]))],
        )
        .unwrap();
        session
            .register_table(
                "t",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        let plan = session
            .state()
            .create_logical_plan("SELECT id FROM t")
            .await
            .unwrap();
        let builder =
            RuntimeStatisticsCatalogBuilder::new(session.clone()).with_full_scan_sketches();

        let mut optimizer = optimizer_context_from_logical_plan(&session, &builder, &plan)
            .await
            .unwrap();
        let installed = optimizer
            .analyses
            .catalog()
            .table_by_ref(&TableRef::bare("t"))
            .unwrap()
            .statistics
            .unwrap()
            .column_statistics["id"]
            .sketches
            .clone()
            .unwrap();
        let two = optd_core::EncodedScalarValue::from_scalar(&ScalarValue::Int64(2)).unwrap();
        assert_eq!(installed.population_rows, 5);
        assert_eq!(
            installed
                .frequent_values
                .as_ref()
                .unwrap()
                .estimate(&two)
                .unwrap()
                .frequency,
            3
        );
        let root = optimizer.query.root().unwrap();
        let profile = optimizer
            .analyses
            .get::<CardinalityEstimationV1>(&optimizer.query, root)
            .unwrap();
        assert!(
            profile
                .columns
                .values()
                .any(|column| column.sketches.is_some())
        );
    }

    #[tokio::test]
    async fn runtime_statistics_installs_asserted_constraints_with_primary_key() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        session
            .register_table(
                "t",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        let plan = session
            .state()
            .create_logical_plan("SELECT id FROM t")
            .await
            .unwrap();
        let constraints = TableConstraints {
            unique_keys: BoundedVec::try_new(vec![UniqueKey::try_new(vec!["id".into()]).unwrap()])
                .unwrap(),
            foreign_keys: BoundedVec::default(),
        };
        let builder = RuntimeStatisticsCatalogBuilder::new(session.clone())
            .with_table_constraints(TableRef::bare("t"), constraints.clone());

        let mut optimizer = optimizer_context_from_logical_plan(&session, &builder, &plan)
            .await
            .unwrap();
        assert_eq!(
            optimizer
                .analyses
                .catalog()
                .table_by_ref(&TableRef::bare("t"))
                .unwrap()
                .statistics
                .unwrap()
                .constraints,
            constraints
        );
        let root = optimizer.query.root().unwrap();
        assert_eq!(
            optimizer
                .analyses
                .get::<CardinalityEstimationV1>(&optimizer.query, root)
                .unwrap()
                .rows
                .value,
            2.0
        );
    }

    #[tokio::test]
    async fn runtime_statistics_installs_asserted_constraints_with_foreign_key() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let parent =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let child = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
        )
        .unwrap();
        session
            .register_table(
                "parent",
                Arc::new(MemTable::try_new(schema.clone(), vec![vec![parent]]).unwrap()),
            )
            .unwrap();
        session
            .register_table(
                "child",
                Arc::new(MemTable::try_new(schema, vec![vec![child]]).unwrap()),
            )
            .unwrap();
        let plan = session
            .state()
            .create_logical_plan("SELECT child.id FROM child JOIN parent ON child.id = parent.id")
            .await
            .unwrap();
        let parent_constraints = TableConstraints {
            unique_keys: BoundedVec::try_new(vec![UniqueKey::try_new(vec!["id".into()]).unwrap()])
                .unwrap(),
            foreign_keys: BoundedVec::default(),
        };
        let child_constraints = TableConstraints {
            unique_keys: BoundedVec::default(),
            foreign_keys: BoundedVec::try_new(vec![
                ForeignKey::try_new(
                    vec!["id".into()],
                    TableRef::bare("parent"),
                    vec!["id".into()],
                )
                .unwrap(),
            ])
            .unwrap(),
        };
        let builder = RuntimeStatisticsCatalogBuilder::new(session.clone())
            .with_table_constraints(TableRef::bare("parent"), parent_constraints)
            .with_table_constraints(TableRef::bare("child"), child_constraints.clone());

        let mut optimizer = optimizer_context_from_logical_plan(&session, &builder, &plan)
            .await
            .unwrap();
        assert_eq!(
            optimizer
                .analyses
                .catalog()
                .table_by_ref(&TableRef::bare("child"))
                .unwrap()
                .statistics
                .unwrap()
                .constraints,
            child_constraints
        );
        let root = optimizer.query.root().unwrap();
        assert_eq!(
            optimizer
                .analyses
                .get::<CardinalityEstimationV1>(&optimizer.query, root)
                .unwrap()
                .rows
                .value,
            3.0
        );
    }

    #[tokio::test]
    async fn full_scan_runtime_statistics_uses_qualified_table_reference() {
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
        let builder =
            RuntimeStatisticsCatalogBuilder::new(session.clone()).with_full_scan_sketches();
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
        assert!(stats.column_statistics["id"].sketches.is_some());
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

    #[tokio::test]
    async fn runtime_statistics_catalog_includes_scans_inside_subqueries() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        for name in ["outer_t", "middle_t", "inner_t"] {
            session.register_table(name, table.clone()).unwrap();
        }
        let plan = session
            .state()
            .create_logical_plan(
                "SELECT id FROM outer_t WHERE id IN (\
                 SELECT id FROM middle_t WHERE id > (SELECT max(id) FROM inner_t)\
                 )",
            )
            .await
            .unwrap();
        let builder = RuntimeStatisticsCatalogBuilder::new(session);

        let catalog = builder.build_for_plan(&plan).await.unwrap();

        for name in ["outer_t", "middle_t", "inner_t"] {
            assert!(catalog.table_by_ref(&TableRef::bare(name)).is_ok());
        }
        assert_eq!(builder.cache_len().await, 3);
    }
}
