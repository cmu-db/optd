//! Operator-subtree cardinality regression measurements and reports.
//!
//! Every reachable operator is treated as the root of a subtree. The harness compares the
//! optimizer's estimated row count with the exact row count for that subtree.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::physical_plan::execute_stream;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use optd_core::{
    CardinalityEstimationV1, FreeColumns, Operator, OperatorData, PlannedQuery, QueryContext,
    Relation,
};
use serde::{Deserialize, Serialize};

use crate::runner::{default_pass_manager, optimizer_context_from_logical_plan};
use crate::runtime_statistics::RuntimeStatisticsCatalogBuilder;
use crate::to_df_physical::to_physical_plan_for_row_count;

/// A named SQL query to measure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySpec {
    pub name: String,
    pub sql: String,
}

/// One estimated-versus-actual measurement for an operator subtree.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubtreeMeasurement {
    pub query: String,
    pub node_path: String,
    pub operator: String,
    /// Number of join-like operators in this subtree.
    ///
    /// Both predicate joins and cross products combine two relational inputs, so both contribute
    /// to this count.
    pub join_count: usize,
    pub estimated_rows: f64,
    pub actual_rows: u64,
    pub q_error: f64,
}

/// Runs cardinality-regression measurements against one DataFusion session.
///
/// A single runtime-statistics builder is shared by all measured queries so base-table statistics
/// are collected once per table/column projection.
pub struct CardinalityRegressionHarness {
    session: SessionContext,
    runtime_stats: RuntimeStatisticsCatalogBuilder,
}

impl CardinalityRegressionHarness {
    pub fn new(session: SessionContext) -> Self {
        Self {
            runtime_stats: RuntimeStatisticsCatalogBuilder::new(session.clone()),
            session,
        }
    }

    /// Plans and measures one query.
    pub async fn measure_query(
        &self,
        query_name: &str,
        sql: &str,
    ) -> Result<Vec<SubtreeMeasurement>, CardinalityRegressionError> {
        let logical = self
            .session
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|error| CardinalityRegressionError::DataFusion(error.to_string()))?;
        let optimizer =
            optimizer_context_from_logical_plan(&self.session, &self.runtime_stats, &logical)
                .await
                .map_err(|error| CardinalityRegressionError::Planning(error.to_string()))?;
        let planned = optimize_for_regression(optimizer)
            .map_err(|error| CardinalityRegressionError::Planning(error.to_string()))?;
        self.measure_planned_query(query_name, &planned).await
    }

    async fn measure_planned_query(
        &self,
        query_name: &str,
        planned: &PlannedQuery,
    ) -> Result<Vec<SubtreeMeasurement>, CardinalityRegressionError> {
        let root = planned.query.root().ok_or_else(|| {
            CardinalityRegressionError::Planning("optimized query has no root".into())
        })?;
        let mut nodes = Vec::new();
        collect_subtrees(root, &planned.query, "0".to_string(), &mut nodes);

        let mut analyses = planned.analyze();
        let mut exact_rows = std::collections::HashMap::<Operator, u64>::new();
        let mut measurements = Vec::with_capacity(nodes.len());
        for node in nodes {
            let free_columns = analyses
                .get::<FreeColumns>(&planned.query, node.operator)
                .map_err(|error| CardinalityRegressionError::Analysis(error.to_string()))?;
            if !free_columns.is_empty() {
                return Err(CardinalityRegressionError::Unsupported(format!(
                    "{} subtree {} has free columns and no context-independent row count",
                    query_name, node.path
                )));
            }
            let profile = analyses
                .get::<CardinalityEstimationV1>(&planned.query, node.operator)
                .map_err(|error| CardinalityRegressionError::Analysis(error.to_string()))?;
            let actual_rows = if let Some(rows) = exact_rows.get(&node.operator) {
                *rows
            } else {
                let rows = self
                    .exact_subtree_rows(planned, node.operator)
                    .await
                    .map_err(|error| {
                        CardinalityRegressionError::Execution(format!(
                            "{query_name} subtree {} ({}): {error}",
                            node.path,
                            operator_name(node.operator.get(&planned.query)),
                        ))
                    })?;
                exact_rows.insert(node.operator, rows);
                rows
            };
            let estimated_rows = profile.rows.value;
            let q_error = row_q_error(estimated_rows, actual_rows)?;
            measurements.push(SubtreeMeasurement {
                query: query_name.to_string(),
                node_path: node.path,
                operator: operator_name(node.operator.get(&planned.query)).to_string(),
                join_count: node.join_count,
                estimated_rows,
                actual_rows,
                q_error,
            });
        }
        Ok(measurements)
    }

    async fn exact_subtree_rows(
        &self,
        planned: &PlannedQuery,
        root: Operator,
    ) -> Result<u64, CardinalityRegressionError> {
        let mut query = planned.query.clone();
        query.set_root(root);
        let subtree = PlannedQuery::new(query, Arc::clone(&planned.catalog))
            .with_cardinality_estimation_config(planned.cardinality_estimation_config())
            .map_err(|error| CardinalityRegressionError::Analysis(error.to_string()))?;
        let physical = to_physical_plan_for_row_count(&subtree, &self.session)
            .await
            .map_err(|error| CardinalityRegressionError::Planning(error.to_string()))?;
        let state = self.session.state();
        let physical = DefaultPhysicalPlanner::default()
            .optimize_physical_plan(physical, &state, |_, _| {})
            .map_err(|error| CardinalityRegressionError::DataFusion(error.to_string()))?;
        let mut stream = execute_stream(physical, state.task_ctx())
            .map_err(|error| CardinalityRegressionError::DataFusion(error.to_string()))?;
        let mut rows = 0_u64;
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|error| CardinalityRegressionError::DataFusion(error.to_string()))?
        {
            let batch_rows = u64::try_from(batch.num_rows()).map_err(|_| {
                CardinalityRegressionError::Execution(
                    "record batch row count does not fit in u64".into(),
                )
            })?;
            rows = rows.checked_add(batch_rows).ok_or_else(|| {
                CardinalityRegressionError::Execution("subtree row count overflowed u64".into())
            })?;
        }
        Ok(rows)
    }
}

fn optimize_for_regression(
    mut optimizer: optd_core::OptimizerContext,
) -> Result<PlannedQuery, optd_core::OptimizeError> {
    let mut pass_manager = default_pass_manager();
    pass_manager.run(&mut optimizer)?;
    if let Some(root) = optimizer.query.root() {
        let resolved = optimizer.rewrites.resolve(root);
        optimizer.query.set_root(resolved);
    }
    Ok(optimizer.into_planned_query())
}

/// Loads `.sql` files or the first `query` block from `.slt` files.
pub fn load_query_specs(path: &Path) -> Result<Vec<QuerySpec>, CardinalityRegressionError> {
    let mut paths = if path.is_file() {
        vec![path.to_path_buf()]
    } else {
        let mut paths = Vec::new();
        for entry in fs::read_dir(path).map_err(CardinalityRegressionError::Io)? {
            let path = entry.map_err(CardinalityRegressionError::Io)?.path();
            if matches!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("sql" | "slt")
            ) {
                paths.push(path);
            }
        }
        paths
    };
    paths.sort_by_key(|left| natural_query_key(left));

    let mut queries = Vec::with_capacity(paths.len());
    for path in paths {
        let text = fs::read_to_string(&path).map_err(CardinalityRegressionError::Io)?;
        let sql = match path.extension().and_then(|ext| ext.to_str()) {
            Some("slt") => first_slt_query(&text).ok_or_else(|| {
                CardinalityRegressionError::Input(format!(
                    "{} contains no query block",
                    path.display()
                ))
            })?,
            _ => text.trim().trim_end_matches(';').to_string(),
        };
        if sql.trim().is_empty() {
            return Err(CardinalityRegressionError::Input(format!(
                "{} contains an empty query",
                path.display()
            )));
        }
        let name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| {
                CardinalityRegressionError::Input("query file has no UTF-8 stem".into())
            })?
            .to_string();
        queries.push(QuerySpec { name, sql });
    }
    if queries.is_empty() {
        return Err(CardinalityRegressionError::Input(format!(
            "{} contains no .sql or .slt query files",
            path.display()
        )));
    }
    Ok(queries)
}

/// Writes raw measurements to `report.json` and returns its path.
pub fn write_report(
    measurements: &[SubtreeMeasurement],
    output_dir: &Path,
) -> Result<PathBuf, CardinalityRegressionError> {
    fs::create_dir_all(output_dir).map_err(CardinalityRegressionError::Io)?;
    let path = output_dir.join("report.json");
    let json = serde_json::to_string_pretty(measurements)
        .map_err(|error| CardinalityRegressionError::Serialization(error.to_string()))?;
    fs::write(&path, json).map_err(CardinalityRegressionError::Io)?;
    Ok(path)
}

/// Q-error with the conventional one-row floor.
///
/// The floor makes empty-result errors observable and finite: estimating zero for ten actual rows
/// has q-error 10, while zero versus zero has q-error 1.
pub fn row_q_error(
    estimated_rows: f64,
    actual_rows: u64,
) -> Result<f64, CardinalityRegressionError> {
    if !estimated_rows.is_finite() || estimated_rows < 0.0 {
        return Err(CardinalityRegressionError::Analysis(format!(
            "invalid row estimate {estimated_rows}"
        )));
    }
    let estimated = estimated_rows.max(1.0);
    let actual = (actual_rows as f64).max(1.0);
    Ok(estimated.max(actual) / estimated.min(actual))
}

#[derive(Debug)]
pub enum CardinalityRegressionError {
    Input(String),
    Planning(String),
    Analysis(String),
    Unsupported(String),
    DataFusion(String),
    Execution(String),
    Serialization(String),
    Io(std::io::Error),
}

impl std::fmt::Display for CardinalityRegressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Input(message) => write!(f, "invalid harness input: {message}"),
            Self::Planning(message) => write!(f, "subtree planning failed: {message}"),
            Self::Analysis(message) => write!(f, "cardinality analysis failed: {message}"),
            Self::Unsupported(message) => write!(f, "subtree cannot be measured: {message}"),
            Self::DataFusion(message) => write!(f, "DataFusion failed: {message}"),
            Self::Execution(message) => write!(f, "subtree execution failed: {message}"),
            Self::Serialization(message) => write!(f, "report serialization failed: {message}"),
            Self::Io(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for CardinalityRegressionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct SubtreeNode {
    operator: Operator,
    path: String,
    join_count: usize,
}

fn collect_subtrees(
    operator: Operator,
    ctx: &QueryContext,
    path: String,
    out: &mut Vec<SubtreeNode>,
) -> usize {
    let output_index = out.len();
    out.push(SubtreeNode {
        operator,
        path: path.clone(),
        join_count: 0,
    });
    let operator_data = operator.get(ctx);
    let mut join_count = usize::from(matches!(
        operator_data,
        OperatorData::Join(_) | OperatorData::CrossProduct(_)
    ));
    for (index, input) in operator_data.inputs().into_iter().enumerate() {
        join_count += collect_subtrees(input, ctx, format!("{path}.{index}"), out);
    }
    out[output_index].join_count = join_count;
    join_count
}

fn operator_name(operator: &OperatorData) -> &'static str {
    match operator {
        OperatorData::Scan(_) => "scan",
        OperatorData::ConstScan(_) => "const_scan",
        OperatorData::TableFunction(_) => "table_function",
        OperatorData::Selection(_) => "selection",
        OperatorData::Projection(_) => "projection",
        OperatorData::Output(_) => "output",
        OperatorData::Sort(_) => "sort",
        OperatorData::Limit(_) => "limit",
        OperatorData::Rename(_) => "rename",
        OperatorData::Map(_) => "map",
        OperatorData::Aggregation(_) => "aggregation",
        OperatorData::CrossProduct(_) => "cross_product",
        OperatorData::Join(_) => "join",
    }
}

fn first_slt_query(text: &str) -> Option<String> {
    let mut in_query = false;
    let mut lines = Vec::new();
    for line in text.lines() {
        if in_query && line.trim() == "----" {
            break;
        }
        if in_query {
            lines.push(line);
        } else if line.starts_with("query ") || line == "query" {
            in_query = true;
        }
    }
    in_query.then(|| lines.join("\n").trim().trim_end_matches(';').to_string())
}

fn natural_query_key(path: &Path) -> (u64, String) {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default();
    let digits = stem
        .trim_start_matches(|character: char| !character.is_ascii_digit())
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    (
        digits.parse().unwrap_or(u64::MAX),
        stem.to_ascii_lowercase(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use optd_core::{CrossProduct, Scan, TableRef};

    use super::*;

    #[test]
    fn q_error_keeps_zero_mismatches_visible() {
        assert_eq!(row_q_error(0.0, 0).expect("test setup should succeed"), 1.0);
        assert_eq!(
            row_q_error(0.0, 10).expect("test setup should succeed"),
            10.0
        );
        assert_eq!(row_q_error(4.0, 2).expect("test setup should succeed"), 2.0);
        assert!(row_q_error(f64::NAN, 1).is_err());
        assert!(row_q_error(-1.0, 1).is_err());
    }

    #[test]
    fn slt_loader_extracts_only_query_text() {
        let sql = first_slt_query("# comment\nquery I\nselect count(*) from t;\n----\n1\n")
            .expect("test setup should succeed");
        assert_eq!(sql, "select count(*) from t");
    }

    #[test]
    fn subtree_join_count_includes_cross_products_and_accumulates_recursively() {
        let mut ctx = QueryContext::new();
        let left = OperatorData::Scan(Scan {
            table: TableRef::bare("left"),
            columns: Vec::new(),
        })
        .add(&mut ctx);
        let middle = OperatorData::Scan(Scan {
            table: TableRef::bare("middle"),
            columns: Vec::new(),
        })
        .add(&mut ctx);
        let right = OperatorData::Scan(Scan {
            table: TableRef::bare("right"),
            columns: Vec::new(),
        })
        .add(&mut ctx);
        let lower = OperatorData::CrossProduct(CrossProduct {
            outer: left,
            inner: middle,
        })
        .add(&mut ctx);
        let root = OperatorData::CrossProduct(CrossProduct {
            outer: lower,
            inner: right,
        })
        .add(&mut ctx);

        let mut nodes = Vec::new();
        assert_eq!(collect_subtrees(root, &ctx, "0".into(), &mut nodes), 2);
        assert_eq!(nodes[0].join_count, 2);
        assert_eq!(nodes[1].join_count, 1);
        assert!(nodes[2..].iter().all(|node| node.join_count == 0));
    }

    #[tokio::test]
    async fn harness_measures_every_input_edge_reachable_subtree() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 2, 4]))],
        )
        .expect("test setup should succeed");
        let table =
            MemTable::try_new(schema, vec![vec![batch]]).expect("test setup should succeed");
        session
            .register_table("t", Arc::new(table))
            .expect("test setup should succeed");

        let harness = CardinalityRegressionHarness::new(session);
        let measurements = harness
            .measure_query("synthetic", "SELECT id FROM t WHERE id = 2")
            .await
            .expect("test setup should succeed");

        assert!(!measurements.is_empty());
        assert_eq!(measurements[0].node_path, "0");
        assert!(measurements.iter().all(|row| row.join_count == 0));
        assert!(measurements.iter().all(|row| row.q_error >= 1.0));
        assert!(
            measurements
                .iter()
                .any(|row| row.operator == "selection" && row.actual_rows == 2)
        );
        assert!(
            measurements
                .iter()
                .any(|row| row.operator == "scan" && row.actual_rows == 4)
        );
    }

    #[tokio::test]
    async fn harness_measures_branching_join_and_empty_aggregate() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for (name, values) in [("l", vec![1, 2, 2]), ("r", vec![2, 2, 3])] {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
                    .expect("test setup should succeed");
            let table = MemTable::try_new(schema.clone(), vec![vec![batch]])
                .expect("test setup should succeed");
            session
                .register_table(name, Arc::new(table))
                .expect("test setup should succeed");
        }

        let harness = CardinalityRegressionHarness::new(session);
        let join_measurements = harness
            .measure_query("join", "SELECT l.id FROM l JOIN r ON l.id = r.id")
            .await
            .expect("test setup should succeed");
        assert_eq!(join_measurements[0].join_count, 1);
        assert!(
            join_measurements
                .iter()
                .any(|row| row.operator == "join" && row.join_count == 1 && row.actual_rows == 4)
        );
        assert_eq!(
            join_measurements
                .iter()
                .filter(|row| {
                    row.operator == "scan" && row.join_count == 0 && row.actual_rows == 3
                })
                .count(),
            2
        );

        let aggregate_measurements = harness
            .measure_query("aggregate", "SELECT COUNT(*) FROM l WHERE id > 100")
            .await
            .expect("test setup should succeed");
        assert!(
            aggregate_measurements
                .iter()
                .any(|row| row.operator == "selection" && row.actual_rows == 0)
        );
        assert!(
            aggregate_measurements
                .iter()
                .any(|row| row.operator == "aggregation" && row.actual_rows == 1)
        );
    }
}
