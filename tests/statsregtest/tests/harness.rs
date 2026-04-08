use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use datafusion::{
    arrow::{array::RecordBatch, datatypes::DataType as ArrowDataType},
    common::Column as DFColumn,
    functions_aggregate::expr_fn::{count, max, min},
    logical_expr::{Expr, LogicalPlanBuilder, expr_fn::ExprFunctionExt},
    prelude::lit,
    scalar::ScalarValue,
};
use optd_core::ir::{Operator, OperatorKind};
use optd_datafusion::DataFusionDB;
use optd_statsregtest::{
    QueryArtifact, RunConfig, apply_baselines,
    artifact::{collect_query_artifact, read_artifact, write_artifact},
    load_tpch_queries, run_against_baselines, split_sql_statements,
};
use tempfile::tempdir;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

async fn load_query(query_id: &str) -> Result<optd_statsregtest::BenchmarkQuery> {
    let workspace_root = workspace_root();
    let queries = load_tpch_queries(&workspace_root, &[query_id.to_string()])?;
    queries
        .into_iter()
        .next()
        .with_context(|| format!("missing query {query_id}"))
}

async fn setup_benchmark_db(
    query_id: &str,
) -> Result<(optd_statsregtest::BenchmarkQuery, DataFusionDB)> {
    let workspace_root = workspace_root();
    let query = load_query(query_id).await?;
    let db = DataFusionDB::new_with_advanced_cardinality().await?;
    for setup in query.harness_setup_sql(&workspace_root)? {
        for statement in split_sql_statements(&setup.sql) {
            db.execute_one(&statement).await?;
        }
    }
    Ok((query, db))
}

fn parse_count(result: &[RecordBatch]) -> Result<u64> {
    let batch = result.first().context("missing count batch")?;
    let scalar = ScalarValue::try_from_array(batch.column(0).as_ref(), 0)?;
    match scalar {
        ScalarValue::Int64(Some(value)) => Ok(value as u64),
        ScalarValue::UInt64(Some(value)) => Ok(value),
        other => anyhow::bail!("unexpected count value {other:?}"),
    }
}

fn total_rows(result: &[RecordBatch]) -> u64 {
    result.iter().map(|batch| batch.num_rows() as u64).sum()
}

fn find_first_node_by_kind(
    node: &Arc<Operator>,
    matches: impl Fn(&OperatorKind) -> bool + Copy,
) -> Option<Arc<Operator>> {
    if matches(&node.kind) {
        return Some(Arc::clone(node));
    }
    for input in node.input_operators() {
        if let Some(found) = find_first_node_by_kind(input, matches) {
            return Some(found);
        }
    }
    None
}

fn supports_count_distinct(data_type: &ArrowDataType) -> bool {
    !matches!(
        data_type,
        ArrowDataType::List(_)
            | ArrowDataType::LargeList(_)
            | ArrowDataType::FixedSizeList(_, _)
            | ArrowDataType::Struct(_)
            | ArrowDataType::Map(_, _)
            | ArrowDataType::Union(_, _)
    )
}

fn supports_min_max(data_type: &ArrowDataType) -> bool {
    !matches!(
        data_type,
        ArrowDataType::List(_)
            | ArrowDataType::LargeList(_)
            | ArrowDataType::FixedSizeList(_, _)
            | ArrowDataType::Struct(_)
            | ArrowDataType::Map(_, _)
            | ArrowDataType::Union(_, _)
    )
}

#[tokio::test]
async fn root_exact_row_count_matches_query_output_count() -> Result<()> {
    let workspace_root = workspace_root();
    let query = load_query("q1").await?;
    let artifact = collect_query_artifact(&query, &workspace_root).await?;

    let (_query, db) = setup_benchmark_db("q1").await?;
    let result_rows = db.execute(&query.sql).await?;
    assert_eq!(
        artifact.nodes[0].exact.row_count,
        Some(total_rows(&result_rows))
    );
    Ok(())
}

#[tokio::test]
async fn filter_node_exact_row_count_matches_manual_count() -> Result<()> {
    let workspace_root = workspace_root();
    let query = load_query("q6").await?;
    let artifact = collect_query_artifact(&query, &workspace_root).await?;
    let select_node = artifact
        .nodes
        .iter()
        .find(|node| node.operator_kind == "Select")
        .context("missing Select node")?;

    let (_query, db) = setup_benchmark_db("q6").await?;
    let expected = parse_count(
        &db.execute(
            r#"
            SELECT COUNT(*)
            FROM lineitem
            WHERE
                l_shipdate >= DATE '2023-01-01'
                AND l_shipdate < DATE '2024-01-01'
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
            "#,
        )
        .await?,
    )?;

    assert_eq!(select_node.exact.row_count, Some(expected));
    Ok(())
}

#[tokio::test]
async fn join_node_exact_row_count_matches_manual_count() -> Result<()> {
    let workspace_root = workspace_root();
    let query = load_query("q19").await?;
    let artifact = collect_query_artifact(&query, &workspace_root).await?;
    let join_node = artifact
        .nodes
        .iter()
        .find(|node| node.operator_kind == "Join")
        .context("missing Join node")?;

    let (_query, db) = setup_benchmark_db("q19").await?;
    let expected = parse_count(
        &db.execute(
            r#"
            SELECT COUNT(*)
            FROM lineitem, part
            WHERE
                (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#12'
                    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    AND l_quantity >= 1 AND l_quantity <= 11
                    AND p_size BETWEEN 1 AND 5
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                ) OR (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#23'
                    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    AND l_quantity >= 10 AND l_quantity <= 20
                    AND p_size BETWEEN 1 AND 10
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                ) OR (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#34'
                    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    AND l_quantity >= 20 AND l_quantity <= 30
                    AND p_size BETWEEN 1 AND 15
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
            "#,
        )
        .await?,
    )?;

    assert_eq!(join_node.exact.row_count, Some(expected));
    Ok(())
}

#[tokio::test]
async fn q6_get_probe_returns_single_aggregate_row() -> Result<()> {
    let (query, db) = setup_benchmark_db("q6").await?;
    let plan_artifacts = db.plan_sql_artifacts(&query.sql).await?;
    let get_node = find_first_node_by_kind(plan_artifacts.optd_physical_plan(), |kind| {
        matches!(kind, OperatorKind::Get(_))
    })
    .context("missing Get node")?;

    let logical_plan = plan_artifacts.subtree_logical_plan(&get_node)?;
    let schema = logical_plan.schema();
    let df_columns = schema.columns();
    let mut projection_exprs = Vec::<Expr>::new();
    let mut aggregate_exprs = Vec::<Expr>::new();

    aggregate_exprs.push(count(lit(1)).alias("row_count"));

    for (ordinal, column) in df_columns.iter().enumerate() {
        let (qualifier, field) = schema.qualified_field(ordinal);
        let projected_name = format!("c{ordinal}");
        let source_column = qualifier
            .map(ToString::to_string)
            .filter(|name| name.contains("__internal_#"))
            .map(|_| DFColumn::from_name(field.name().clone()))
            .unwrap_or_else(|| column.clone());
        projection_exprs.push(Expr::Column(source_column).alias(projected_name.clone()));

        let expr = Expr::Column(DFColumn::from_name(projected_name));
        aggregate_exprs.push(count(expr.clone()).alias(format!("c{ordinal}_nonnull")));
        if supports_count_distinct(field.data_type()) {
            aggregate_exprs.push(
                count(expr.clone())
                    .distinct()
                    .build()?
                    .alias(format!("c{ordinal}_ndv")),
            );
        }
        if supports_min_max(field.data_type()) {
            aggregate_exprs.push(min(expr.clone()).alias(format!("c{ordinal}_min")));
            aggregate_exprs.push(max(expr).alias(format!("c{ordinal}_max")));
        }
    }

    let projected_plan = LogicalPlanBuilder::from(logical_plan)
        .project(projection_exprs)?
        .build()?;
    let aggregate_plan = LogicalPlanBuilder::from(projected_plan)
        .aggregate(Vec::<Expr>::new(), aggregate_exprs)?
        .build()?;

    let batches = plan_artifacts.execute_logical_plan(aggregate_plan).await?;
    let total_output_rows = total_rows(&batches);
    assert_eq!(
        total_output_rows,
        1,
        "expected one aggregate row for q6 Get exact probe, found {total_output_rows} rows across {} batches",
        batches.len()
    );
    Ok(())
}

#[tokio::test]
async fn q6_get_probe_matches_manual_lineitem_count() -> Result<()> {
    let (query, db) = setup_benchmark_db("q6").await?;
    let plan_artifacts = db.plan_sql_artifacts(&query.sql).await?;
    let get_node = find_first_node_by_kind(plan_artifacts.optd_physical_plan(), |kind| {
        matches!(kind, OperatorKind::Get(_))
    })
    .context("missing Get node")?;

    let logical_plan = plan_artifacts.subtree_logical_plan(&get_node)?;
    let aggregate_plan = LogicalPlanBuilder::from(logical_plan)
        .aggregate(Vec::<Expr>::new(), vec![count(lit(1)).alias("row_count")])?
        .build()?;
    let batches = plan_artifacts.execute_logical_plan(aggregate_plan).await?;
    let exact_probe_count = parse_count(&batches)?;
    let manual_count = parse_count(&db.execute("SELECT COUNT(*) FROM lineitem").await?)?;

    assert_eq!(exact_probe_count, manual_count);
    Ok(())
}

#[tokio::test]
async fn q6_get_exact_row_count_matches_manual_lineitem_count() -> Result<()> {
    let workspace_root = workspace_root();
    let query = load_query("q6").await?;
    let artifact = collect_query_artifact(&query, &workspace_root).await?;
    let get_node = artifact
        .nodes
        .iter()
        .find(|node| node.operator_kind == "Get")
        .context("missing Get node in artifact")?;

    let (_query, db) = setup_benchmark_db("q6").await?;
    let manual_count = parse_count(&db.execute("SELECT COUNT(*) FROM lineitem").await?)?;

    assert_eq!(get_node.exact.row_count, Some(manual_count));
    Ok(())
}

#[tokio::test]
async fn apply_then_run_has_no_diff_for_same_query() -> Result<()> {
    let temp = tempdir()?;
    let mut config = RunConfig::new(workspace_root(), "tpch", vec!["q1".to_string()]);
    config.baseline_root = temp.path().join("baselines");
    config.output_root = temp.path().join("runs");

    let summary = apply_baselines(&config).await?;
    assert_eq!(summary.written.len(), 1);

    let report = run_against_baselines(&config).await?;
    assert!(report.missing_baselines.is_empty());
    assert_eq!(report.comparisons.len(), 1);
    let comparison = &report.comparisons[0];
    assert_eq!(comparison.added_nodes, 0);
    assert_eq!(comparison.removed_nodes, 0);
    assert_eq!(comparison.root.delta, Some(0.0));
    Ok(())
}

#[tokio::test]
async fn baseline_perturbation_is_reported() -> Result<()> {
    let temp = tempdir()?;
    let mut config = RunConfig::new(workspace_root(), "tpch", vec!["q1".to_string()]);
    config.baseline_root = temp.path().join("baselines");
    config.output_root = temp.path().join("runs");

    apply_baselines(&config).await?;
    let baseline_path = config.baseline_root.join("tpch/q1.json");
    let mut baseline: QueryArtifact = read_artifact(&baseline_path)?;
    baseline.nodes[0].error.row_count_q_error = baseline.nodes[0]
        .error
        .row_count_q_error
        .map(|value| value + 1.0);
    write_artifact(&baseline_path, &baseline)?;

    let report = run_against_baselines(&config).await?;
    assert_eq!(report.comparisons.len(), 1);
    assert!(report.comparisons[0].root.delta.unwrap_or(0.0) < 0.0);
    Ok(())
}
