use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result};
use datafusion::{
    arrow::datatypes::DataType as ArrowDataType,
    common::Column as DFColumn,
    functions_aggregate::expr_fn::{count, max, min},
    logical_expr::{Expr, LogicalPlanBuilder, expr_fn::ExprFunctionExt},
    prelude::lit,
    scalar::ScalarValue,
};
use optd_core::ir::{Operator, OperatorKind, explain::quick_explain, operator::Get};
use optd_datafusion::{DataFusionDB, OptdPlanArtifacts};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::manifest::{BenchmarkQuery, split_sql_statements};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryArtifact {
    pub suite: String,
    pub query_id: String,
    pub desc: Option<String>,
    pub sql_hash: String,
    pub dataset_fingerprint: String,
    pub plan_hash: String,
    pub nodes: Vec<NodeArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeArtifact {
    pub node_path: String,
    pub operator_kind: String,
    pub short_explain: String,
    pub subtree_fingerprint: String,
    pub output_schema: Vec<OutputColumn>,
    pub estimated: EstimatedNodeStats,
    pub exact: ExactNodeStats,
    pub error: NodeErrorMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutputColumn {
    pub ordinal: usize,
    pub relation: String,
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EstimatedNodeStats {
    pub row_count: Option<f64>,
    pub columns: Vec<EstimatedColumnStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExactNodeStats {
    pub row_count: Option<u64>,
    pub columns: Vec<ExactColumnStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EstimatedColumnStats {
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExactColumnStats {
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeErrorMetrics {
    pub row_count_q_error: Option<f64>,
    pub columns: Vec<ColumnErrorMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnErrorMetrics {
    pub distinct_count_q_error: Option<f64>,
    pub null_fraction_abs_error: Option<f64>,
    pub min_value_match: Option<bool>,
    pub max_value_match: Option<bool>,
}

pub async fn collect_query_artifact(
    query: &BenchmarkQuery,
    workspace_root: &Path,
) -> Result<QueryArtifact> {
    let db = DataFusionDB::new_with_advanced_cardinality().await?;
    for setup in query.harness_setup_sql(workspace_root)? {
        for statement in split_sql_statements(&setup.sql) {
            db.execute_one(&statement)
                .await
                .with_context(|| format!("failed to execute setup from {}", setup.source))?;
        }
    }

    let plan_artifacts = db
        .plan_sql_artifacts(&query.sql)
        .await
        .with_context(|| format!("failed to plan {}", query.query_id))?;

    let dataset_fingerprint = hash_path_contents(&workspace_root.join("data/tpch"))?;
    let sql_hash = hash_string(&query.sql);
    let mut exact_cache = HashMap::<String, ExactNodeStats>::new();
    let nodes = collect_nodes(&plan_artifacts, &mut exact_cache).await?;
    let plan_hash = nodes
        .first()
        .map(|node| node.subtree_fingerprint.clone())
        .unwrap_or_else(|| hash_string(""));

    Ok(QueryArtifact {
        suite: query.suite.clone(),
        query_id: query.query_id.clone(),
        desc: query.desc.clone(),
        sql_hash,
        dataset_fingerprint,
        plan_hash,
        nodes,
    })
}

pub fn write_artifact(path: &Path, artifact: &QueryArtifact) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let payload = serde_json::to_string_pretty(artifact)?;
    fs::write(path, payload).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

pub fn read_artifact(path: &Path) -> Result<QueryArtifact> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

async fn collect_nodes(
    plan_artifacts: &OptdPlanArtifacts,
    exact_cache: &mut HashMap<String, ExactNodeStats>,
) -> Result<Vec<NodeArtifact>> {
    let mut nodes = Vec::new();
    let mut stack = vec![(
        plan_artifacts.optd_physical_plan().clone(),
        String::from("0"),
    )];
    while let Some((node, node_path)) = stack.pop() {
        let artifact =
            collect_node_artifact(plan_artifacts, &node, &node_path, exact_cache).await?;
        for (child_index, child) in node.input_operators().iter().enumerate().rev() {
            stack.push((child.clone(), format!("{node_path}.{child_index}")));
        }
        nodes.push(artifact);
    }
    Ok(nodes)
}

async fn collect_node_artifact(
    plan_artifacts: &OptdPlanArtifacts,
    node: &Arc<Operator>,
    node_path: &str,
    exact_cache: &mut HashMap<String, ExactNodeStats>,
) -> Result<NodeArtifact> {
    let ctx = plan_artifacts.ir_context();
    let explain = quick_explain(node, ctx);
    let normalized_explain = normalize_explain(&explain);
    let output_schema = build_output_schema(node, ctx)?;
    let estimated = build_estimated_stats(node, ctx, &output_schema)?;
    let subtree_fingerprint = hash_string(&normalized_explain);
    let exact = if let Some(cached) = exact_cache.get(&subtree_fingerprint) {
        cached.clone()
    } else {
        let computed = build_exact_stats(plan_artifacts, node, &output_schema)
            .await
            .with_context(|| {
                format!(
                    "failed exact stats for node_path={node_path} kind={} explain={}",
                    operator_kind_name(node),
                    explain.lines().next().unwrap_or_default().trim()
                )
            })?;
        exact_cache.insert(subtree_fingerprint.clone(), computed.clone());
        computed
    };
    let error = build_error_metrics(&estimated, &exact);

    Ok(NodeArtifact {
        node_path: node_path.to_string(),
        operator_kind: operator_kind_name(node),
        short_explain: explain
            .lines()
            .next()
            .map(str::trim)
            .unwrap_or_default()
            .to_string(),
        subtree_fingerprint,
        output_schema,
        estimated,
        exact,
        error,
    })
}

fn build_output_schema(
    node: &Operator,
    ctx: &optd_core::ir::IRContext,
) -> Result<Vec<OutputColumn>> {
    let schema = node.output_schema(ctx)?;
    Ok(schema
        .iter()
        .enumerate()
        .map(|(ordinal, (table_ref, field))| OutputColumn {
            ordinal,
            relation: table_ref.to_string(),
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        })
        .collect())
}

fn build_estimated_stats(
    node: &Operator,
    ctx: &optd_core::ir::IRContext,
    output_schema: &[OutputColumn],
) -> Result<EstimatedNodeStats> {
    let mut columns = vec![
        EstimatedColumnStats {
            null_count: None,
            distinct_count: None,
            min_value: None,
            max_value: None,
        };
        output_schema.len()
    ];

    if let OperatorKind::Get(meta) = &node.kind {
        let get = Get::borrow_raw_parts(meta, &node.common);
        if let Ok(table_meta) = ctx.catalog.table(*get.data_source_id())
            && let Some(table_stats) = table_meta.statistics
        {
            for (ordinal, projected_index) in get.projections().iter().enumerate() {
                if let Some(col_stats) = table_stats.column_statistics.get(*projected_index)
                    && let Some(slot) = columns.get_mut(ordinal)
                {
                    slot.null_count = col_stats.null_count.map(|value| value as u64);
                    slot.distinct_count = col_stats.distinct_count.map(|value| value as u64);
                    slot.min_value = col_stats.min_value.clone();
                    slot.max_value = col_stats.max_value.clone();
                }
            }
        }
    }

    Ok(EstimatedNodeStats {
        row_count: Some(node.cardinality(ctx).as_f64()),
        columns,
    })
}

async fn build_exact_stats(
    plan_artifacts: &OptdPlanArtifacts,
    node: &Arc<Operator>,
    output_schema: &[OutputColumn],
) -> Result<ExactNodeStats> {
    let logical_plan = plan_artifacts
        .subtree_logical_plan(node)
        .context("failed to rebuild subtree as logical plan")?;
    let schema = logical_plan.schema();
    let df_columns = schema.columns();
    let mut projection_exprs = Vec::<Expr>::new();
    let mut layouts = Vec::<ProbeColumnLayout>::new();
    let mut projected_column_names = Vec::<String>::new();

    for (ordinal, column) in df_columns.iter().enumerate() {
        let (qualifier, field) = schema.qualified_field(ordinal);
        let projected_name = format!("c{ordinal}");
        let source_column = qualifier
            .map(ToString::to_string)
            .filter(|name| name.contains("__internal_#"))
            .map(|_| DFColumn::from_name(field.name().clone()))
            .unwrap_or_else(|| column.clone());
        projection_exprs.push(Expr::Column(source_column).alias(projected_name.clone()));
        projected_column_names.push(projected_name);
        let distinct_supported = supports_count_distinct(field.data_type());
        let min_max_supported = supports_min_max(field.data_type());

        layouts.push(ProbeColumnLayout {
            distinct_supported,
            min_max_supported,
        });
    }

    let projected_plan = LogicalPlanBuilder::from(logical_plan)
        .project(projection_exprs)?
        .build()
        .context("failed to build exact-stats projection probe")?;

    let row_count_batch = execute_single_row_probe(
        plan_artifacts,
        projected_plan.clone(),
        vec![count(lit(1)).alias("row_count")],
        "exact row-count probe",
    )
    .await?;
    let row_count = scalar_to_u64(&ScalarValue::try_from_array(
        row_count_batch.column(0).as_ref(),
        0,
    )?)?;

    let mut columns = Vec::with_capacity(output_schema.len());
    for (ordinal, layout) in layouts.iter().enumerate() {
        let expr = Expr::Column(DFColumn::from_name(projected_column_names[ordinal].clone()));
        let mut aggregate_exprs = vec![count(expr.clone()).alias("nonnull_count")];
        if layout.distinct_supported {
            aggregate_exprs.push(
                count(expr.clone())
                    .distinct()
                    .build()?
                    .alias("distinct_count"),
            );
        }
        if layout.min_max_supported {
            aggregate_exprs.push(min(expr.clone()).alias("min_value"));
            aggregate_exprs.push(max(expr).alias("max_value"));
        }
        let column_batch = execute_single_row_probe(
            plan_artifacts,
            projected_plan.clone(),
            aggregate_exprs,
            &format!("exact column probe for column {ordinal}"),
        )
        .await?;

        let mut index = 0;
        let non_null_count = scalar_to_u64(&ScalarValue::try_from_array(
            column_batch.column(index).as_ref(),
            0,
        )?)?;
        index += 1;
        let distinct_count = if layout.distinct_supported {
            let value = scalar_to_u64(&ScalarValue::try_from_array(
                column_batch.column(index).as_ref(),
                0,
            )?)?;
            index += 1;
            value
        } else {
            None
        };
        let min_value = if layout.min_max_supported {
            let value = scalar_to_string(&ScalarValue::try_from_array(
                column_batch.column(index).as_ref(),
                0,
            )?);
            index += 1;
            value
        } else {
            None
        };
        let max_value = if layout.min_max_supported {
            let value = scalar_to_string(&ScalarValue::try_from_array(
                column_batch.column(index).as_ref(),
                0,
            )?);
            value
        } else {
            None
        };

        let null_count = match (row_count, non_null_count) {
            (Some(rows), Some(non_null)) => Some(rows.saturating_sub(non_null)),
            _ => None,
        };
        columns.push(ExactColumnStats {
            null_count,
            distinct_count,
            min_value,
            max_value,
        });
    }

    Ok(ExactNodeStats { row_count, columns })
}

async fn execute_single_row_probe(
    plan_artifacts: &OptdPlanArtifacts,
    input_plan: datafusion::logical_expr::LogicalPlan,
    aggregate_exprs: Vec<Expr>,
    probe_name: &str,
) -> Result<datafusion::arrow::array::RecordBatch> {
    let aggregate_plan = LogicalPlanBuilder::from(input_plan)
        .aggregate(Vec::<Expr>::new(), aggregate_exprs)?
        .build()
        .with_context(|| format!("failed to build {probe_name}"))?;
    let batches = plan_artifacts
        .execute_logical_plan(aggregate_plan)
        .await
        .with_context(|| format!("failed to execute {probe_name}"))?;
    let total_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    if total_rows != 1 {
        anyhow::bail!(
            "{probe_name} returned {total_rows} rows across {} batches",
            batches.len()
        );
    }
    batches
        .into_iter()
        .find(|batch| batch.num_rows() == 1)
        .with_context(|| format!("{probe_name} returned no non-empty batch"))
}

fn build_error_metrics(estimated: &EstimatedNodeStats, exact: &ExactNodeStats) -> NodeErrorMetrics {
    let columns = estimated
        .columns
        .iter()
        .zip(exact.columns.iter())
        .map(|(estimated_col, exact_col)| {
            let null_fraction_abs_error = match (
                estimated.row_count,
                exact.row_count,
                estimated_col.null_count,
                exact_col.null_count,
            ) {
                (
                    Some(estimated_rows),
                    Some(exact_rows),
                    Some(estimated_nulls),
                    Some(exact_nulls),
                ) if estimated_rows > 0.0 && exact_rows > 0 => {
                    let estimated_fraction = estimated_nulls as f64 / estimated_rows;
                    let exact_fraction = exact_nulls as f64 / exact_rows as f64;
                    Some((estimated_fraction - exact_fraction).abs())
                }
                _ => None,
            };

            ColumnErrorMetrics {
                distinct_count_q_error: q_error(
                    estimated_col.distinct_count.map(|value| value as f64),
                    exact_col.distinct_count.map(|value| value as f64),
                ),
                null_fraction_abs_error,
                min_value_match: exact_col
                    .min_value
                    .as_ref()
                    .map(|exact_min| estimated_col.min_value.as_ref() == Some(exact_min)),
                max_value_match: exact_col
                    .max_value
                    .as_ref()
                    .map(|exact_max| estimated_col.max_value.as_ref() == Some(exact_max)),
            }
        })
        .collect();

    NodeErrorMetrics {
        row_count_q_error: q_error(
            estimated.row_count,
            exact.row_count.map(|value| value as f64),
        ),
        columns,
    }
}

fn operator_kind_name(node: &Operator) -> String {
    match &node.kind {
        OperatorKind::Group(_) => "Group",
        OperatorKind::Get(_) => "Get",
        OperatorKind::Join(_) => "Join",
        OperatorKind::DependentJoin(_) => "DependentJoin",
        OperatorKind::Select(_) => "Select",
        OperatorKind::Project(_) => "Project",
        OperatorKind::Aggregate(_) => "Aggregate",
        OperatorKind::Limit(_) => "Limit",
        OperatorKind::OrderBy(_) => "OrderBy",
        OperatorKind::Remap(_) => "Remap",
        OperatorKind::Subquery(_) => "Subquery",
        OperatorKind::EnforcerSort(_) => "EnforcerSort",
    }
    .to_string()
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

fn scalar_to_u64(value: &ScalarValue) -> Result<Option<u64>> {
    Ok(match value {
        ScalarValue::UInt64(Some(value)) => Some(*value),
        ScalarValue::UInt32(Some(value)) => Some((*value).into()),
        ScalarValue::UInt16(Some(value)) => Some((*value).into()),
        ScalarValue::UInt8(Some(value)) => Some((*value).into()),
        ScalarValue::Int64(Some(value)) => Some((*value).try_into().context("negative count")?),
        ScalarValue::Int32(Some(value)) => Some((*value).try_into().context("negative count")?),
        ScalarValue::Int16(Some(value)) => Some((*value).try_into().context("negative count")?),
        ScalarValue::Int8(Some(value)) => Some((*value).try_into().context("negative count")?),
        _ if value.is_null() => None,
        other => anyhow::bail!("expected integer count, found {other:?}"),
    })
}

fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    (!value.is_null()).then(|| value.to_string())
}

/// Quotient error -- q = max(a/b, b/a)
/// Measure of how many order of magnitude actual and estimated are apart.
/// Treats larger and smaller relative estimates symmetrically.
fn q_error(estimated: Option<f64>, actual: Option<f64>) -> Option<f64> {
    let (estimated, actual) = (estimated?, actual?);
    if estimated == 0.0 && actual == 0.0 {
        return Some(1.0);
    }
    if estimated <= 0.0 || actual <= 0.0 {
        return None;
    }
    Some((estimated / actual).max(actual / estimated))
}

fn hash_path_contents(path: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut paths = fs::read_dir(path)
        .with_context(|| format!("failed to read {}", path.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|entry| entry.is_file())
        .collect::<Vec<_>>();
    paths.sort();

    for file in paths {
        hasher.update(
            file.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default(),
        );
        hasher
            .update(fs::read(&file).with_context(|| format!("failed to read {}", file.display()))?);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn hash_string(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn normalize_explain(explain: &str) -> String {
    static INTERNAL_TABLE_RE: OnceLock<Regex> = OnceLock::new();
    static COLUMN_ID_RE: OnceLock<Regex> = OnceLock::new();
    static TABLE_INDEX_RE: OnceLock<Regex> = OnceLock::new();
    static CARDINALITY_RE: OnceLock<Regex> = OnceLock::new();

    let mut normalized = explain.to_string();
    normalized = INTERNAL_TABLE_RE
        .get_or_init(|| Regex::new(r"__internal_#\d+").unwrap())
        .replace_all(&normalized, "__internal_#X")
        .into_owned();
    normalized = COLUMN_ID_RE
        .get_or_init(|| Regex::new(r"#\d+\.\d+").unwrap())
        .replace_all(&normalized, "#X.Y")
        .into_owned();
    normalized = TABLE_INDEX_RE
        .get_or_init(|| {
            Regex::new(r"\.(table_index|key_table_index|aggregate_table_index): \d+").unwrap()
        })
        .replace_all(&normalized, ".$1: X")
        .into_owned();
    CARDINALITY_RE
        .get_or_init(|| Regex::new(r"\(\.cardinality\): [0-9.]+").unwrap())
        .replace_all(&normalized, "(.cardinality): ?")
        .into_owned()
}

#[derive(Debug, Clone, Copy)]
struct ProbeColumnLayout {
    distinct_supported: bool,
    min_max_supported: bool,
}

#[cfg(test)]
mod tests {
    use super::{hash_string, normalize_explain, q_error};

    #[test]
    fn normalizes_ids_out_of_explain() {
        let explain =
            "Project { .table_index: 5, (.cardinality): 10.00 } `__internal_#5`.`x`(#5.0)";
        let normalized = normalize_explain(explain);
        assert!(!normalized.contains(".table_index: 5"));
        assert!(!normalized.contains("__internal_#5"));
        assert!(!normalized.contains("#5.0"));
        assert!(normalized.contains("__internal_#X"));
    }

    #[test]
    fn hashes_are_deterministic() {
        assert_eq!(hash_string("abc"), hash_string("abc"));
    }

    #[test]
    fn q_error_handles_zero_cases() {
        assert_eq!(q_error(Some(0.0), Some(0.0)), Some(1.0));
        assert_eq!(q_error(Some(10.0), Some(5.0)), Some(2.0));
        assert_eq!(q_error(Some(0.0), Some(5.0)), None);
    }
}
