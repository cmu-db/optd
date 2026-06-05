//! UDFs that parse a SQL string through the optd IR and return
//! formatted plan information.
//!
//! - `explain_box(sql)`  — box-drawing plan with free-column analysis
//! - `explain_json(sql)` — recursive JSON display tree
//! - `explain_flat(sql)` — flat DFS post-order JSON
//! - `explain_optimizer_json(sql)` — optd optimizer visualizer pass timeline
//! - `explain_steps(sql, format)` — table function with plan snapshots after each optimizer step

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::ScalarValue as DFScalarValue;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, Expr as DFExpr, LogicalPlan, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use optd_core::{
    OptimizerContext, PassProfile, PassResult, PassTrace, QueryContext, QueryFormatConfig,
    optimizer_visualizer_trace_json,
};

use crate::from_df_logical::from_logical_plan;
use crate::runner::default_pass_manager;

/// Registers `explain_box`, `explain_json`, `explain_flat`, `explain_optimizer_json`, and
/// `explain_steps` on `ctx`.
pub fn register_explain_udfs(ctx: &SessionContext) {
    let state = Arc::new(ctx.state());
    for (name, format) in [
        ("explain_box", Format::Box),
        ("explain_json", Format::Json),
        ("explain_flat", Format::Flat),
        ("explain_optimizer_json", Format::OptimizerJson),
    ] {
        ctx.register_udf(ScalarUDF::from(ExplainUdf {
            name,
            format,
            state: IgnoredForEq(Arc::clone(&state)),
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
        }));
    }

    ctx.register_udtf(
        "explain_steps",
        Arc::new(ExplainStepsFunction {
            state: Arc::clone(&state),
        }),
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Format {
    Box,
    Json,
    Flat,
    OptimizerJson,
}

impl Format {
    fn parse(value: &str) -> DFResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "box" => Ok(Self::Box),
            "json" => Ok(Self::Json),
            "flat" => Ok(Self::Flat),
            "optimizer-json" | "optimizer_json" => Ok(Self::OptimizerJson),
            other => Err(DataFusionError::Plan(format!(
                "unsupported explain format '{other}', expected 'box', 'json', 'flat', or 'optimizer-json'"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExplainUdf {
    name: &'static str,
    format: Format,
    // SessionState is not Hash/Eq; wrap in a newtype that ignores it for those impls.
    state: IgnoredForEq<Arc<datafusion::execution::SessionState>>,
    signature: Signature,
}

struct ExplainStepsFunction {
    state: Arc<datafusion::execution::SessionState>,
}

impl std::fmt::Debug for ExplainStepsFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExplainStepsFunction")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ExplainStep {
    pub step: i64,
    pub iteration: Option<i64>,
    pub pass_index: Option<i64>,
    pub pass: String,
    pub result: String,
    pub duration_ms: Option<f64>,
    pub plan: String,
}

/// Wrapper that implements `PartialEq`, `Eq`, and `Hash` by ignoring the value.
#[derive(Debug, Clone)]
struct IgnoredForEq<T>(T);

impl<T> PartialEq for IgnoredForEq<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl<T> Eq for IgnoredForEq<T> {}
impl<T> std::hash::Hash for IgnoredForEq<T> {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

impl ScalarUDFImpl for ExplainUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let arr = match &args.args[0] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let sql_arr = arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("expected Utf8 argument".into()))?;

        let state = Arc::clone(&self.state.0);
        let format = self.format;
        let results: Vec<Option<String>> = (0..sql_arr.len())
            .map(|i| {
                if sql_arr.is_null(i) {
                    return None;
                }
                let sql = sql_arr.value(i).to_string();
                Some(explain_sql(&sql, &state, format).unwrap_or_else(|e| e.to_string()))
            })
            .collect();

        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(results)) as ArrayRef
        ))
    }
}

impl TableFunctionImpl for ExplainStepsFunction {
    fn call(&self, args: &[DFExpr]) -> DFResult<Arc<dyn TableProvider>> {
        let [sql, format] = args else {
            return Err(DataFusionError::Plan(
                "explain_steps expects arguments: sql, format".into(),
            ));
        };
        let sql = string_literal_arg(sql, "sql")?;
        let format = Format::parse(&string_literal_arg(format, "format")?)?;
        let steps = explain_steps(&sql, &self.state, format, QueryFormatConfig::default())?;
        steps_to_table(steps)
    }
}

pub fn explain_steps_box(
    sql: &str,
    ctx: &SessionContext,
) -> datafusion::error::Result<Vec<ExplainStep>> {
    let state = Arc::new(ctx.state());
    explain_steps(sql, &state, Format::Box, QueryFormatConfig::default())
}

pub fn explain_steps_box_with_config(
    sql: &str,
    ctx: &SessionContext,
    config: QueryFormatConfig,
) -> datafusion::error::Result<Vec<ExplainStep>> {
    let state = Arc::new(ctx.state());
    explain_steps(sql, &state, Format::Box, config)
}

fn explain_sql(
    sql: &str,
    state: &Arc<datafusion::execution::SessionState>,
    format: Format,
) -> datafusion::error::Result<String> {
    let state = Arc::clone(state);
    let sql = sql.to_string();

    // Spawn a dedicated thread with its own single-threaded runtime so we can
    // call async APIs (create_logical_plan) without blocking the caller's runtime.
    if format == Format::OptimizerJson {
        return std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                .block_on(build_optimizer_visualizer_trace(&sql, &state))
        })
        .join()
        .map_err(|_| datafusion::error::DataFusionError::Plan("explain thread panicked".into()))?;
    }

    let ctx = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            .block_on(build_ir(&sql, &state))
    })
    .join()
    .map_err(|_| datafusion::error::DataFusionError::Plan("explain thread panicked".into()))??;

    Ok(format_query(&ctx, format, QueryFormatConfig::default()))
}

fn explain_steps(
    sql: &str,
    state: &Arc<datafusion::execution::SessionState>,
    format: Format,
    config: QueryFormatConfig,
) -> datafusion::error::Result<Vec<ExplainStep>> {
    let state = Arc::clone(state);
    let sql = sql.to_string();

    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            .block_on(build_ir_trace(&sql, &state, format, config))
    })
    .join()
    .map_err(|_| datafusion::error::DataFusionError::Plan("explain thread panicked".into()))?
}

fn format_query(ctx: &QueryContext, format: Format, config: QueryFormatConfig) -> String {
    let text = match format {
        Format::Box => ctx.pretty_with_config(config),
        Format::Json => ctx.pretty_json(),
        Format::Flat => ctx.pretty_flat(),
        Format::OptimizerJson => ctx.optimizer_visualizer_json("0. Plan"),
    };
    trim_trailing_line_whitespace(&text)
}

fn trim_trailing_line_whitespace(text: &str) -> String {
    text.trim()
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}

async fn build_ir(
    sql: &str,
    state: &datafusion::execution::SessionState,
) -> datafusion::error::Result<QueryContext> {
    let plan = state.create_logical_plan(sql).await?;
    let plan = explain_input_plan(&plan);
    let mut ctx = QueryContext::new();
    let root =
        from_logical_plan(plan, &mut ctx).map_err(|e| DataFusionError::Plan(e.to_string()))?;
    ctx.set_root(root);

    let mut opt = OptimizerContext::new(ctx);
    let mut pm = default_pass_manager();
    pm.run(&mut opt)
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    if let Some(root) = opt.query.root() {
        let resolved = opt.rewrites.resolve(root);
        opt.query.set_root(resolved);
    }
    Ok(opt.into_query())
}

async fn build_ir_trace(
    sql: &str,
    state: &datafusion::execution::SessionState,
    format: Format,
    config: QueryFormatConfig,
) -> datafusion::error::Result<Vec<ExplainStep>> {
    let plan = state.create_logical_plan(sql).await?;
    let plan = explain_input_plan(&plan);
    let mut ctx = QueryContext::new();
    let root =
        from_logical_plan(plan, &mut ctx).map_err(|e| DataFusionError::Plan(e.to_string()))?;
    ctx.set_root(root);

    let mut steps = vec![ExplainStep {
        step: 0,
        iteration: None,
        pass_index: None,
        pass: "initial".to_string(),
        result: "initial".to_string(),
        duration_ms: None,
        plan: format_query(&ctx, format, config.clone()),
    }];

    let mut opt = OptimizerContext::new(ctx);
    let mut pm = default_pass_manager();
    let trace = pm
        .run_with_trace(&mut opt)
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    let trace = aggregate_trace_by_pass(trace);
    steps.extend(trace.into_iter().enumerate().map(|(idx, trace)| {
        let profile = trace.profile;
        ExplainStep {
            step: (idx + 1) as i64,
            iteration: Some(profile.iteration as i64),
            pass_index: Some(profile.pass_index as i64),
            pass: profile.pass.to_string(),
            result: pass_result(profile.result),
            duration_ms: Some(profile.duration_ms),
            plan: format_query(&trace.query, format, config.clone()),
        }
    }));

    Ok(steps)
}

async fn build_optimizer_visualizer_trace(
    sql: &str,
    state: &datafusion::execution::SessionState,
) -> datafusion::error::Result<String> {
    let plan = state.create_logical_plan(sql).await?;
    let plan = explain_input_plan(&plan);
    let mut ctx = QueryContext::new();
    let root =
        from_logical_plan(plan, &mut ctx).map_err(|e| DataFusionError::Plan(e.to_string()))?;
    ctx.set_root(root);
    let initial = ctx.clone();

    let mut opt = OptimizerContext::new(ctx);
    let mut pm = default_pass_manager();
    let trace = pm
        .run_with_trace(&mut opt)
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    let trace = aggregate_trace_by_pass(trace);

    let passes = optimizer_visualizer_trace_json(&initial, &trace);
    Ok(format!(
        "{{\n  \"passes\": {},\n  \"query\": {:?}\n}}",
        passes, sql
    ))
}

fn explain_input_plan(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Explain(explain) => explain.plan.as_ref(),
        LogicalPlan::Analyze(analyze) => analyze.input.as_ref(),
        _ => plan,
    }
}

fn pass_result(result: Option<PassResult>) -> String {
    match result {
        Some(PassResult::Changed) => "changed",
        Some(PassResult::Unchanged) => "unchanged",
        None => "error",
    }
    .to_string()
}

fn merge_pass_result(left: Option<PassResult>, right: Option<PassResult>) -> Option<PassResult> {
    match (left, right) {
        (None, _) | (_, None) => None,
        (Some(PassResult::Changed), _) | (_, Some(PassResult::Changed)) => {
            Some(PassResult::Changed)
        }
        (Some(PassResult::Unchanged), Some(PassResult::Unchanged)) => Some(PassResult::Unchanged),
    }
}

fn aggregate_trace_by_pass(trace: Vec<PassTrace>) -> Vec<PassTrace> {
    let mut aggregated: Vec<PassTrace> = Vec::new();
    for entry in trace {
        if let Some(last) = aggregated.last_mut()
            && last.profile.pass_index == entry.profile.pass_index
        {
            last.profile = PassProfile {
                iteration: entry.profile.iteration,
                pass_index: entry.profile.pass_index,
                pass: entry.profile.pass,
                result: merge_pass_result(last.profile.result, entry.profile.result),
                duration_ms: last.profile.duration_ms + entry.profile.duration_ms,
            };
            last.query = entry.query;
            continue;
        }
        aggregated.push(entry);
    }
    aggregated
}

fn string_literal_arg(expr: &DFExpr, name: &str) -> DFResult<String> {
    match expr {
        DFExpr::Literal(DFScalarValue::Utf8(Some(value)), _)
        | DFExpr::Literal(DFScalarValue::LargeUtf8(Some(value)), _) => Ok(value.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "explain_steps {name} argument must be a non-null string literal"
        ))),
    }
}

fn steps_to_table(steps: Vec<ExplainStep>) -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int64, false),
        Field::new("iteration", DataType::Int64, true),
        Field::new("pass_index", DataType::Int64, true),
        Field::new("pass", DataType::Utf8, false),
        Field::new("result", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Float64, true),
        Field::new("plan", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                steps.iter().map(|step| step.step).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                steps.iter().map(|step| step.iteration).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                steps.iter().map(|step| step.pass_index).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                steps
                    .iter()
                    .map(|step| step.pass.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                steps
                    .iter()
                    .map(|step| step.result.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                steps
                    .iter()
                    .map(|step| step.duration_ms)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                steps
                    .iter()
                    .map(|step| step.plan.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use optd_core::optimize::unnesting::correlated_subquery_joins;

    use super::build_ir;
    use crate::setup::setup_tpch_session;

    #[tokio::test]
    async fn tpch_queries_have_no_correlated_subquery_join_inputs_after_unnesting() {
        let session = setup_tpch_session().await.unwrap();
        let state = Arc::new(session.state());
        let mut failures = Vec::new();

        for entry in std::fs::read_dir("tests/slt/tpch/results").unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("slt") {
                continue;
            }

            let sql = first_query_sql(&path);
            let query = build_ir(&sql, &state).await.unwrap();
            let Some(root) = query.root() else {
                continue;
            };
            let correlated = correlated_subquery_joins(&query, root).unwrap();
            if !correlated.is_empty() {
                failures.push(format!(
                    "{}: {:?}",
                    path.file_name().unwrap().to_string_lossy(),
                    correlated
                        .iter()
                        .map(|join| (join.join, join.join_type.clone(), join.free_columns.clone()))
                        .collect::<Vec<_>>()
                ));
            }
        }

        assert!(
            failures.is_empty(),
            "remaining correlated subquery joins:\n{}",
            failures.join("\n")
        );
    }

    fn first_query_sql(path: &std::path::Path) -> String {
        let text = std::fs::read_to_string(path).unwrap();
        let mut in_query = false;
        let mut lines = Vec::new();

        for line in text.lines() {
            if line.trim() == "----" {
                break;
            }
            if in_query {
                lines.push(line);
                continue;
            }
            if line.starts_with("query ") {
                in_query = true;
            }
        }

        lines.join("\n")
    }
}
