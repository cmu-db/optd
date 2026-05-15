//! Scalar UDFs that parse a SQL string through the simple-graph IR and return
//! the plan as formatted text.
//!
//! - `explain_box(sql)`  — box-drawing plan with free-column analysis
//! - `explain_json(sql)` — recursive JSON display tree
//! - `explain_flat(sql)` — flat DFS post-order JSON

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use simple_graph::{OptimizerContext, PassManager, QueryContext, SubqueryToJoin};

use crate::from_df::from_logical_plan;

/// Registers `explain_box`, `explain_json`, and `explain_flat` on `ctx`.
pub fn register_explain_udfs(ctx: &SessionContext) {
    let state = Arc::new(ctx.state());
    for (name, format) in [
        ("explain_box", Format::Box),
        ("explain_json", Format::Json),
        ("explain_flat", Format::Flat),
    ] {
        ctx.register_udf(ScalarUDF::from(ExplainUdf {
            name,
            format,
            state: IgnoredForEq(Arc::clone(&state)),
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
        }));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Format {
    Box,
    Json,
    Flat,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExplainUdf {
    name: &'static str,
    format: Format,
    // SessionState is not Hash/Eq; wrap in a newtype that ignores it for those impls.
    state: IgnoredForEq<Arc<datafusion::execution::SessionState>>,
    signature: Signature,
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

fn explain_sql(
    sql: &str,
    state: &Arc<datafusion::execution::SessionState>,
    format: Format,
) -> datafusion::error::Result<String> {
    let state = Arc::clone(state);
    let sql = sql.to_string();

    // Spawn a dedicated thread with its own single-threaded runtime so we can
    // call async APIs (create_logical_plan) without blocking the caller's runtime.
    let ctx = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            .block_on(build_ir(&sql, &state))
    })
    .join()
    .map_err(|_| datafusion::error::DataFusionError::Plan("explain thread panicked".into()))??;

    Ok(match format {
        Format::Box => ctx.pretty().trim().to_string(),
        Format::Json => ctx.pretty_json().trim().to_string(),
        Format::Flat => ctx.pretty_flat().trim().to_string(),
    })
}

async fn build_ir(
    sql: &str,
    state: &datafusion::execution::SessionState,
) -> datafusion::error::Result<QueryContext> {
    let plan = state.create_logical_plan(sql).await?;
    let mut ctx = QueryContext::new();
    let root =
        from_logical_plan(&plan, &mut ctx).map_err(|e| DataFusionError::Plan(e.to_string()))?;
    ctx.set_root(root);

    let mut opt = OptimizerContext::new(ctx);
    let mut pm = PassManager::new(10);
    pm.add_pass(SubqueryToJoin);
    pm.run(&mut opt)
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    if let Some(root) = opt.query.root() {
        let resolved = opt.rewrites.resolve(root);
        opt.query.set_root(resolved);
    }
    Ok(opt.into_query())
}
