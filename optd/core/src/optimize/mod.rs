pub mod expr_simplify;
pub mod holistic_unnesting;
pub mod join_ordering;
pub mod join_tree_normalize;
pub mod mark_join_to_semi_join;
pub mod predicate_pushdown;
pub mod projection_elimination;
pub mod subquery_to_join;
pub mod unnesting;
pub use expr_simplify::ExprSimplify;
pub use holistic_unnesting::HolisticUnnesting;
pub use join_ordering::JoinOrdering;
pub use join_tree_normalize::JoinTreeNormalize;
pub use mark_join_to_semi_join::MarkJoinToSemiJoin;
pub use predicate_pushdown::PredicatePushdown;
pub use projection_elimination::ProjectionElimination;
pub use subquery_to_join::SubqueryToJoin;
pub use unnesting::Unnesting;

use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use crate::{Operator, OperatorData, OptimizerContext, QueryContext, Relation};

/// Tracks append-only operator replacements for one pass invocation.
///
/// Passes append new operators to [`OptimizerContext`] and record the mapping here.
/// [`PassManager`] resolves the query root through the map after each pass.
#[derive(Debug, Default)]
pub struct RewriteMap {
    replacements: HashMap<Operator, Operator>,
}

impl RewriteMap {
    /// Creates an empty rewrite map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that `old` should be replaced by `new`.
    pub fn replace(&mut self, old: Operator, new: Operator) {
        self.replacements.insert(old, new);
    }

    /// Follows the replacement chain for `operator`, returning the latest replacement.
    ///
    /// If `operator` has no replacement, returns `operator` unchanged.
    pub fn resolve(&self, operator: Operator) -> Operator {
        let mut current = operator;
        while let Some(&next) = self.replacements.get(&current) {
            current = next;
        }
        current
    }
}

#[derive(Debug)]
pub enum OptimizeError {
    /// A pass returned an error with a message.
    PassError { pass: &'static str, message: String },
    /// The pass manager reached its iteration limit without converging.
    MaxIterationsReached { pass: &'static str },
}

impl std::fmt::Display for OptimizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PassError { pass, message } => write!(f, "pass '{pass}' failed: {message}"),
            Self::MaxIterationsReached { pass } => {
                write!(f, "optimizer reached max iterations at {pass}")
            }
        }
    }
}

impl std::error::Error for OptimizeError {}

/// Result type for optimization operations.
pub type OptimizeResult<T> = Result<T, OptimizeError>;

/// Base trait for all optimizer passes.
pub trait Pass {
    fn name(&self) -> &'static str;
}

/// Formats an internal pass name for display.
pub fn display_pass_name(pass: &str) -> String {
    pass.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// Whether a pass changed the reachable plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassResult {
    Unchanged,
    Changed,
}

/// A pass that rewrites a full query.
pub trait QueryPass: Pass {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult>;
}

/// Timing information for one pass invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct PassProfile {
    /// 1-based fixpoint iteration number.
    pub iteration: usize,
    /// 0-based registration index of the pass in the manager.
    pub pass_index: usize,
    /// Pass name returned by [`Pass::name`].
    pub pass: &'static str,
    /// Pass result. `None` means the pass returned an error.
    pub result: Option<PassResult>,
    /// Wall-clock time spent inside this pass invocation, in milliseconds.
    pub duration_ms: f64,
}

/// A query snapshot captured immediately after one pass invocation.
#[derive(Debug, Clone)]
pub struct PassTrace {
    /// Profile data for the pass invocation.
    pub profile: PassProfile,
    /// Query state after this pass invocation and root resolution.
    pub query: QueryContext,
}

/// Traversal direction for [`OperatorRewrite`] rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Visit children before parents (post-order). Default for most rules.
    BottomUp,
    /// Visit parents before children (pre-order).
    TopDown,
}

impl Direction {
    fn collect_ops(
        self,
        root: Operator,
        query: &QueryContext,
        rewrites: &RewriteMap,
    ) -> Vec<Operator> {
        match self {
            Self::BottomUp => collect_post_order(root, query, rewrites),
            Self::TopDown => collect_pre_order(root, query, rewrites),
        }
    }

    fn materialize_inputs_before_rule(self) -> bool {
        matches!(self, Self::BottomUp)
    }
}

/// Result returned by an [`OperatorRewrite`] rule.
#[derive(Debug)]
pub enum Rewrite {
    /// Leave this operator unchanged.
    Keep,
    /// Replace this operator with a new one.
    Replace(Operator),
}

/// A local rewrite rule over one operator.
///
/// The adaptor owns traversal, input resolution, and rewrite-map updates.
/// Rules only need to pattern-match and return [`Rewrite::Keep`] or [`Rewrite::Replace`].
pub trait OperatorRewrite: Pass {
    /// Traversal order. Defaults to bottom-up.
    fn direction(&self) -> Direction {
        Direction::BottomUp
    }

    fn rewrite(&mut self, op: Operator, ctx: &mut OptimizerContext) -> OptimizeResult<Rewrite>;
}

/// Adapts an [`OperatorRewrite`] rule into a [`QueryPass`].
///
/// Owns traversal (bottom-up or top-down), resolves operators through the
/// rewrite map before passing them to the rule, and records replacements.
pub struct OperatorRewriteAdaptor<R> {
    rule: R,
}

impl<R: OperatorRewrite> OperatorRewriteAdaptor<R> {
    pub fn new(rule: R) -> Self {
        Self { rule }
    }
}

impl<R: OperatorRewrite> Pass for OperatorRewriteAdaptor<R> {
    fn name(&self) -> &'static str {
        self.rule.name()
    }
}

impl<R: OperatorRewrite> QueryPass for OperatorRewriteAdaptor<R> {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };

        let direction = self.rule.direction();
        let ops = direction.collect_ops(root, &ctx.query, &ctx.rewrites);

        let mut changed = false;
        for op in ops {
            let mut current = ctx.rewrites.resolve(op);
            if direction.materialize_inputs_before_rule() {
                let materialized = materialize_operator_inputs(current, ctx);
                current = materialized.op;
                changed |= materialized.changed;
            }

            match self.rule.rewrite(current, ctx)? {
                Rewrite::Keep => {}
                Rewrite::Replace(replacement) => {
                    ctx.rewrites.replace(current, replacement);
                    materialize_reachable_rewrites(root, ctx);
                    changed = true;
                }
            }
        }

        Ok(if changed {
            PassResult::Changed
        } else {
            PassResult::Unchanged
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct MaterializedOperator {
    op: Operator,
    changed: bool,
}

fn materialize_operator_inputs(op: Operator, ctx: &mut OptimizerContext) -> MaterializedOperator {
    let data = ctx.query.operator(op).clone();
    let (rebuilt, changed) = operator_with_resolved_inputs(data, ctx);
    if changed {
        let rebuilt = rebuilt.add(&mut ctx.query);
        ctx.rewrites.replace(op, rebuilt);
        MaterializedOperator {
            op: rebuilt,
            changed: true,
        }
    } else {
        MaterializedOperator { op, changed: false }
    }
}

pub(crate) fn materialize_reachable_rewrites(
    root: Operator,
    ctx: &mut OptimizerContext,
) -> Operator {
    for op in collect_post_order(root, &ctx.query, &ctx.rewrites) {
        let current = ctx.rewrites.resolve(op);
        materialize_operator_inputs(current, ctx);
    }

    ctx.rewrites.resolve(root)
}

fn operator_with_resolved_inputs(
    data: OperatorData,
    ctx: &OptimizerContext,
) -> (OperatorData, bool) {
    let mut changed = false;
    let data = data.map_inputs(|input| {
        let resolved = ctx.rewrites.resolve(input);
        changed |= resolved != input;
        resolved
    });
    (data, changed)
}

fn collect_post_order(root: Operator, ctx: &QueryContext, rewrites: &RewriteMap) -> Vec<Operator> {
    let mut visited = HashSet::new();
    let mut result = Vec::new();
    post_order(root, ctx, rewrites, &mut visited, &mut result);
    result
}

fn post_order(
    op: Operator,
    ctx: &QueryContext,
    rewrites: &RewriteMap,
    visited: &mut HashSet<Operator>,
    result: &mut Vec<Operator>,
) {
    let op = rewrites.resolve(op);
    if !visited.insert(op) {
        return;
    }
    for child in op.get(ctx).inputs() {
        post_order(child, ctx, rewrites, visited, result);
    }
    result.push(op);
}

fn collect_pre_order(root: Operator, ctx: &QueryContext, rewrites: &RewriteMap) -> Vec<Operator> {
    let mut visited = HashSet::new();
    let mut result = Vec::new();
    pre_order(root, ctx, rewrites, &mut visited, &mut result);
    result
}

fn pre_order(
    op: Operator,
    ctx: &QueryContext,
    rewrites: &RewriteMap,
    visited: &mut HashSet<Operator>,
    result: &mut Vec<Operator>,
) {
    let op = rewrites.resolve(op);
    if !visited.insert(op) {
        return;
    }
    result.push(op);
    for child in op.get(ctx).inputs() {
        pre_order(child, ctx, rewrites, visited, result);
    }
}

/// Runs query passes in registration order until no pass reports changes.
pub struct PassManager {
    passes: Vec<Box<dyn QueryPass>>,
    max_iterations: Option<usize>,
    next_run_id: u64,
    profiles: Vec<PassProfile>,
}

impl PassManager {
    /// Creates a pass manager with no iteration limit.
    pub fn new() -> Self {
        Self {
            passes: Vec::new(),
            max_iterations: None,
            next_run_id: 1,
            profiles: Vec::new(),
        }
    }

    /// Creates a pass manager with a per-pass iteration limit.
    pub fn with_max_iterations(max_iterations: usize) -> Self {
        Self {
            passes: Vec::new(),
            max_iterations: Some(max_iterations),
            next_run_id: 1,
            profiles: Vec::new(),
        }
    }

    /// Registers a pass to run in order.
    pub fn add_pass(&mut self, pass: impl QueryPass + 'static) {
        self.passes.push(Box::new(pass));
    }

    /// Returns profiling records from the most recent [`PassManager::run`].
    pub fn profiles(&self) -> &[PassProfile] {
        &self.profiles
    }

    /// Runs each pass to fixpoint in registration order, up to `max_iterations` per pass.
    pub fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<()> {
        self.run_inner(ctx, None)
    }

    /// Runs each pass to fixpoint in registration order and captures a query snapshot after each invocation.
    pub fn run_with_trace(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<Vec<PassTrace>> {
        let mut trace = Vec::new();
        self.run_inner(ctx, Some(&mut trace))?;
        Ok(trace)
    }

    fn run_inner(
        &mut self,
        ctx: &mut OptimizerContext,
        mut trace: Option<&mut Vec<PassTrace>>,
    ) -> OptimizeResult<()> {
        self.profiles.clear();
        ctx.optimizer_run_id = self.next_run_id;
        self.next_run_id += 1;

        for (pass_index, pass) in self.passes.iter_mut().enumerate() {
            let mut converged = false;
            let mut iteration = 0usize;
            loop {
                iteration += 1;
                if let Some(max) = self.max_iterations
                    && iteration > max
                {
                    break;
                }
                let pass_name = pass.name();
                let started = Instant::now();
                let result = pass.run(ctx);
                let duration_ms = started.elapsed().as_secs_f64() * 1_000.0;
                let result_value = match result {
                    Ok(result) => Some(result),
                    Err(error) => {
                        let profile = PassProfile {
                            iteration,
                            pass_index,
                            pass: pass_name,
                            result: None,
                            duration_ms,
                        };
                        self.profiles.push(profile.clone());
                        if let Some(trace) = trace.as_deref_mut() {
                            trace.push(PassTrace {
                                profile,
                                query: ctx.query.clone(),
                            });
                        }
                        return Err(error);
                    }
                };
                let changed = result_value == Some(PassResult::Changed);
                let profile = PassProfile {
                    iteration,
                    pass_index,
                    pass: pass_name,
                    result: result_value,
                    duration_ms,
                };
                self.profiles.push(profile.clone());

                if changed {
                    // Resolve the query root through the pass's rewrite map.
                    if let Some(root) = ctx.query.root() {
                        let resolved = ctx.rewrites.resolve(root);
                        if resolved != root {
                            ctx.query.set_root(resolved);
                        }
                    }
                }

                if let Some(trace) = trace.as_deref_mut() {
                    trace.push(PassTrace {
                        profile,
                        query: ctx.query.clone(),
                    });
                }

                if !changed {
                    converged = true;
                    break;
                }
            }

            if !converged {
                return Err(OptimizeError::MaxIterationsReached { pass: pass.name() });
            }
        }
        Ok(())
    }
}

impl Default for PassManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ColumnData, ExprData, Join, JoinType, OperatorData, Output, QueryContext, ScalarValue,
        Scan, Selection, TableRef,
    };
    use arrow_schema::DataType;

    // --- RewriteMap tests ---

    #[test]
    fn rewrite_map_resolve_returns_original_when_no_replacement() {
        let map = RewriteMap::new();
        let mut ctx = QueryContext::new();
        let op = OperatorData::Output(Output {
            input: OperatorData::Scan(Scan {
                table: TableRef::bare("t"),
                columns: vec![],
            })
            .add(&mut ctx),
        })
        .add(&mut ctx);
        assert_eq!(map.resolve(op), op);
    }

    #[test]
    fn rewrite_map_resolve_follows_chain() {
        let mut map = RewriteMap::new();
        let mut ctx = QueryContext::new();
        let col = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let a = OperatorData::Scan(Scan {
            table: TableRef::bare("a"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let b = OperatorData::Scan(Scan {
            table: TableRef::bare("b"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let c = OperatorData::Scan(Scan {
            table: TableRef::bare("c"),
            columns: vec![col],
        })
        .add(&mut ctx);
        map.replace(a, b);
        map.replace(b, c);
        assert_eq!(map.resolve(a), c);
        assert_eq!(map.resolve(b), c);
        assert_eq!(map.resolve(c), c);
    }

    #[test]
    fn pass_manager_resolves_root_through_rewrite_map() {
        use crate::OptimizerContext;

        struct ReplaceRootPass {
            fired: bool,
        }
        impl Pass for ReplaceRootPass {
            fn name(&self) -> &'static str {
                "replace_root"
            }
        }
        impl QueryPass for ReplaceRootPass {
            fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
                if self.fired {
                    return Ok(PassResult::Unchanged);
                }
                let Some(root) = ctx.query.root() else {
                    return Ok(PassResult::Unchanged);
                };
                // Append a replacement and record it.
                let replacement = ctx.query.operator(root).clone().add(&mut ctx.query);
                ctx.rewrites.replace(root, replacement);
                self.fired = true;
                Ok(PassResult::Changed)
            }
        }

        let mut ctx = QueryContext::new();
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![],
        })
        .add(&mut ctx);
        ctx.set_root(scan);

        let mut opt_ctx = crate::test_optimizer_context(ctx);
        let mut pm = PassManager::new();
        pm.add_pass(ReplaceRootPass { fired: false });
        pm.run(&mut opt_ctx).unwrap();

        // Root should have been updated to the replacement.
        assert_ne!(opt_ctx.query.root(), Some(scan));
    }

    #[test]
    fn operator_rewrite_adaptor_rebuilds_parent_with_rewritten_child() {
        struct ReplaceScan {
            replacement: Operator,
        }

        impl Pass for ReplaceScan {
            fn name(&self) -> &'static str {
                "replace_scan"
            }
        }

        impl OperatorRewrite for ReplaceScan {
            fn rewrite(
                &mut self,
                op: Operator,
                ctx: &mut OptimizerContext,
            ) -> OptimizeResult<Rewrite> {
                Ok(match ctx.query.operator(op) {
                    OperatorData::Scan(_) => Rewrite::Replace(self.replacement),
                    _ => Rewrite::Keep,
                })
            }
        }

        let mut ctx = QueryContext::new();
        let col = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("old"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let replacement = OperatorData::Scan(Scan {
            table: TableRef::bare("new"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: scan }).add(&mut ctx);
        ctx.set_root(output);

        let mut opt_ctx = crate::test_optimizer_context(ctx);
        let mut pass = OperatorRewriteAdaptor::new(ReplaceScan { replacement });

        assert_eq!(pass.run(&mut opt_ctx).unwrap(), PassResult::Changed);

        let root = opt_ctx.rewrites.resolve(output);
        assert_ne!(root, output);
        let OperatorData::Output(rebuilt) = opt_ctx.query.operator(root) else {
            panic!("expected rebuilt output");
        };
        assert_eq!(rebuilt.input, replacement);
    }

    #[test]
    fn top_down_operator_rewrite_adaptor_rebuilds_parent_after_child_rewrite() {
        struct ReplaceScanTopDown {
            replacement: Operator,
        }

        impl Pass for ReplaceScanTopDown {
            fn name(&self) -> &'static str {
                "replace_scan_top_down"
            }
        }

        impl OperatorRewrite for ReplaceScanTopDown {
            fn direction(&self) -> Direction {
                Direction::TopDown
            }

            fn rewrite(
                &mut self,
                op: Operator,
                ctx: &mut OptimizerContext,
            ) -> OptimizeResult<Rewrite> {
                Ok(match ctx.query.operator(op) {
                    OperatorData::Scan(_) => Rewrite::Replace(self.replacement),
                    _ => Rewrite::Keep,
                })
            }
        }

        let mut ctx = QueryContext::new();
        let col = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("old"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let replacement = OperatorData::Scan(Scan {
            table: TableRef::bare("new"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: scan }).add(&mut ctx);
        ctx.set_root(output);

        let mut opt_ctx = crate::test_optimizer_context(ctx);
        let mut pass = OperatorRewriteAdaptor::new(ReplaceScanTopDown { replacement });

        assert_eq!(pass.run(&mut opt_ctx).unwrap(), PassResult::Changed);

        let root = opt_ctx.rewrites.resolve(output);
        assert_ne!(root, output);
        let OperatorData::Output(rebuilt) = opt_ctx.query.operator(root) else {
            panic!("expected rebuilt output");
        };
        assert_eq!(rebuilt.input, replacement);
    }

    #[test]
    fn top_down_operator_rewrite_adaptor_rebuilds_new_intermediate_nodes() {
        struct WrapSelectionAndReplaceScan {
            replacement: Operator,
        }

        impl Pass for WrapSelectionAndReplaceScan {
            fn name(&self) -> &'static str {
                "wrap_selection_and_replace_scan"
            }
        }

        impl OperatorRewrite for WrapSelectionAndReplaceScan {
            fn direction(&self) -> Direction {
                Direction::TopDown
            }

            fn rewrite(
                &mut self,
                op: Operator,
                ctx: &mut OptimizerContext,
            ) -> OptimizeResult<Rewrite> {
                Ok(match ctx.query.operator(op).clone() {
                    OperatorData::Selection(selection) => {
                        let inner = OperatorData::Selection(Selection {
                            predicate: selection.predicate,
                            input: selection.input,
                        })
                        .add(&mut ctx.query);
                        let outer = OperatorData::Selection(Selection {
                            predicate: selection.predicate,
                            input: inner,
                        })
                        .add(&mut ctx.query);
                        Rewrite::Replace(outer)
                    }
                    OperatorData::Scan(_) => Rewrite::Replace(self.replacement),
                    _ => Rewrite::Keep,
                })
            }
        }

        let mut ctx = QueryContext::new();
        let col = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("old"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let replacement = OperatorData::Scan(Scan {
            table: TableRef::bare("new"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: selection }).add(&mut ctx);
        ctx.set_root(output);

        let mut opt_ctx = crate::test_optimizer_context(ctx);
        let mut pass = OperatorRewriteAdaptor::new(WrapSelectionAndReplaceScan { replacement });

        assert_eq!(pass.run(&mut opt_ctx).unwrap(), PassResult::Changed);

        let root = opt_ctx.rewrites.resolve(output);
        let OperatorData::Output(rebuilt_output) = opt_ctx.query.operator(root) else {
            panic!("expected rebuilt output");
        };
        let OperatorData::Selection(outer) = opt_ctx.query.operator(rebuilt_output.input) else {
            panic!("expected outer selection");
        };
        let OperatorData::Selection(inner) = opt_ctx.query.operator(outer.input) else {
            panic!("expected inner selection");
        };
        assert_eq!(inner.input, replacement);
    }

    #[test]
    fn materialize_reachable_rewrites_rebuilds_shared_parent_paths() {
        let mut ctx = QueryContext::new();
        let col = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let left_predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let left = OperatorData::Selection(Selection {
            predicate: left_predicate,
            input: scan,
        })
        .add(&mut ctx);
        let right_predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let right = OperatorData::Selection(Selection {
            predicate: right_predicate,
            input: scan,
        })
        .add(&mut ctx);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: left,
            inner: right,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: join }).add(&mut ctx);
        ctx.set_root(output);

        let replacement = OperatorData::Scan(Scan {
            table: TableRef::bare("replacement"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let mut opt_ctx = crate::test_optimizer_context(ctx);
        opt_ctx.rewrites.replace(scan, replacement);

        let root = materialize_reachable_rewrites(output, &mut opt_ctx);

        assert_ne!(root, output);
        let OperatorData::Output(rebuilt_output) = opt_ctx.query.operator(root) else {
            panic!("root should remain an output");
        };
        let OperatorData::Join(rebuilt_join) = opt_ctx.query.operator(rebuilt_output.input) else {
            panic!("output input should remain a join");
        };
        let OperatorData::Selection(rebuilt_left) = opt_ctx.query.operator(rebuilt_join.outer)
        else {
            panic!("join outer should remain a selection");
        };
        let OperatorData::Selection(rebuilt_right) = opt_ctx.query.operator(rebuilt_join.inner)
        else {
            panic!("join inner should remain a selection");
        };
        assert_eq!(rebuilt_left.input, replacement);
        assert_eq!(rebuilt_right.input, replacement);
    }

    // --- PassManager tests ---

    struct NoopPass;
    impl Pass for NoopPass {
        fn name(&self) -> &'static str {
            "noop"
        }
    }
    impl QueryPass for NoopPass {
        fn run(&mut self, _ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
            Ok(PassResult::Unchanged)
        }
    }

    struct AlwaysChangesPass(usize);
    impl Pass for AlwaysChangesPass {
        fn name(&self) -> &'static str {
            "always_changes"
        }
    }
    impl QueryPass for AlwaysChangesPass {
        fn run(&mut self, _ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
            self.0 += 1;
            Ok(PassResult::Changed)
        }
    }

    #[test]
    fn noop_pass_converges_in_one_iteration() {
        let mut pm = PassManager::new();
        pm.add_pass(NoopPass);
        let mut ctx = crate::test_optimizer_context(QueryContext::new());
        pm.run(&mut ctx).unwrap();

        let profiles = pm.profiles();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].iteration, 1);
        assert_eq!(profiles[0].pass_index, 0);
        assert_eq!(profiles[0].pass, "noop");
        assert_eq!(profiles[0].result, Some(PassResult::Unchanged));
        assert!(profiles[0].duration_ms >= 0.0);
    }

    #[test]
    fn always_changes_pass_hits_max_iterations() {
        let mut pm = PassManager::with_max_iterations(3);
        pm.add_pass(AlwaysChangesPass(0));
        let mut ctx = crate::test_optimizer_context(QueryContext::new());
        assert!(matches!(
            pm.run(&mut ctx),
            Err(OptimizeError::MaxIterationsReached { .. })
        ));
    }

    #[test]
    fn pass_manager_runs_passes_in_order() {
        use std::sync::{Arc, Mutex};

        let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

        struct LogPass {
            name: &'static str,
            log: Arc<Mutex<Vec<&'static str>>>,
            calls: usize,
        }
        impl Pass for LogPass {
            fn name(&self) -> &'static str {
                self.name
            }
        }
        impl QueryPass for LogPass {
            fn run(&mut self, _ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
                self.log.lock().unwrap().push(self.name);
                self.calls += 1;
                // Report Changed on first call to trigger a second iteration.
                if self.calls == 1 {
                    Ok(PassResult::Changed)
                } else {
                    Ok(PassResult::Unchanged)
                }
            }
        }

        let mut pm = PassManager::new();
        pm.add_pass(LogPass {
            name: "a",
            log: Arc::clone(&log),
            calls: 0,
        });
        pm.add_pass(LogPass {
            name: "b",
            log: Arc::clone(&log),
            calls: 0,
        });

        let mut ctx = crate::test_optimizer_context(QueryContext::new());
        pm.run(&mut ctx).unwrap();

        assert_eq!(*log.lock().unwrap(), vec!["a", "a", "b", "b"]);
        assert_eq!(pm.profiles().len(), 4);
        assert_eq!(
            pm.profiles()
                .iter()
                .map(|profile| (profile.iteration, profile.pass_index, profile.pass))
                .collect::<Vec<_>>(),
            vec![(1, 0, "a"), (2, 0, "a"), (1, 1, "b"), (2, 1, "b")]
        );
    }

    #[test]
    fn pass_manager_replaces_profiles_on_each_run() {
        let mut pm = PassManager::new();
        pm.add_pass(NoopPass);

        let mut first = crate::test_optimizer_context(QueryContext::new());
        pm.run(&mut first).unwrap();
        assert_eq!(pm.profiles().len(), 1);

        let mut second = crate::test_optimizer_context(QueryContext::new());
        pm.run(&mut second).unwrap();

        assert_eq!(pm.profiles().len(), 1);
        assert_eq!(pm.profiles()[0].iteration, 1);
    }

    #[test]
    fn run_with_trace_records_each_pass_invocation() {
        struct OneChangeThenStable {
            calls: usize,
        }

        impl Pass for OneChangeThenStable {
            fn name(&self) -> &'static str {
                "one_change_then_stable"
            }
        }

        impl QueryPass for OneChangeThenStable {
            fn run(&mut self, _ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
                self.calls += 1;
                if self.calls == 1 {
                    Ok(PassResult::Changed)
                } else {
                    Ok(PassResult::Unchanged)
                }
            }
        }

        let mut pm = PassManager::new();
        pm.add_pass(OneChangeThenStable { calls: 0 });
        let mut ctx = crate::test_optimizer_context(QueryContext::new());

        let trace = pm.run_with_trace(&mut ctx).unwrap();
        assert_eq!(trace.len(), 2);
        assert_eq!(trace[0].profile.iteration, 1);
        assert_eq!(trace[0].profile.result, Some(PassResult::Changed));
        assert_eq!(trace[1].profile.iteration, 2);
        assert_eq!(trace[1].profile.result, Some(PassResult::Unchanged));
    }

    #[test]
    fn pass_manager_records_failed_pass_profile() {
        struct ErrorPass;

        impl Pass for ErrorPass {
            fn name(&self) -> &'static str {
                "error"
            }
        }

        impl QueryPass for ErrorPass {
            fn run(&mut self, _ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
                Err(OptimizeError::PassError {
                    pass: self.name(),
                    message: "boom".to_string(),
                })
            }
        }

        let mut pm = PassManager::new();
        pm.add_pass(ErrorPass);
        let mut ctx = crate::test_optimizer_context(QueryContext::new());

        assert!(matches!(
            pm.run(&mut ctx),
            Err(OptimizeError::PassError { pass: "error", .. })
        ));

        let profiles = pm.profiles();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].pass, "error");
        assert_eq!(profiles[0].result, None);
        assert!(profiles[0].duration_ms >= 0.0);
    }
}
