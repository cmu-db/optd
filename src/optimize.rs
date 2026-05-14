use std::collections::{HashMap, HashSet};

use crate::{Operator, OptimizerContext, QueryContext, Relation};

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
    MaxIterationsReached,
}

impl std::fmt::Display for OptimizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PassError { pass, message } => write!(f, "pass '{pass}' failed: {message}"),
            Self::MaxIterationsReached => write!(f, "optimizer reached max iterations"),
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

/// Traversal direction for [`OperatorRewrite`] rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Visit children before parents (post-order). Default for most rules.
    BottomUp,
    /// Visit parents before children (pre-order).
    TopDown,
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
    const DIRECTION: Direction = Direction::BottomUp;

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

        let ops = match R::DIRECTION {
            Direction::BottomUp => collect_post_order(root, &ctx.query, &ctx.rewrites),
            Direction::TopDown => collect_pre_order(root, &ctx.query, &ctx.rewrites),
        };

        let mut changed = false;
        for op in ops {
            let resolved = ctx.rewrites.resolve(op);
            match self.rule.rewrite(resolved, ctx)? {
                Rewrite::Keep => {}
                Rewrite::Replace(replacement) => {
                    ctx.rewrites.replace(resolved, replacement);
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
    max_iterations: usize,
}

impl PassManager {
    /// Creates a pass manager with the given iteration limit.
    pub fn new(max_iterations: usize) -> Self {
        Self {
            passes: Vec::new(),
            max_iterations,
        }
    }

    /// Registers a pass to run in order.
    pub fn add_pass(&mut self, pass: impl QueryPass + 'static) {
        self.passes.push(Box::new(pass));
    }

    /// Runs all passes in a fixpoint loop until convergence or `max_iterations`.
    pub fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<()> {
        for _ in 0..self.max_iterations {
            let mut any_changed = false;
            for pass in &mut self.passes {
                if pass.run(ctx)? == PassResult::Changed {
                    any_changed = true;
                    // Resolve the query root through the pass's rewrite map.
                    if let Some(root) = ctx.query.root() {
                        let resolved = ctx.rewrites.resolve(root);
                        if resolved != root {
                            ctx.query.set_root(resolved);
                        }
                    }
                }
            }
            if !any_changed {
                return Ok(());
            }
        }
        Err(OptimizeError::MaxIterationsReached)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnData, OperatorData, Output, QueryContext, Scan, TableRef};
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

        let mut opt_ctx = OptimizerContext::new(ctx);
        let mut pm = PassManager::new(10);
        pm.add_pass(ReplaceRootPass { fired: false });
        pm.run(&mut opt_ctx).unwrap();

        // Root should have been updated to the replacement.
        assert_ne!(opt_ctx.query.root(), Some(scan));
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
        let mut pm = PassManager::new(10);
        pm.add_pass(NoopPass);
        let mut ctx = OptimizerContext::new(QueryContext::new());
        pm.run(&mut ctx).unwrap();
    }

    #[test]
    fn always_changes_pass_hits_max_iterations() {
        let mut pm = PassManager::new(3);
        pm.add_pass(AlwaysChangesPass(0));
        let mut ctx = OptimizerContext::new(QueryContext::new());
        assert!(matches!(
            pm.run(&mut ctx),
            Err(OptimizeError::MaxIterationsReached)
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

        let mut pm = PassManager::new(10);
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

        let mut ctx = OptimizerContext::new(QueryContext::new());
        pm.run(&mut ctx).unwrap();

        assert_eq!(*log.lock().unwrap(), vec!["a", "b", "a", "b"]);
    }
}
