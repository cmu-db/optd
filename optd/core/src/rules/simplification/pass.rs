use super::{
    prune::ColumnPruningRulePass,
    rewrite::{
        MergeProjectRulePass, MergeSelectRulePass, PushSelectThroughJoinRulePass,
        PushSelectThroughProjectRulePass, ScalarSimplificationRulePass,
    },
    rule::RulePass,
};
use crate::ir::{IRContext, Operator};
use std::sync::Arc;

/// The maximum number of iterations simplification should run, if it hasn't
/// yet converged onto a fixed point
const MAX_ITERATIONS: usize = 10;

/// A deterministic logical simplification pass that runs before cascades.
///
/// This pass focuses on common rewrites:
/// - scalar simplification
/// - filter/predicate pushdown
/// - select/project merging
/// - top-down column pruning (including `LogicalGet` projection pruning)
pub struct SimplificationPass {
    max_iterations: usize,
}

impl Default for SimplificationPass {
    fn default() -> Self {
        Self::new()
    }
}

impl SimplificationPass {
    pub fn new() -> Self {
        Self {
            max_iterations: MAX_ITERATIONS,
        }
    }

    pub fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
        let rules: [&dyn RulePass; 6] = [
            &ScalarSimplificationRulePass,
            &MergeSelectRulePass,
            &PushSelectThroughProjectRulePass,
            &PushSelectThroughJoinRulePass,
            &MergeProjectRulePass,
            &ColumnPruningRulePass,
        ];

        let mut current = root;
        for _ in 0..self.max_iterations {
            let mut next = current.clone();
            for rule in rules {
                next = rule.apply(next, ctx);
            }

            if next == current {
                return next;
            }
            current = next;
        }

        current
    }
}
