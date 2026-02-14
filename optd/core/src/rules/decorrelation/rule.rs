/// This is the top level rule called when decorrelation needs to be triggered
/// This rule takes in plans that are written in terms of dependent joins,
/// and unnests arbitrary levels of these dependent joins
use std::sync::Arc;

use crate::ir::IRContext;
use crate::ir::operator::{LogicalDependentJoin, Operator, OperatorKind};

pub struct UnnestingRule {}

impl UnnestingRule {
    pub fn new() -> Self {
        Self {}
    }

    pub fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
        self.traverse_and_unnest(root, ctx)
    }

    fn traverse_and_unnest(&self, op: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
        // If we are unnesting a dependent join, we run the decorrelation algorithm
        // Note that according to the paper, this does NOT always decorrelate all
        // joins, since decorrelation may end before reaching a child dependent join
        // Thus, we then map decorrelation onto all child operators, so that we keep
        // going until there still exists a dependent join operator
        if let OperatorKind::LogicalDependentJoin(meta) = &op.kind {
            let dep = LogicalDependentJoin::borrow_raw_parts(meta, &op.common);
            let res = self.d_join_elimination(dep, None, None, ctx);
            return self.traverse_and_unnest(res, ctx);
        }

        // TODO: We are recursing deep! For query plans with hundreds of nodes,
        // this may overflow. This is therefore potentially better to do with DFS
        let new_inputs: Vec<Arc<Operator>> = op
            .input_operators()
            .iter()
            .map(|child| self.traverse_and_unnest(child.clone(), ctx))
            .collect();

        if new_inputs != op.input_operators() {
            Arc::new(op.clone_with_inputs(Some(Arc::from(new_inputs)), None))
        } else {
            op
        }
    }
}
