use crate::ir::{IRContext, Operator};
/// Top level trait for what a simplification rule needs to implement, as well
/// as the recursive applifcation function
use std::sync::Arc;

pub(super) trait RulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Arc<Operator>;
}

pub(super) fn rewrite_bottom_up<F>(op: Arc<Operator>, ctx: &IRContext, rewrite: &F) -> Arc<Operator>
where
    F: Fn(Arc<Operator>, &IRContext) -> Arc<Operator>,
{
    let new_inputs = op
        .input_operators()
        .iter()
        .map(|input| rewrite_bottom_up(input.clone(), ctx, rewrite))
        .collect::<Vec<_>>();

    let with_new_inputs = if new_inputs.as_slice() == op.input_operators() {
        op
    } else {
        Arc::new(op.clone_with_inputs(Some(Arc::from(new_inputs)), None))
    };

    rewrite(with_new_inputs, ctx)
}
