use crate::{
    error::Result,
    ir::{IRContext, Operator},
};

use std::sync::Arc;

/// A simplification pass that rewrites an operator tree into an equivalent,
/// typically simpler, form.
pub trait RulePass {
    /// Applies this simplification pass to `root` and returns the rewritten
    /// root operator.
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>>;
}

/// Recursively rewrites an operator tree in bottom-up order.
///
/// Child operators are rewritten first, then the current operator is rebuilt
/// only if any input changed, and finally `rewrite` is applied to the current
/// node.
pub fn rewrite_bottom_up<F>(
    op: Arc<Operator>,
    ctx: &IRContext,
    rewrite: &F,
) -> Result<Arc<Operator>>
where
    F: Fn(Arc<Operator>, &IRContext) -> Result<Arc<Operator>>,
{
    let new_inputs = op
        .input_operators()
        .iter()
        .map(|input| rewrite_bottom_up(input.clone(), ctx, rewrite))
        .collect::<Result<Vec<_>>>()?;

    let op_with_new_inputs = if new_inputs.as_slice() == op.input_operators() {
        op
    } else {
        Arc::new(op.clone_with_inputs(Some(Arc::from(new_inputs)), None))
    };

    rewrite(op_with_new_inputs, ctx)
}
