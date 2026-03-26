use super::super::{
    rule::{RulePass, rewrite_bottom_up},
    scalar::simplify_scalar_recursively,
};
use crate::{
    error::Result,
    ir::{IRContext, Operator},
};
use std::sync::Arc;

/// Simplifies scalar inputs attached to each operator.
pub struct ScalarSimplificationRulePass;

impl RulePass for ScalarSimplificationRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        rewrite_bottom_up(root, ctx, &|op, _ctx| {
            let rewritten_scalars = op
                .input_scalars()
                .iter()
                .map(|scalar| simplify_scalar_recursively(scalar.clone()))
                .collect::<Vec<_>>();
            if rewritten_scalars.as_slice() == op.input_scalars() {
                Ok(op)
            } else {
                Ok(Arc::new(op.clone_with_inputs(
                    None,
                    Some(Arc::from(rewritten_scalars)),
                )))
            }
        })
    }
}
