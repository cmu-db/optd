use super::super::{
    rule::{RulePass, rewrite_bottom_up},
    scalar::substitute_columns,
};
use super::extract_projection_substitutions;
use crate::{
    error::Result,
    ir::{
        IRContext, Operator,
        convert::{IntoOperator, IntoScalar},
        operator::Project,
        scalar::List,
    },
};
use std::sync::Arc;

/// Merges adjacent `Project` operators into one projection list.
pub struct MergeProjectRulePass;

impl RulePass for MergeProjectRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        rewrite_bottom_up(root, ctx, &|op, _ctx| {
            let Ok(project) = op.try_borrow::<Project>() else {
                return Ok(op);
            };
            let Ok(inner_project) = project.input().try_borrow::<Project>() else {
                return Ok(op);
            };
            let Some(substitutions) = extract_projection_substitutions(
                *inner_project.table_index(),
                inner_project.projections(),
            ) else {
                return Ok(op);
            };
            let Ok(outer_list) = project.projections().try_borrow::<List>() else {
                return Ok(op);
            };

            let merged_members = outer_list
                .members()
                .iter()
                .map(|member| substitute_columns(member.clone(), &substitutions))
                .collect::<Vec<_>>();

            Ok(Project::new(
                *project.table_index(),
                inner_project.input().clone(),
                List::new(Arc::from(merged_members)).into_scalar(),
            )
            .into_operator())
        })
    }
}
