use super::super::{
    rule::{RulePass, rewrite_bottom_up},
    scalar::{combine_conjuncts_simplified, split_conjuncts, substitute_columns},
};
use super::extract_projection_substitutions;
use crate::{
    error::Result,
    ir::{
        ColumnSet, IRContext, Operator,
        convert::IntoOperator,
        operator::{Join, Project, Select, join::JoinType},
    },
};
use std::sync::Arc;

/// Merges adjacent `Select` operators into one predicate.
pub struct MergeSelectRulePass;
/// Pushes a `Select` below a `Project` when the predicate can be rewritten.
pub struct PushSelectThroughProjectRulePass;
/// Pushes filter conjuncts from a `Select` into a `Join` and its inputs.
pub struct PushSelectThroughJoinRulePass;

impl RulePass for MergeSelectRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        rewrite_bottom_up(root, ctx, &|op, _ctx| {
            let Ok(select) = op.try_borrow::<Select>() else {
                return Ok(op);
            };
            let Ok(inner_select) = select.input().try_borrow::<Select>() else {
                return Ok(op);
            };

            let merged = combine_conjuncts_simplified(vec![
                inner_select.predicate().clone(),
                select.predicate().clone(),
            ]);
            Ok(Select::new(inner_select.input().clone(), merged).into_operator())
        })
    }
}

impl RulePass for PushSelectThroughProjectRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        rewrite_bottom_up(root, ctx, &|op, _ctx| {
            let Ok(select) = op.try_borrow::<Select>() else {
                return Ok(op);
            };
            let Ok(project) = select.input().try_borrow::<Project>() else {
                return Ok(op);
            };
            let Some(substitutions) =
                extract_projection_substitutions(*project.table_index(), project.projections())
            else {
                return Ok(op);
            };
            let outputs: ColumnSet = substitutions.keys().copied().collect();
            if !select.predicate().used_columns().is_subset(&outputs) {
                return Ok(op);
            }

            let pushed_predicate = substitute_columns(select.predicate().clone(), &substitutions);
            let pushed_input =
                Select::new(project.input().clone(), pushed_predicate).into_operator();
            Ok(Project::new(
                *project.table_index(),
                pushed_input,
                project.projections().clone(),
            )
            .into_operator())
        })
    }
}

impl RulePass for PushSelectThroughJoinRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        rewrite_bottom_up(root, ctx, &|op, ctx| {
            let Ok(select) = op.try_borrow::<Select>() else {
                return Ok(op);
            };
            let Ok(join) = select.input().try_borrow::<Join>() else {
                return Ok(op);
            };

            let outer_cols = join.outer().output_columns(ctx)?;
            let inner_cols = join.inner().output_columns(ctx)?;
            let mut outer_filters = Vec::new();
            let mut inner_filters = Vec::new();
            let mut join_conds = Vec::new();
            let mut top_filters = Vec::new();

            for cond in split_conjuncts(join.join_cond().clone()) {
                let used = cond.used_columns();
                if used.is_subset(outer_cols.as_ref()) {
                    outer_filters.push(cond);
                } else if used.is_subset(inner_cols.as_ref()) {
                    inner_filters.push(cond);
                } else {
                    join_conds.push(cond);
                }
            }

            for cond in split_conjuncts(select.predicate().clone()) {
                let used = cond.used_columns();
                match join.join_type() {
                    JoinType::Inner => {
                        if used.is_subset(outer_cols.as_ref()) {
                            outer_filters.push(cond);
                        } else if used.is_subset(inner_cols.as_ref()) {
                            inner_filters.push(cond);
                        } else {
                            join_conds.push(cond);
                        }
                    }
                    JoinType::Left => {
                        if used.is_subset(outer_cols.as_ref()) {
                            outer_filters.push(cond);
                        } else {
                            top_filters.push(cond);
                        }
                    }
                    JoinType::Single | JoinType::Mark(_) => top_filters.push(cond),
                }
            }

            let new_outer = if outer_filters.is_empty() {
                join.outer().clone()
            } else {
                Select::new(
                    join.outer().clone(),
                    combine_conjuncts_simplified(outer_filters),
                )
                .into_operator()
            };
            let new_inner = if inner_filters.is_empty() {
                join.inner().clone()
            } else {
                Select::new(
                    join.inner().clone(),
                    combine_conjuncts_simplified(inner_filters),
                )
                .into_operator()
            };
            let new_join_cond = combine_conjuncts_simplified(join_conds);
            let new_join = Join::new(
                *join.join_type(),
                new_outer,
                new_inner,
                new_join_cond,
                join.implementation().clone(),
            )
            .into_operator();

            if top_filters.is_empty() {
                Ok(new_join)
            } else {
                Ok(
                    Select::new(new_join, combine_conjuncts_simplified(top_filters))
                        .into_operator(),
                )
            }
        })
    }
}
