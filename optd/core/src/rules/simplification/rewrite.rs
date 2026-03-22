use super::{
    rule::{RulePass, rewrite_bottom_up},
    scalar::{
        combine_conjuncts_simplified, simplify_scalar_recursively, split_conjuncts,
        substitute_columns,
    },
};
use crate::{
    error::Result,
    ir::{
        Column, ColumnSet, IRContext, Operator, Scalar,
        convert::{IntoOperator, IntoScalar},
        operator::{Join, Project, Select, join::JoinType},
        scalar::List,
    },
};
use std::{collections::HashMap, sync::Arc};

pub(super) struct ScalarSimplificationRulePass;
pub(super) struct MergeSelectRulePass;
pub(super) struct PushSelectThroughProjectRulePass;
pub(super) struct PushSelectThroughJoinRulePass;
pub(super) struct MergeProjectRulePass;

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
        rewrite_bottom_up(root, ctx, &|op, rule_ctx| {
            let Ok(select) = op.try_borrow::<Select>() else {
                return Ok(op);
            };
            let Ok(join) = select.input().try_borrow::<Join>() else {
                return Ok(op);
            };

            let outer_cols = join.outer().output_columns(rule_ctx)?;
            let inner_cols = join.inner().output_columns(rule_ctx)?;
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

fn extract_projection_substitutions(
    table_index: i64,
    projections: &Arc<Scalar>,
) -> Option<HashMap<Column, Arc<Scalar>>> {
    let list = projections.try_borrow::<List>().ok()?;
    let mut substitutions = HashMap::with_capacity(list.members().len());
    for (idx, member) in list.members().iter().enumerate() {
        substitutions.insert(Column(table_index, idx), member.clone());
    }
    Some(substitutions)
}
