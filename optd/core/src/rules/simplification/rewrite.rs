use std::{collections::HashMap, sync::Arc};
use crate::ir::{
    Column, ColumnSet, IRContext, Operator, Scalar,
    convert::{IntoOperator, IntoScalar},
    operator::{LogicalJoin, LogicalProject, LogicalSelect, join::JoinType},
    scalar::{ColumnAssign, ColumnRef, List},
};
use super::{
    rule::{RulePass, rewrite_bottom_up},
    scalar::{
        combine_conjuncts_simplified, simplify_scalar_recursively, split_conjuncts,
        substitute_columns,
    },
};

pub(super) struct ScalarSimplificationRulePass;
pub(super) struct MergeSelectRulePass;
pub(super) struct PushSelectThroughProjectRulePass;
pub(super) struct PushSelectThroughJoinRulePass;
pub(super) struct MergeProjectRulePass;

impl RulePass for ScalarSimplificationRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
        rewrite_bottom_up(root, ctx, &|op, _ctx| {
            let rewritten_scalars = op
                .input_scalars()
                .iter()
                .map(|scalar| simplify_scalar_recursively(scalar.clone()))
                .collect::<Vec<_>>();
            if rewritten_scalars.as_slice() == op.input_scalars() {
                op
            } else {
                Arc::new(op.clone_with_inputs(None, Some(Arc::from(rewritten_scalars))))
            }
        })
    }
}

impl RulePass for MergeSelectRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
        rewrite_bottom_up(root, ctx, &|op, _ctx| {
            let Ok(select) = op.try_borrow::<LogicalSelect>() else {
                return op;
            };
            let Ok(inner_select) = select.input().try_borrow::<LogicalSelect>() else {
                return op;
            };

            let merged = combine_conjuncts_simplified(vec![
                inner_select.predicate().clone(),
                select.predicate().clone(),
            ]);
            LogicalSelect::new(inner_select.input().clone(), merged).into_operator()
        })
    }
}

impl RulePass for PushSelectThroughProjectRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
        rewrite_bottom_up(root, ctx, &|op, _ctx| {
            let Ok(select) = op.try_borrow::<LogicalSelect>() else {
                return op;
            };
            let Ok(project) = select.input().try_borrow::<LogicalProject>() else {
                return op;
            };
            let Some(substitutions) = extract_projection_substitutions(project.projections())
            else {
                return op;
            };
            let outputs: ColumnSet = substitutions.keys().copied().collect();
            if !select.predicate().used_columns().is_subset(&outputs) {
                return op;
            }

            let pushed_predicate = substitute_columns(select.predicate().clone(), &substitutions);
            let pushed_input =
                LogicalSelect::new(project.input().clone(), pushed_predicate).into_operator();
            LogicalProject::new(pushed_input, project.projections().clone()).into_operator()
        })
    }
}

impl RulePass for PushSelectThroughJoinRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
        rewrite_bottom_up(root, ctx, &|op, rule_ctx| {
            let Ok(select) = op.try_borrow::<LogicalSelect>() else {
                return op;
            };
            let Ok(join) = select.input().try_borrow::<LogicalJoin>() else {
                return op;
            };

            let outer_cols = join.outer().output_columns(rule_ctx);
            let inner_cols = join.inner().output_columns(rule_ctx);
            let mut outer_filters = Vec::new();
            let mut inner_filters = Vec::new();
            let mut join_conds = Vec::new();
            let mut top_filters = Vec::new();

            for cond in split_conjuncts(join.join_cond().clone()) {
                let used = cond.used_columns();
                if used.is_subset(&outer_cols) {
                    outer_filters.push(cond);
                } else if used.is_subset(&inner_cols) {
                    inner_filters.push(cond);
                } else {
                    join_conds.push(cond);
                }
            }

            for cond in split_conjuncts(select.predicate().clone()) {
                let used = cond.used_columns();
                match join.join_type() {
                    JoinType::Inner => {
                        if used.is_subset(&outer_cols) {
                            outer_filters.push(cond);
                        } else if used.is_subset(&inner_cols) {
                            inner_filters.push(cond);
                        } else {
                            join_conds.push(cond);
                        }
                    }
                    JoinType::Left => {
                        if used.is_subset(&outer_cols) {
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
                LogicalSelect::new(join.outer().clone(), combine_conjuncts_simplified(outer_filters)).into_operator()
            };
            let new_inner = if inner_filters.is_empty() {
                join.inner().clone()
            } else {
                LogicalSelect::new(join.inner().clone(), combine_conjuncts_simplified(inner_filters)).into_operator()
            };
            let new_join_cond = combine_conjuncts_simplified(join_conds);
            let new_join =
                LogicalJoin::new(*join.join_type(), new_outer, new_inner, new_join_cond).into_operator();

            if top_filters.is_empty() {
                new_join
            } else {
                LogicalSelect::new(new_join, combine_conjuncts_simplified(top_filters)).into_operator()
            }
        })
    }
}

impl RulePass for MergeProjectRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
        rewrite_bottom_up(root, ctx, &|op, _ctx| {
            let Ok(project) = op.try_borrow::<LogicalProject>() else {
                return op;
            };
            let Ok(inner_project) = project.input().try_borrow::<LogicalProject>() else {
                return op;
            };
            let Some(substitutions) = extract_projection_substitutions(inner_project.projections())
            else {
                return op;
            };
            let Ok(outer_list) = project.projections().try_borrow::<List>() else {
                return op;
            };

            let mut merged_members = Vec::with_capacity(outer_list.members().len());
            for member in outer_list.members() {
                if let Ok(assign) = member.try_borrow::<ColumnAssign>() {
                    let expr = substitute_columns(assign.expr().clone(), &substitutions);
                    merged_members.push(ColumnAssign::new(*assign.column(), expr).into_scalar());
                } else if let Ok(column_ref) = member.try_borrow::<ColumnRef>() {
                    let expr = substitutions
                        .get(column_ref.column())
                        .cloned()
                        .unwrap_or_else(|| member.clone());
                    merged_members
                        .push(ColumnAssign::new(*column_ref.column(), expr).into_scalar());
                } else {
                    return op;
                }
            }

            LogicalProject::new(
                inner_project.input().clone(),
                List::new(Arc::from(merged_members)).into_scalar(),
            )
            .into_operator()
        })
    }
}

fn extract_projection_substitutions(
    projections: &Arc<Scalar>,
) -> Option<HashMap<Column, Arc<Scalar>>> {
    let list = projections.try_borrow::<List>().ok()?;
    let mut substitutions = HashMap::with_capacity(list.members().len());
    for member in list.members() {
        if let Ok(assign) = member.try_borrow::<ColumnAssign>() {
            substitutions.insert(*assign.column(), assign.expr().clone());
        } else if let Ok(column_ref) = member.try_borrow::<ColumnRef>() {
            substitutions.insert(*column_ref.column(), member.clone());
        } else {
            return None;
        }
    }
    Some(substitutions)
}
