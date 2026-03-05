use super::{rule::RulePass, scalar::simplify_scalar_recursively};
use crate::ir::{
    Column, ColumnSet, IRContext, Operator,
    convert::{IntoOperator, IntoScalar},
    operator::{
        LogicalAggregate, LogicalDependentJoin, LogicalGet, LogicalJoin, LogicalOrderBy,
        LogicalProject, LogicalRemap, LogicalSelect, OperatorKind,
    },
    scalar::{ColumnAssign, ColumnRef, List},
};
use std::sync::Arc;

pub(super) struct ColumnPruningRulePass;

impl RulePass for ColumnPruningRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
        let required = root.output_columns(ctx).as_ref().clone();
        prune_operator(root, &required, ctx)
    }
}

fn prune_operator(op: Arc<Operator>, required: &ColumnSet, ctx: &IRContext) -> Arc<Operator> {
    match &op.kind {
        OperatorKind::LogicalSelect(meta) => {
            let select = LogicalSelect::borrow_raw_parts(meta, &op.common);
            let predicate = simplify_scalar_recursively(select.predicate().clone());

            let mut input_required = required.clone();
            input_required |= &predicate.used_columns();
            let new_input = prune_operator(select.input().clone(), &input_required, ctx);

            if predicate.is_true_scalar() {
                new_input
            } else {
                LogicalSelect::new(new_input, predicate).into_operator()
            }
        }
        OperatorKind::LogicalProject(meta) => {
            let project = LogicalProject::borrow_raw_parts(meta, &op.common);
            let list = project.projections().try_borrow::<List>().unwrap();
            let members = list.members();
            let mut kept = Vec::new();
            let mut input_required = ColumnSet::default();
            for member in members {
                let keep = if let Ok(assign) = member.try_borrow::<ColumnAssign>() {
                    Some(*assign.column())
                } else if let Ok(column_ref) = member.try_borrow::<ColumnRef>() {
                    Some(*column_ref.column())
                } else {
                    None
                }
                .map(|column| required.contains(&column))
                .unwrap_or(true);
                if keep {
                    kept.push(member.clone());
                    input_required |= &member.used_columns();
                }
            }
            if kept.is_empty()
                && let Some(first) = members.first()
            {
                kept.push(first.clone());
                input_required |= &first.used_columns();
            }

            let new_input = prune_operator(project.input().clone(), &input_required, ctx);
            let new_projections = if kept.as_slice() == members {
                project.projections().clone()
            } else {
                List::new(Arc::from(kept)).into_scalar()
            };
            LogicalProject::new(new_input, new_projections).into_operator()
        }
        OperatorKind::LogicalRemap(meta) => {
            let remap = LogicalRemap::borrow_raw_parts(meta, &op.common);
            let list = remap.mappings().try_borrow::<List>().unwrap();
            let members = list.members();
            let mut kept = Vec::new();
            let mut input_required = ColumnSet::default();
            for member in members {
                let keep = member
                    .try_borrow::<ColumnAssign>()
                    .map(|assign| required.contains(assign.column()))
                    .unwrap_or(true);
                if keep {
                    kept.push(member.clone());
                    input_required |= &member.used_columns();
                }
            }
            if kept.is_empty()
                && let Some(first) = members.first()
            {
                kept.push(first.clone());
                input_required |= &first.used_columns();
            }

            let new_input = prune_operator(remap.input().clone(), &input_required, ctx);
            let new_mappings = if kept.as_slice() == members {
                remap.mappings().clone()
            } else {
                List::new(Arc::from(kept)).into_scalar()
            };
            LogicalRemap::new(new_input, new_mappings).into_operator()
        }
        OperatorKind::LogicalAggregate(meta) => {
            let agg = LogicalAggregate::borrow_raw_parts(meta, &op.common);
            let exprs_list = agg.exprs().try_borrow::<List>().unwrap();
            let exprs = exprs_list.members();
            let keys_list = agg.keys().try_borrow::<List>().unwrap();
            let keys = keys_list.members();

            let mut kept_exprs = exprs
                .iter()
                .filter(|expr| {
                    expr.try_borrow::<ColumnAssign>()
                        .map(|assign| required.contains(assign.column()))
                        .unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>();

            if kept_exprs.is_empty()
                && keys.is_empty()
                && let Some(first) = exprs.first()
            {
                kept_exprs.push(first.clone());
            }

            let mut input_required = ColumnSet::default();
            for expr in &kept_exprs {
                input_required |= &expr.used_columns();
            }
            for key in keys {
                input_required |= &key.used_columns();
            }

            let new_input = prune_operator(agg.input().clone(), &input_required, ctx);
            let new_exprs = if kept_exprs.as_slice() == exprs {
                agg.exprs().clone()
            } else {
                List::new(Arc::from(kept_exprs)).into_scalar()
            };
            LogicalAggregate::new(new_input, new_exprs, agg.keys().clone()).into_operator()
        }
        OperatorKind::LogicalJoin(meta) => {
            let join = LogicalJoin::borrow_raw_parts(meta, &op.common);
            let outer_cols = join.outer().output_columns(ctx);
            let inner_cols = join.inner().output_columns(ctx);
            let used = join.join_cond().used_columns();

            let mut outer_required = required.clone() & outer_cols.as_ref();
            let mut inner_required = required.clone() & inner_cols.as_ref();
            outer_required |= &(used.clone() & outer_cols.as_ref());
            inner_required |= &(used & inner_cols.as_ref());

            let new_outer = prune_operator(join.outer().clone(), &outer_required, ctx);
            let new_inner = prune_operator(join.inner().clone(), &inner_required, ctx);
            LogicalJoin::new(
                *join.join_type(),
                new_outer,
                new_inner,
                join.join_cond().clone(),
            )
            .into_operator()
        }
        OperatorKind::LogicalDependentJoin(meta) => {
            let join = LogicalDependentJoin::borrow_raw_parts(meta, &op.common);
            let outer_cols = join.outer().output_columns(ctx);
            let inner_cols = join.inner().output_columns(ctx);
            let used = join.join_cond().used_columns();

            let mut outer_required = required.clone() & outer_cols.as_ref();
            let mut inner_required = required.clone() & inner_cols.as_ref();
            outer_required |= &(used.clone() & outer_cols.as_ref());
            inner_required |= &(used & inner_cols.as_ref());

            let new_outer = prune_operator(join.outer().clone(), &outer_required, ctx);
            let new_inner = prune_operator(join.inner().clone(), &inner_required, ctx);
            LogicalDependentJoin::new(
                *join.join_type(),
                new_outer,
                new_inner,
                join.join_cond().clone(),
            )
            .into_operator()
        }
        OperatorKind::LogicalOrderBy(meta) => {
            let order_by = LogicalOrderBy::borrow_raw_parts(meta, &op.common);
            let mut input_required = required.clone();
            for expr in order_by.exprs() {
                input_required |= &expr.used_columns();
            }
            let new_input = prune_operator(order_by.input().clone(), &input_required, ctx);
            Arc::new(op.clone_with_inputs(Some(Arc::new([new_input])), None))
        }
        OperatorKind::LogicalGet(meta) => {
            let get = LogicalGet::borrow_raw_parts(meta, &op.common);
            let mut kept = get
                .projections()
                .iter()
                .copied()
                .filter(|index| {
                    let column = Column(get.first_column().0 + index);
                    required.contains(&column)
                })
                .collect::<Vec<_>>();
            if kept.is_empty()
                && let Some(first) = get.projections().first()
            {
                kept.push(*first);
            }
            if kept.as_slice() == get.projections().as_ref() {
                op
            } else {
                LogicalGet::new(*get.source(), *get.first_column(), Arc::from(kept)).into_operator()
            }
        }
        OperatorKind::LogicalSubquery(_) => {
            let input = op.input_operators()[0].clone();
            let input_required = required.clone();
            let new_input = prune_operator(input, &input_required, ctx);
            Arc::new(op.clone_with_inputs(Some(Arc::new([new_input])), None))
        }
        _ => {
            let new_inputs = op
                .input_operators()
                .iter()
                .map(|input| {
                    let all_cols = input.output_columns(ctx).as_ref().clone();
                    prune_operator(input.clone(), &all_cols, ctx)
                })
                .collect::<Vec<_>>();
            if new_inputs.as_slice() == op.input_operators() {
                op
            } else {
                Arc::new(op.clone_with_inputs(Some(Arc::from(new_inputs)), None))
            }
        }
    }
}
