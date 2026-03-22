use super::{rule::RulePass, scalar::simplify_scalar_recursively};
use crate::{
    error::Result,
    ir::{
        Column, ColumnSet, IRContext, Operator,
        convert::IntoOperator,
        operator::{
            Aggregate, DependentJoin, Get, Join, OperatorKind, OrderBy, Project, Remap, Select,
            Subquery,
        },
        scalar::List,
    },
};
use std::sync::Arc;

/// Prunes unused columns from operator inputs.
pub struct ColumnPruningRulePass;

impl RulePass for ColumnPruningRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        let required = root.output_columns(ctx)?;
        prune_operator(root, required.as_ref(), ctx)
    }
}

fn prune_operator(
    op: Arc<Operator>,
    required: &ColumnSet,
    ctx: &IRContext,
) -> Result<Arc<Operator>> {
    Ok(match &op.kind {
        OperatorKind::Select(meta) => {
            let select = Select::borrow_raw_parts(meta, &op.common);
            let predicate = simplify_scalar_recursively(select.predicate().clone());

            let mut input_required = required.clone();
            input_required |= &predicate.used_columns();
            let new_input = prune_operator(select.input().clone(), &input_required, ctx)?;

            if predicate.is_true_scalar() {
                new_input
            } else {
                Select::new(new_input, predicate).into_operator()
            }
        }
        OperatorKind::Project(meta) => {
            let project = Project::borrow_raw_parts(meta, &op.common);
            let list = project.projections().try_borrow::<List>().unwrap();
            let members = list.members();
            let mut input_required = ColumnSet::default();
            for member in members.iter() {
                input_required |= &member.used_columns();
            }

            let new_input = prune_operator(project.input().clone(), &input_required, ctx)?;
            Project::new(
                *project.table_index(),
                new_input,
                project.projections().clone(),
            )
            .into_operator()
        }
        OperatorKind::Remap(meta) => {
            let remap = Remap::borrow_raw_parts(meta, &op.common);
            let input_required = remap.input().output_columns(ctx)?.as_ref().clone();

            let new_input = prune_operator(remap.input().clone(), &input_required, ctx)?;
            Remap::new(*remap.table_index(), new_input).into_operator()
        }
        OperatorKind::Aggregate(meta) => {
            let agg = Aggregate::borrow_raw_parts(meta, &op.common);
            let exprs_list = agg.exprs().try_borrow::<List>().unwrap();
            let exprs = exprs_list.members();
            let keys_list = agg.keys().borrow::<List>();
            let keys = keys_list.members();

            let mut input_required = ColumnSet::default();
            for expr in exprs.iter() {
                input_required |= &expr.used_columns();
            }
            for key in keys.iter() {
                input_required |= &key.used_columns();
            }

            let new_input = prune_operator(agg.input().clone(), &input_required, ctx)?;
            Aggregate::new(
                *agg.aggregate_table_index(),
                new_input,
                agg.exprs().clone(),
                agg.keys().clone(),
                *agg.implementation(),
            )
            .into_operator()
        }
        OperatorKind::Join(meta) => {
            let join = Join::borrow_raw_parts(meta, &op.common);
            let outer_cols = join.outer().output_columns(ctx)?;
            let inner_cols = join.inner().output_columns(ctx)?;
            let used = join.join_cond().used_columns();

            let mut outer_required = required.clone() & outer_cols.as_ref();
            let mut inner_required = required.clone() & inner_cols.as_ref();
            outer_required |= &(used.clone() & outer_cols.as_ref());
            inner_required |= &(used & inner_cols.as_ref());

            let new_outer = prune_operator(join.outer().clone(), &outer_required, ctx)?;
            let new_inner = prune_operator(join.inner().clone(), &inner_required, ctx)?;
            Join::new(
                *join.join_type(),
                new_outer,
                new_inner,
                join.join_cond().clone(),
                join.implementation().clone(),
            )
            .into_operator()
        }
        OperatorKind::DependentJoin(meta) => {
            let join = DependentJoin::borrow_raw_parts(meta, &op.common);
            let outer_cols = join.outer().output_columns(ctx)?;
            let inner_cols = join.inner().output_columns(ctx)?;
            let used = join.join_cond().used_columns();

            let mut outer_required = required.clone() & outer_cols.as_ref();
            let mut inner_required = required.clone() & inner_cols.as_ref();
            outer_required |= &(used.clone() & outer_cols.as_ref());
            inner_required |= &(used & inner_cols.as_ref());

            let new_outer = prune_operator(join.outer().clone(), &outer_required, ctx)?;
            let new_inner = prune_operator(join.inner().clone(), &inner_required, ctx)?;
            DependentJoin::new(
                *join.join_type(),
                new_outer,
                new_inner,
                join.join_cond().clone(),
            )
            .into_operator()
        }
        OperatorKind::OrderBy(meta) => {
            let order_by = OrderBy::borrow_raw_parts(meta, &op.common);
            let mut input_required = required.clone();
            for expr in order_by.exprs().iter() {
                input_required |= &expr.used_columns();
            }
            let new_input = prune_operator(order_by.input().clone(), &input_required, ctx)?;
            Arc::new(op.clone_with_inputs(Some(Arc::new([new_input])), None))
        }
        OperatorKind::Get(meta) => {
            let get = Get::borrow_raw_parts(meta, &op.common);
            let mut kept = get
                .projections()
                .iter()
                .copied()
                .filter(|index| {
                    let column = Column(*get.table_index(), *index);
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
                Get::new(
                    *get.data_source_id(),
                    *get.table_index(),
                    Arc::from(kept),
                    *get.implementation(),
                )
                .into_operator()
            }
        }
        OperatorKind::Subquery(meta) => {
            let subquery = Subquery::borrow_raw_parts(meta, &op.common);
            let new_input = prune_operator(subquery.input().clone(), required, ctx)?;
            Subquery::new(new_input).into_operator()
        }
        _ => {
            let new_inputs = op
                .input_operators()
                .iter()
                .map(|input| {
                    let all_cols = input.output_columns(ctx)?;
                    prune_operator(input.clone(), all_cols.as_ref(), ctx)
                })
                .collect::<Result<Vec<_>>>()?;
            if new_inputs.as_slice() == op.input_operators() {
                op
            } else {
                Arc::new(op.clone_with_inputs(Some(Arc::from(new_inputs)), None))
            }
        }
    })
}
