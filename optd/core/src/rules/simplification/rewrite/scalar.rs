use super::super::{
    rule::{RulePass, rewrite_bottom_up},
    scalar::simplify_scalar_recursively_with_ctx,
};
use crate::{
    error::Result,
    ir::{
        Column, ColumnSet, IRContext, Operator, Scalar, ScalarKind,
        convert::{IntoOperator, IntoScalar},
        operator::{DependentJoin, Join, Select},
        scalar::{BinaryOp, BinaryOpKind, ColumnRef, Literal},
    },
};
use std::sync::Arc;

/// Simplifies scalar inputs attached to each operator.
pub struct ScalarSimplificationRulePass;

impl RulePass for ScalarSimplificationRulePass {
    fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        rewrite_bottom_up(root, ctx, &|op, _ctx| {
            let op = simplify_attached_scalars(op, ctx);
            simplify_predicate_self_equalities(op, ctx)
        })
    }
}

// Simple scalar simplification
fn simplify_attached_scalars(op: Arc<Operator>, ctx: &IRContext) -> Arc<Operator> {
    let rewritten_scalars = op
        .input_scalars()
        .iter()
        .map(|scalar| simplify_scalar_recursively_with_ctx(scalar.clone(), ctx))
        .collect::<Vec<_>>();
    if rewritten_scalars.as_slice() == op.input_scalars() {
        op
    } else {
        Arc::new(op.clone_with_inputs(None, Some(Arc::from(rewritten_scalars))))
    }
}

// Simplifies "COLUMN = COLUMN" filtering predicates if it is known that
// COLUMN is non-NULL
fn simplify_predicate_self_equalities(op: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
    if let Ok(select) = op.try_borrow::<Select>() {
        let non_null_columns = non_nullable_output_columns(select.input(), ctx)?;
        let predicate =
            simplify_non_null_self_equalities(select.predicate().clone(), &non_null_columns);
        return Ok(if predicate == *select.predicate() {
            op
        } else {
            Select::new(select.input().clone(), predicate).into_operator()
        });
    }

    if let Ok(join) = op.try_borrow::<Join>() {
        let non_null_columns = non_nullable_output_columns(join.outer(), ctx)?
            | &non_nullable_output_columns(join.inner(), ctx)?;
        let join_cond =
            simplify_non_null_self_equalities(join.join_cond().clone(), &non_null_columns);
        return Ok(if join_cond == *join.join_cond() {
            op
        } else {
            Join::new(
                *join.join_type(),
                join.outer().clone(),
                join.inner().clone(),
                join_cond,
                join.implementation().clone(),
            )
            .into_operator()
        });
    }

    if let Ok(join) = op.try_borrow::<DependentJoin>() {
        let non_null_columns = non_nullable_output_columns(join.outer(), ctx)?
            | &non_nullable_output_columns(join.inner(), ctx)?;
        let join_cond =
            simplify_non_null_self_equalities(join.join_cond().clone(), &non_null_columns);
        return Ok(if join_cond == *join.join_cond() {
            op
        } else {
            DependentJoin::new(
                *join.join_type(),
                join.outer().clone(),
                join.inner().clone(),
                join_cond,
            )
            .into_operator()
        });
    }

    Ok(op)
}

fn non_nullable_output_columns(op: &Operator, ctx: &IRContext) -> Result<ColumnSet> {
    let columns = op.output_columns_in_order(ctx)?;
    let schema = op.output_schema(ctx)?;
    Ok(columns
        .into_iter()
        .zip(schema.inner().fields().iter())
        .filter_map(|(column, field)| (!field.is_nullable()).then_some(column))
        .collect())
}

fn simplify_non_null_self_equalities(
    scalar: Arc<Scalar>,
    non_null_columns: &ColumnSet,
) -> Arc<Scalar> {
    let rewritten_inputs = scalar
        .input_scalars()
        .iter()
        .map(|input| simplify_non_null_self_equalities(input.clone(), non_null_columns))
        .collect::<Vec<_>>();
    let rewritten = if rewritten_inputs.as_slice() == scalar.input_scalars() {
        scalar
    } else {
        Arc::new(scalar.clone_with_inputs(Some(Arc::from(rewritten_inputs)), None))
    };

    if let Ok(binary) = rewritten.try_borrow::<BinaryOp>()
        && binary.op_kind() == &BinaryOpKind::Eq
        && binary.lhs() == binary.rhs()
        && referenced_non_null_column(binary.lhs(), non_null_columns).is_some()
    {
        return Literal::boolean(true).into_scalar();
    }

    rewritten.simplify_nary_scalar()
}

fn referenced_non_null_column(
    scalar: &Arc<Scalar>,
    non_null_columns: &ColumnSet,
) -> Option<Column> {
    let ScalarKind::ColumnRef(meta) = &scalar.kind else {
        return None;
    };
    let column_ref = ColumnRef::borrow_raw_parts(meta, &scalar.common);
    non_null_columns
        .contains(column_ref.column())
        .then_some(*column_ref.column())
}
