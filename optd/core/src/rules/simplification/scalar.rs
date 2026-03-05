use std::{collections::HashMap, sync::Arc};
use crate::ir::{
    Column, Scalar,
    convert::IntoScalar,
    scalar::{ColumnRef, NaryOp},
};

pub(super) fn split_conjuncts(predicate: Arc<Scalar>) -> Vec<Arc<Scalar>> {
    if let Ok(and) = predicate.try_borrow::<NaryOp>()
        && and.is_and()
    {
        and.terms()
            .iter()
            .flat_map(|term| split_conjuncts(term.clone()))
            .collect()
    } else {
        vec![predicate]
    }
}

pub(super) fn combine_conjuncts_simplified(predicates: Vec<Arc<Scalar>>) -> Arc<Scalar> {
    simplify_scalar_recursively(Scalar::combine_conjuncts(predicates))
}

pub(super) fn substitute_columns(
    scalar: Arc<Scalar>,
    substitutions: &HashMap<Column, Arc<Scalar>>,
) -> Arc<Scalar> {
    let rewritten = if let Ok(column_ref) = scalar.try_borrow::<ColumnRef>() {
        substitutions
            .get(column_ref.column())
            .cloned()
            .unwrap_or(scalar)
    } else {
        let new_inputs = scalar
            .input_scalars()
            .iter()
            .map(|input| substitute_columns(input.clone(), substitutions))
            .collect::<Vec<_>>();
        if new_inputs.as_slice() == scalar.input_scalars() {
            scalar
        } else {
            Arc::new(scalar.clone_with_inputs(Some(Arc::from(new_inputs)), None))
        }
    };
    simplify_scalar_recursively(rewritten)
}

pub(super) fn simplify_scalar_recursively(scalar: Arc<Scalar>) -> Arc<Scalar> {
    let rewritten_inputs = scalar
        .input_scalars()
        .iter()
        .map(|input| simplify_scalar_recursively(input.clone()))
        .collect::<Vec<_>>();
    let mut rewritten = if rewritten_inputs.as_slice() == scalar.input_scalars() {
        scalar
    } else {
        Arc::new(scalar.clone_with_inputs(Some(Arc::from(rewritten_inputs)), None))
    };

    if let Ok(nary) = rewritten.try_borrow::<NaryOp>() {
        let op_kind = *nary.op_kind();
        let mut flattened = Vec::new();
        let mut changed = false;
        for term in nary.terms() {
            if let Ok(child_nary) = term.try_borrow::<NaryOp>()
                && child_nary.op_kind() == &op_kind
            {
                flattened.extend(child_nary.terms().iter().cloned());
                changed = true;
            } else {
                flattened.push(term.clone());
            }
        }
        if changed {
            rewritten = NaryOp::new(op_kind, Arc::from(flattened)).into_scalar();
        }
    }

    rewritten.simplify_nary_scalar()
}
