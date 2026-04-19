use crate::ir::{
    Column, Scalar, ScalarValue,
    convert::IntoScalar,
    scalar::{BinaryOp, BinaryOpKind, Cast, ColumnRef, Literal, NaryOp},
};
use arrow::datatypes::IntervalMonthDayNano;
use chrono::{Duration, Months, NaiveDate};
use std::{collections::HashMap, sync::Arc};

/// Splits a predicate into a flat list of conjuncts.
pub fn split_conjuncts(predicate: Arc<Scalar>) -> Vec<Arc<Scalar>> {
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

/// Splits a predicate into a flat list of disjuncts.
pub fn split_disjuncts(predicate: Arc<Scalar>) -> Vec<Arc<Scalar>> {
    if let Ok(or) = predicate.try_borrow::<NaryOp>()
        && or.is_or()
    {
        or.terms()
            .iter()
            .flat_map(|term| split_disjuncts(term.clone()))
            .collect()
    } else {
        vec![predicate]
    }
}

/// Combines conjuncts and simplifies the result.
pub fn combine_conjuncts_simplified(predicates: Vec<Arc<Scalar>>) -> Arc<Scalar> {
    simplify_scalar_recursively(Scalar::combine_conjuncts(predicates))
}

/// Combines disjuncts and simplifies the result.
pub fn combine_disjuncts_simplified(predicates: Vec<Arc<Scalar>>) -> Arc<Scalar> {
    simplify_scalar_recursively(Scalar::combine_disjuncts(predicates))
}

/// Replaces column references using `substitutions` and simplifies the result.
pub fn substitute_columns(
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

/// Simplifies a scalar expression bottom-up.
pub fn simplify_scalar_recursively(scalar: Arc<Scalar>) -> Arc<Scalar> {
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

    if let Some(simplified) = factor_common_conjuncts_from_or(&rewritten)
        .or_else(|| simplify_cast(&rewritten))
        .or_else(|| simplify_literal_binary_op(&rewritten))
    {
        rewritten = simplified;
    }

    rewritten.simplify_nary_scalar()
}

fn factor_common_conjuncts_from_or(scalar: &Arc<Scalar>) -> Option<Arc<Scalar>> {
    let disjuncts = split_disjuncts(scalar.clone());
    if disjuncts.len() < 2 {
        return None;
    }

    let disjunct_conjuncts = disjuncts
        .iter()
        .map(|term| split_conjuncts(term.clone()))
        .collect::<Vec<_>>();

    let mut common = Vec::new();
    for candidate in &disjunct_conjuncts[0] {
        if common.contains(candidate) {
            continue;
        }
        if disjunct_conjuncts[1..]
            .iter()
            .all(|conjuncts| conjuncts.contains(candidate))
        {
            common.push(candidate.clone());
        }
    }

    if common.is_empty() {
        return None;
    }

    let reduced_disjuncts = disjunct_conjuncts
        .into_iter()
        .map(|conjuncts| {
            let residual = conjuncts
                .into_iter()
                .filter(|conjunct| !common.contains(conjunct))
                .collect::<Vec<_>>();
            Scalar::combine_conjuncts(residual)
        })
        .collect::<Vec<_>>();

    let mut conjuncts = common;
    conjuncts.push(combine_disjuncts_simplified(reduced_disjuncts));
    Some(combine_conjuncts_simplified(conjuncts))
}

fn simplify_cast(scalar: &Arc<Scalar>) -> Option<Arc<Scalar>> {
    let cast = scalar.try_borrow::<Cast>().ok()?;
    let expr = cast.expr().clone();

    if let Ok(inner_cast) = expr.try_borrow::<Cast>()
        && inner_cast.data_type() == cast.data_type()
    {
        return Some(expr);
    }

    let literal = expr.try_borrow::<Literal>().ok()?;
    literal
        .value()
        .cast_to(cast.data_type())
        .ok()
        .map(|value| Literal::new(value).into_scalar())
}

fn simplify_literal_binary_op(scalar: &Arc<Scalar>) -> Option<Arc<Scalar>> {
    fn literal_value(scalar: &Arc<Scalar>) -> Option<ScalarValue> {
        scalar
            .try_borrow::<Literal>()
            .ok()
            .map(|literal| literal.value().clone())
    }

    let binary = scalar.try_borrow::<BinaryOp>().ok()?;
    let lhs = literal_value(binary.lhs())?;
    let rhs = literal_value(binary.rhs())?;

    fold_date_interval_arithmetic(*binary.op_kind(), &lhs, &rhs)
        .map(|value| Literal::new(value).into_scalar())
}

fn fold_date_interval_arithmetic(
    op_kind: BinaryOpKind,
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> Option<ScalarValue> {
    fn negate_interval(interval: IntervalMonthDayNano) -> Option<IntervalMonthDayNano> {
        Some(IntervalMonthDayNano {
            months: interval.months.checked_neg()?,
            days: interval.days.checked_neg()?,
            nanoseconds: interval.nanoseconds.checked_neg()?,
        })
    }

    match (op_kind, lhs, rhs) {
        (
            BinaryOpKind::Plus,
            ScalarValue::Date32(Some(days)),
            ScalarValue::IntervalMonthDayNano(Some(interval)),
        )
        | (
            BinaryOpKind::Plus,
            ScalarValue::IntervalMonthDayNano(Some(interval)),
            ScalarValue::Date32(Some(days)),
        ) => apply_interval_to_date(*days, *interval).map(|days| ScalarValue::Date32(Some(days))),
        (
            BinaryOpKind::Minus,
            ScalarValue::Date32(Some(days)),
            ScalarValue::IntervalMonthDayNano(Some(interval)),
        ) => negate_interval(*interval)
            .and_then(|interval| apply_interval_to_date(*days, interval))
            .map(|days| ScalarValue::Date32(Some(days))),
        _ => None,
    }
}

fn apply_interval_to_date(date_days: i32, interval: IntervalMonthDayNano) -> Option<i32> {
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    if interval.nanoseconds % NANOS_PER_DAY != 0 {
        return None;
    }

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let mut date = epoch.checked_add_signed(Duration::try_days(date_days as i64)?)?;

    date = if interval.months >= 0 {
        date.checked_add_months(Months::new(interval.months as u32))
    } else {
        date.checked_sub_months(Months::new(interval.months.checked_abs()? as u32))
    }?;

    let extra_days = interval.nanoseconds / NANOS_PER_DAY;
    let total_days = (interval.days as i64).checked_add(extra_days)?;
    date = date.checked_add_signed(Duration::try_days(total_days)?)?;

    i32::try_from(date.signed_duration_since(epoch).num_days()).ok()
}
