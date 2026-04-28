use crate::ir::{
    Column, DataType, IRContext, Scalar, ScalarKind, ScalarValue,
    convert::IntoScalar,
    scalar::{
        BinaryOp, BinaryOpKind, Case, Cast, ColumnRef, Function, InList, List, Literal, NaryOp,
    },
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
    simplify_scalar_recursively_internal(scalar, None)
}

/// Simplifies a scalar expression bottom-up using `ctx` for type-aware rewrites.
pub fn simplify_scalar_recursively_with_ctx(scalar: Arc<Scalar>, ctx: &IRContext) -> Arc<Scalar> {
    simplify_scalar_recursively_internal(scalar, Some(ctx))
}

fn simplify_scalar_recursively_internal(
    scalar: Arc<Scalar>,
    ctx: Option<&IRContext>,
) -> Arc<Scalar> {
    let rewritten_inputs = scalar
        .input_scalars()
        .iter()
        .map(|input| simplify_scalar_recursively_internal(input.clone(), ctx))
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

    if let Some(simplified) = factor_common_conjuncts_from_or(&rewritten, ctx)
        .or_else(|| simplify_cast_in_comparison(&rewritten, ctx))
        .or_else(|| simplify_cast_in_list(&rewritten, ctx))
        .or_else(|| simplify_cast(&rewritten))
        .or_else(|| simplify_literal_binary_op(&rewritten))
    {
        rewritten = simplified;
    }

    rewritten.simplify_nary_scalar()
}

fn factor_common_conjuncts_from_or(
    scalar: &Arc<Scalar>,
    ctx: Option<&IRContext>,
) -> Option<Arc<Scalar>> {
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
    conjuncts.push(match ctx {
        Some(ctx) => {
            simplify_scalar_recursively_with_ctx(Scalar::combine_disjuncts(reduced_disjuncts), ctx)
        }
        None => combine_disjuncts_simplified(reduced_disjuncts),
    });
    Some(match ctx {
        Some(ctx) => {
            simplify_scalar_recursively_with_ctx(Scalar::combine_conjuncts(conjuncts), ctx)
        }
        None => combine_conjuncts_simplified(conjuncts),
    })
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

fn simplify_cast_in_comparison(
    scalar: &Arc<Scalar>,
    ctx: Option<&IRContext>,
) -> Option<Arc<Scalar>> {
    let ctx = ctx?;
    let binary = scalar.try_borrow::<BinaryOp>().ok()?;
    if !supports_cast_unwrap_in_comparison(*binary.op_kind()) {
        return None;
    }

    rewrite_cast_comparison(binary.lhs(), binary.rhs(), *binary.op_kind(), ctx)
        .map(|(new_lhs, new_rhs)| BinaryOp::new(*binary.op_kind(), new_lhs, new_rhs).into_scalar())
        .or_else(|| {
            rewrite_cast_comparison(binary.rhs(), binary.lhs(), *binary.op_kind(), ctx).map(
                |(new_rhs, new_lhs)| {
                    BinaryOp::new(*binary.op_kind(), new_lhs, new_rhs).into_scalar()
                },
            )
        })
}

fn simplify_cast_in_list(scalar: &Arc<Scalar>, ctx: Option<&IRContext>) -> Option<Arc<Scalar>> {
    let ctx = ctx?;
    let in_list = scalar.try_borrow::<InList>().ok()?;
    let cast = in_list.expr().try_borrow::<Cast>().ok()?;
    let list = in_list.list().try_borrow::<List>().ok()?;
    let expr_type = infer_scalar_data_type(ctx, cast.expr().as_ref())?;
    if !is_safe_unwrap_cast(&expr_type, cast.data_type()) {
        return None;
    }

    let rewritten_members = list
        .members()
        .iter()
        .map(|member| {
            let literal = member.try_borrow::<Literal>().ok()?;
            cast_literal_losslessly(literal.value(), &expr_type).map(Literal::new)
        })
        .collect::<Option<Vec<_>>>()?;

    Some(
        InList::new(
            cast.expr().clone(),
            List::new(Arc::from(
                rewritten_members
                    .into_iter()
                    .map(IntoScalar::into_scalar)
                    .collect::<Vec<_>>(),
            ))
            .into_scalar(),
            *in_list.negated(),
        )
        .into_scalar(),
    )
}

fn supports_cast_unwrap_in_comparison(op_kind: BinaryOpKind) -> bool {
    matches!(
        op_kind,
        BinaryOpKind::Eq
            | BinaryOpKind::Ne
            | BinaryOpKind::Lt
            | BinaryOpKind::Le
            | BinaryOpKind::Gt
            | BinaryOpKind::Ge
    )
}

fn rewrite_cast_comparison(
    expr_side: &Arc<Scalar>,
    literal_side: &Arc<Scalar>,
    op_kind: BinaryOpKind,
    ctx: &IRContext,
) -> Option<(Arc<Scalar>, Arc<Scalar>)> {
    let cast = expr_side.try_borrow::<Cast>().ok()?;
    let literal = literal_side.try_borrow::<Literal>().ok()?;
    let expr_type = infer_scalar_data_type(ctx, cast.expr().as_ref())?;
    if !is_safe_unwrap_cast(&expr_type, cast.data_type()) {
        return None;
    }

    let casted_literal = cast_literal_losslessly(literal.value(), &expr_type)?;
    let new_expr_side = cast.expr().clone();
    let new_literal_side = Literal::new(casted_literal).into_scalar();
    Some(match op_kind {
        BinaryOpKind::Eq
        | BinaryOpKind::Ne
        | BinaryOpKind::Lt
        | BinaryOpKind::Le
        | BinaryOpKind::Gt
        | BinaryOpKind::Ge => (new_expr_side, new_literal_side),
        _ => unreachable!("checked by supports_cast_unwrap_in_comparison"),
    })
}

fn infer_scalar_data_type(ctx: &IRContext, scalar: &Scalar) -> Option<DataType> {
    match &scalar.kind {
        ScalarKind::ColumnRef(meta) => {
            let column = ColumnRef::borrow_raw_parts(meta, &scalar.common);
            Some(ctx.get_column_meta(column.column()).data_type)
        }
        ScalarKind::Literal(meta) => {
            let literal = Literal::borrow_raw_parts(meta, &scalar.common);
            Some(literal.value().data_type())
        }
        ScalarKind::BinaryOp(meta) => {
            let binary = BinaryOp::borrow_raw_parts(meta, &scalar.common);
            let lhs_type = infer_scalar_data_type(ctx, binary.lhs().as_ref())?;
            Some(match binary.op_kind() {
                BinaryOpKind::Eq
                | BinaryOpKind::Ne
                | BinaryOpKind::IsDistinctFrom
                | BinaryOpKind::IsNotDistinctFrom
                | BinaryOpKind::Lt
                | BinaryOpKind::Le
                | BinaryOpKind::Gt
                | BinaryOpKind::Ge
                | BinaryOpKind::RegexMatch
                | BinaryOpKind::RegexIMatch
                | BinaryOpKind::RegexNotMatch
                | BinaryOpKind::RegexNotIMatch
                | BinaryOpKind::LikeMatch
                | BinaryOpKind::ILikeMatch
                | BinaryOpKind::NotLikeMatch
                | BinaryOpKind::NotILikeMatch
                | BinaryOpKind::AtArrow
                | BinaryOpKind::ArrowAt
                | BinaryOpKind::AtAt
                | BinaryOpKind::AtQuestion
                | BinaryOpKind::Question
                | BinaryOpKind::QuestionAnd
                | BinaryOpKind::QuestionPipe => DataType::Boolean,
                _ => lhs_type,
            })
        }
        ScalarKind::NaryOp(_) => Some(DataType::Boolean),
        ScalarKind::Function(meta) => {
            let function = Function::borrow_raw_parts(meta, &scalar.common);
            Some(function.return_type().clone())
        }
        ScalarKind::Cast(meta) => {
            let cast = Cast::borrow_raw_parts(meta, &scalar.common);
            Some(cast.data_type().clone())
        }
        ScalarKind::InList(_)
        | ScalarKind::IsNull(_)
        | ScalarKind::IsNotNull(_)
        | ScalarKind::Like(_) => Some(DataType::Boolean),
        ScalarKind::Case(meta) => {
            let case = Case::borrow_raw_parts(meta, &scalar.common);
            case.when_then_expr()
                .next()
                .map(|(_, then)| infer_scalar_data_type(ctx, then.as_ref()))
                .or_else(|| {
                    case.else_expr()
                        .map(|expr| infer_scalar_data_type(ctx, expr.as_ref()))
                })
                .flatten()
        }
        ScalarKind::List(_) => None,
    }
}

fn is_safe_unwrap_cast(expr_type: &DataType, cast_type: &DataType) -> bool {
    expr_type == cast_type
        || signed_integer_rank(expr_type)
            .zip(signed_integer_rank(cast_type))
            .is_some_and(|(expr_rank, cast_rank)| expr_rank < cast_rank)
        || unsigned_integer_rank(expr_type)
            .zip(unsigned_integer_rank(cast_type))
            .is_some_and(|(expr_rank, cast_rank)| expr_rank < cast_rank)
        || decimal_widening(expr_type, cast_type)
}

fn signed_integer_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        _ => None,
    }
}

fn unsigned_integer_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::UInt8 => Some(0),
        DataType::UInt16 => Some(1),
        DataType::UInt32 => Some(2),
        DataType::UInt64 => Some(3),
        _ => None,
    }
}

fn cast_literal_losslessly(literal: &ScalarValue, target_type: &DataType) -> Option<ScalarValue> {
    if literal.is_null() {
        return None;
    }

    let casted = literal.cast_to(target_type).ok()?;
    let round_tripped = casted.cast_to(&literal.data_type()).ok()?;
    (round_tripped == *literal).then_some(casted)
}

fn decimal_widening(expr_type: &DataType, cast_type: &DataType) -> bool {
    match (expr_type, cast_type) {
        (
            DataType::Decimal128(expr_precision, expr_scale),
            DataType::Decimal128(cast_precision, cast_scale),
        ) => cast_precision >= expr_precision && cast_scale >= expr_scale,
        (
            DataType::Decimal256(expr_precision, expr_scale),
            DataType::Decimal256(cast_precision, cast_scale),
        ) => cast_precision >= expr_precision && cast_scale >= expr_scale,
        _ => false,
    }
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
