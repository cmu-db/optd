//! Central expression-statistics transformations.
//!
//! Logical value identity remains owned by `LogicalFacts`. This module only describes how a
//! recognized expression transforms a column profile. Unsupported or unsafe transformations return
//! `None`, allowing the caller to retain the stable unknown-expression fallback.

use arrow_schema::DataType;

use crate::{BinaryOp, Column, Expr, ExprData, QueryContext, ScalarFunction, ScalarValue, UnaryOp};

use super::{CardinalityProfile, ColumnProfile, Estimate, ValueId};

/// Whether an expression preserves distinctness of its non-null input values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Injectivity {
    /// The expression is the same logical value, such as a direct alias.
    Identity,
    /// Distinct non-null inputs remain distinct outputs.
    Injective,
    /// Multiple inputs may map to one output, with a proven output-domain cap.
    ManyToOne,
}

/// Ordering relationship between an expression's input and output values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Monotonicity {
    Increasing,
    Decreasing,
    NonDecreasing,
}

/// Null behavior of a recognized expression transformation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NullBehavior {
    Preserves,
}

/// Semantic properties proven for one expression transformation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TransformProperties {
    pub injectivity: Injectivity,
    pub monotonicity: Monotonicity,
    pub null_behavior: NullBehavior,
}

/// Derived profile paired with the properties that justified it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct TransformedProfile {
    pub profile: ColumnProfile,
    pub properties: TransformProperties,
}

/// Derives a profile for one map output using only sound built-in transformation rules.
pub(super) fn derive_expression_profile(
    input: &CardinalityProfile,
    expr: Expr,
    output: Column,
    ctx: &QueryContext,
) -> Option<TransformedProfile> {
    let mut transformed = derive_expression(input, expr, ctx)?;
    if transformed.properties.injectivity != Injectivity::Identity {
        if transformed.properties.injectivity == Injectivity::ManyToOne {
            coerce_bucket_bounds(&mut transformed.profile, &ctx.column(output).ty)?;
        }
        transformed.profile.value = Some(ValueId::Derived(output));
        transformed.profile.sketches = None;
    }
    Some(transformed)
}

fn derive_expression(
    input: &CardinalityProfile,
    expr: Expr,
    ctx: &QueryContext,
) -> Option<TransformedProfile> {
    match expr.get(ctx) {
        ExprData::ColumnRef(column) => Some(TransformedProfile {
            profile: input.columns.get(column)?.clone(),
            properties: TransformProperties {
                injectivity: Injectivity::Identity,
                monotonicity: Monotonicity::Increasing,
                null_behavior: NullBehavior::Preserves,
            },
        }),
        ExprData::Cast { expr, ty } => derive_cast(input, *expr, ty, ctx),
        ExprData::Unary {
            op: UnaryOp::Negate,
            expr,
        } => derive_integer_affine(input, *expr, -1, 0, ctx),
        ExprData::Binary { op, left, right } => {
            derive_binary_affine(input, *op, *left, *right, ctx)
        }
        ExprData::ScalarFunction { function, args } => {
            derive_scalar_function(input, function, args, ctx)
        }
        _ => None,
    }
}

fn derive_cast(
    input: &CardinalityProfile,
    expr: Expr,
    target: &DataType,
    ctx: &QueryContext,
) -> Option<TransformedProfile> {
    let source_type = expression_type(expr, ctx)?;
    if !is_lossless_cast(&source_type, target) {
        return None;
    }
    let mut transformed = derive_expression(input, expr, ctx)?;
    transformed.profile.lower_bound = match transformed.profile.lower_bound.as_ref() {
        Some(value) => Some(cast_scalar(value, target)?),
        None => None,
    };
    transformed.profile.upper_bound = match transformed.profile.upper_bound.as_ref() {
        Some(value) => Some(cast_scalar(value, target)?),
        None => None,
    };
    transformed.properties = TransformProperties {
        // LogicalFacts intentionally assigns casts a derived identity. Even a no-op cast therefore
        // remains a proven injective transform rather than a direct alias here.
        injectivity: Injectivity::Injective,
        monotonicity: Monotonicity::Increasing,
        null_behavior: NullBehavior::Preserves,
    };
    transformed.profile.distinct = preserve_distinct(&transformed.profile);
    Some(transformed)
}

fn derive_binary_affine(
    input: &CardinalityProfile,
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
) -> Option<TransformedProfile> {
    let left_literal = literal_i128(left, ctx);
    let right_literal = literal_i128(right, ctx);
    match (op, left_literal, right_literal) {
        (BinaryOp::Add, Some(constant), None) => {
            derive_integer_affine(input, right, 1, constant, ctx)
        }
        (BinaryOp::Add, None, Some(constant)) => {
            derive_integer_affine(input, left, 1, constant, ctx)
        }
        (BinaryOp::Subtract, None, Some(constant)) => {
            derive_integer_affine(input, left, 1, -constant, ctx)
        }
        (BinaryOp::Subtract, Some(constant), None) => {
            derive_integer_affine(input, right, -1, constant, ctx)
        }
        (BinaryOp::Multiply, Some(constant), None) if constant != 0 => {
            derive_integer_affine(input, right, constant, 0, ctx)
        }
        (BinaryOp::Multiply, None, Some(constant)) if constant != 0 => {
            derive_integer_affine(input, left, constant, 0, ctx)
        }
        _ => None,
    }
}

fn derive_integer_affine(
    input: &CardinalityProfile,
    expr: Expr,
    multiplier: i128,
    offset: i128,
    ctx: &QueryContext,
) -> Option<TransformedProfile> {
    let mut transformed = derive_expression(input, expr, ctx)?;
    let lower = transformed.profile.lower_bound.as_ref()?;
    let upper = transformed.profile.upper_bound.as_ref()?;
    let mapped_lower = affine_integer_scalar(lower, multiplier, offset)?;
    let mapped_upper = affine_integer_scalar(upper, multiplier, offset)?;
    let increasing = multiplier > 0;
    let (output_lower, output_upper) = if increasing {
        (mapped_lower, mapped_upper)
    } else {
        (mapped_upper, mapped_lower)
    };
    transformed.profile.lower_bound = Some(output_lower);
    transformed.profile.upper_bound = Some(output_upper);
    transformed.profile.distinct = preserve_distinct(&transformed.profile);
    transformed.properties = TransformProperties {
        injectivity: Injectivity::Injective,
        monotonicity: if increasing {
            Monotonicity::Increasing
        } else {
            Monotonicity::Decreasing
        },
        null_behavior: NullBehavior::Preserves,
    };
    Some(transformed)
}

fn derive_scalar_function(
    input: &CardinalityProfile,
    function: &ScalarFunction,
    args: &[Expr],
    ctx: &QueryContext,
) -> Option<TransformedProfile> {
    let ScalarFunction::Extension(name) = function else {
        return None;
    };
    if !matches!(
        name.to_ascii_lowercase().as_str(),
        "date_part" | "datepart" | "extract"
    ) || args.len() != 2
    {
        return None;
    }
    let (part, value_expr) = match (args[0].get(ctx), args[1].get(ctx)) {
        (ExprData::Literal(ScalarValue::Utf8(part)), _) => (part, args[1]),
        (_, ExprData::Literal(ScalarValue::Utf8(part))) => (part, args[0]),
        _ => return None,
    };
    if !part.eq_ignore_ascii_case("year") {
        return None;
    }
    derive_date_year(input, value_expr, ctx)
}

fn derive_date_year(
    input: &CardinalityProfile,
    expr: Expr,
    ctx: &QueryContext,
) -> Option<TransformedProfile> {
    let mut transformed = derive_expression(input, expr, ctx)?;
    let ScalarValue::Date32(lower_days) = transformed.profile.lower_bound.as_ref()? else {
        return None;
    };
    let ScalarValue::Date32(upper_days) = transformed.profile.upper_bound.as_ref()? else {
        return None;
    };
    let lower_year = year_from_days_since_unix_epoch(*lower_days);
    let upper_year = year_from_days_since_unix_epoch(*upper_days);
    let bucket_count = i64::from(upper_year) - i64::from(lower_year) + 1;
    if bucket_count <= 0 {
        return None;
    }
    let bucket_count = bucket_count as f64;
    transformed.profile.lower_bound = Some(ScalarValue::Int64(i64::from(lower_year)));
    transformed.profile.upper_bound = Some(ScalarValue::Int64(i64::from(upper_year)));
    transformed.profile.distinct = cap_distinct(&transformed.profile, bucket_count);
    transformed.properties = TransformProperties {
        injectivity: Injectivity::ManyToOne,
        monotonicity: Monotonicity::NonDecreasing,
        null_behavior: NullBehavior::Preserves,
    };
    Some(transformed)
}

fn preserve_distinct(profile: &ColumnProfile) -> Estimate {
    let upper = min_optional(profile.distinct.upper, profile.frequency.upper);
    let lower = if profile.frequency.value <= 0.0 {
        Some(0.0)
    } else {
        Some(profile.distinct.lower.unwrap_or(1.0).max(1.0))
    };
    Estimate::derived(profile.distinct.value, lower, upper)
}

fn cap_distinct(profile: &ColumnProfile, cap: f64) -> Estimate {
    let upper = min_optional(
        profile.distinct.upper.map(|value| value.min(cap)),
        profile.frequency.upper.map(|value| value.min(cap)),
    )
    .or(Some(cap));
    let lower = if profile.frequency.value <= 0.0 {
        Some(0.0)
    } else {
        Some(1.0)
    };
    Estimate::derived(profile.distinct.value.min(cap), lower, upper)
}

fn coerce_bucket_bounds(profile: &mut ColumnProfile, target: &DataType) -> Option<()> {
    profile.lower_bound = match profile.lower_bound.as_ref()? {
        ScalarValue::Int64(value) => Some(match target {
            DataType::Int32 => ScalarValue::Int32(i32::try_from(*value).ok()?),
            DataType::Int64 => ScalarValue::Int64(*value),
            DataType::Float64 => ScalarValue::Float64(*value as f64),
            _ => return None,
        }),
        _ => return None,
    };
    profile.upper_bound = match profile.upper_bound.as_ref()? {
        ScalarValue::Int64(value) => Some(match target {
            DataType::Int32 => ScalarValue::Int32(i32::try_from(*value).ok()?),
            DataType::Int64 => ScalarValue::Int64(*value),
            DataType::Float64 => ScalarValue::Float64(*value as f64),
            _ => return None,
        }),
        _ => return None,
    };
    Some(())
}

fn min_optional(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn expression_type(expr: Expr, ctx: &QueryContext) -> Option<DataType> {
    match expr.get(ctx) {
        ExprData::Literal(value) => Some(value.data_type()),
        ExprData::ColumnRef(column) => Some(ctx.column(*column).ty.clone()),
        ExprData::Cast { ty, .. } => Some(ty.clone()),
        ExprData::Unary {
            op: UnaryOp::Negate,
            expr,
        } => expression_type(*expr, ctx),
        ExprData::Binary {
            op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide,
            left,
            ..
        } => expression_type(*left, ctx),
        _ => None,
    }
}

fn is_lossless_cast(source: &DataType, target: &DataType) -> bool {
    source == target
        || matches!((source, target), (DataType::Int32, DataType::Int64))
        || matches!(
            (source, target),
            (
                DataType::Decimal128(source_precision, source_scale),
                DataType::Decimal128(target_precision, target_scale)
            ) if source_scale == target_scale && target_precision >= source_precision
        )
}

fn cast_scalar(value: &ScalarValue, target: &DataType) -> Option<ScalarValue> {
    if &value.data_type() == target {
        return Some(value.clone());
    }
    match (value, target) {
        (ScalarValue::Null(_), target) => Some(ScalarValue::Null(target.clone())),
        (ScalarValue::Int32(value), DataType::Int64) => Some(ScalarValue::Int64(i64::from(*value))),
        (
            ScalarValue::Decimal128 { value, scale, .. },
            DataType::Decimal128(precision, target_scale),
        ) if scale == target_scale => Some(ScalarValue::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *target_scale,
        }),
        _ => None,
    }
}

fn literal_i128(expr: Expr, ctx: &QueryContext) -> Option<i128> {
    match expr.get(ctx) {
        ExprData::Literal(ScalarValue::Int32(value)) => Some(i128::from(*value)),
        ExprData::Literal(ScalarValue::Int64(value)) => Some(i128::from(*value)),
        _ => None,
    }
}

fn affine_integer_scalar(
    value: &ScalarValue,
    multiplier: i128,
    offset: i128,
) -> Option<ScalarValue> {
    let transformed = match value {
        ScalarValue::Int32(value) => i128::from(*value)
            .checked_mul(multiplier)?
            .checked_add(offset)?,
        ScalarValue::Int64(value) => i128::from(*value)
            .checked_mul(multiplier)?
            .checked_add(offset)?,
        _ => return None,
    };
    match value {
        ScalarValue::Int32(_) => i32::try_from(transformed).ok().map(ScalarValue::Int32),
        ScalarValue::Int64(_) => i64::try_from(transformed).ok().map(ScalarValue::Int64),
        _ => None,
    }
}

// Howard Hinnant's civil-from-days algorithm, with input measured from 1970-01-01.
fn year_from_days_since_unix_epoch(days: i32) -> i32 {
    let z = i64::from(days) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    i32::try_from(year_of_era + era * 400 + i64::from(month <= 2)).unwrap_or(if z < 0 {
        i32::MIN
    } else {
        i32::MAX
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{ColumnData, ExprData};

    use super::*;

    fn profile_for(
        column: Column,
        lower_bound: ScalarValue,
        upper_bound: ScalarValue,
        rows: f64,
        distinct: f64,
    ) -> CardinalityProfile {
        CardinalityProfile::new(
            Estimate::exact(rows),
            [(
                column,
                ColumnProfile {
                    lower_bound: Some(lower_bound),
                    upper_bound: Some(upper_bound),
                    frequency: Estimate::exact(rows),
                    distinct: Estimate::exact(distinct),
                    value: None,
                    sketches: None,
                },
            )]
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        )
    }

    #[test]
    fn civil_year_conversion_handles_epoch_and_leap_boundaries() {
        assert_eq!(year_from_days_since_unix_epoch(0), 1970);
        assert_eq!(year_from_days_since_unix_epoch(365), 1971);
        assert_eq!(year_from_days_since_unix_epoch(-1), 1969);
        assert_eq!(year_from_days_since_unix_epoch(11_016), 2000);
    }

    #[test]
    fn lossless_integer_cast_preserves_ndv_and_converts_bounds() {
        let mut ctx = QueryContext::new();
        let input_column = ColumnData::new("small", DataType::Int32).add(&mut ctx);
        let output_column = ColumnData::new("wide", DataType::Int64).add(&mut ctx);
        let input_ref = ExprData::ColumnRef(input_column).add(&mut ctx);
        let cast = ExprData::Cast {
            expr: input_ref,
            ty: DataType::Int64,
        }
        .add(&mut ctx);
        let input = profile_for(
            input_column,
            ScalarValue::Int32(-10),
            ScalarValue::Int32(20),
            100.0,
            31.0,
        );

        let transformed =
            derive_expression_profile(&input, cast, output_column, &ctx).expect("lossless cast");

        assert_eq!(transformed.properties.injectivity, Injectivity::Injective);
        assert_eq!(
            transformed.profile.lower_bound,
            Some(ScalarValue::Int64(-10))
        );
        assert_eq!(
            transformed.profile.upper_bound,
            Some(ScalarValue::Int64(20))
        );
        assert_eq!(
            transformed.profile.distinct,
            Estimate::derived(31.0, Some(31.0), Some(31.0))
        );
        assert_eq!(
            transformed.profile.value,
            Some(ValueId::Derived(output_column))
        );
    }

    #[test]
    fn narrowing_integer_cast_is_not_treated_as_injective() {
        let mut ctx = QueryContext::new();
        let input_column = ColumnData::new("wide", DataType::Int64).add(&mut ctx);
        let output_column = ColumnData::new("small", DataType::Int32).add(&mut ctx);
        let input_ref = ExprData::ColumnRef(input_column).add(&mut ctx);
        let cast = ExprData::Cast {
            expr: input_ref,
            ty: DataType::Int32,
        }
        .add(&mut ctx);
        let input = profile_for(
            input_column,
            ScalarValue::Int64(-10),
            ScalarValue::Int64(20),
            100.0,
            31.0,
        );

        assert!(derive_expression_profile(&input, cast, output_column, &ctx).is_none());
    }

    #[test]
    fn date_year_caps_ndv_by_proven_output_domain() {
        let mut ctx = QueryContext::new();
        let input_column = ColumnData::new("order_date", DataType::Date32).add(&mut ctx);
        let output_column = ColumnData::new("order_year", DataType::Float64).add(&mut ctx);
        let part = ExprData::Literal(ScalarValue::Utf8("year".to_string())).add(&mut ctx);
        let input_ref = ExprData::ColumnRef(input_column).add(&mut ctx);
        let extract = ExprData::ScalarFunction {
            function: ScalarFunction::extension("date_part"),
            args: vec![part, input_ref],
        }
        .add(&mut ctx);
        let input = profile_for(
            input_column,
            ScalarValue::Date32(8_035),
            ScalarValue::Date32(10_591),
            10_000.0,
            2_557.0,
        );

        let transformed = derive_expression_profile(&input, extract, output_column, &ctx)
            .expect("bounded year extraction");

        assert_eq!(transformed.properties.injectivity, Injectivity::ManyToOne);
        assert_eq!(
            transformed.properties.monotonicity,
            Monotonicity::NonDecreasing
        );
        assert_eq!(
            transformed.profile.lower_bound,
            Some(ScalarValue::Float64(1992.0))
        );
        assert_eq!(
            transformed.profile.upper_bound,
            Some(ScalarValue::Float64(1998.0))
        );
        assert_eq!(transformed.profile.distinct.value, 7.0);
        assert_eq!(transformed.profile.distinct.lower, Some(1.0));
        assert_eq!(transformed.profile.distinct.upper, Some(7.0));
    }
}
