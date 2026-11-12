//! Leaf sample selectivity helpers for [`super::CardinalityEstimationV1`].
//!
//! Catalog table samples estimate **selection predicate** selectivity (Umbra/Cedar style).
//! Column NDVs / sketches / equivalence classes stay on the classic V1 profile path so joins
//! keep catalog distincts. This module never joins samples.

use optd_sampling::{
    SampleCompareOp, SamplePredicate, SampleRelation, SampleScalar, match_fraction,
};

use crate::{
    BinaryOp, Expr, ExprData, NaryOp, Operator, OperatorData, QueryContext, ScalarValue, Scan,
};

use super::{AnalysisContext, AnalysisError, AnalysisResult};

/// Loads a sample relation for a scan-pipeline operator (no joins), for per-conjunct selectivity.
pub(super) fn try_leaf_sample_relation(
    operator: Operator,
    ctx: &QueryContext,
    analyses: &AnalysisContext,
) -> AnalysisResult<Option<SampleRelation>> {
    if !scan_pipeline_has_samples(operator, ctx, analyses)? {
        return Ok(None);
    }
    evaluate_sample_tree_no_filter(operator, ctx, analyses)
}

/// Selectivity of one conjunct against a leaf sample (`matches/n`, or `0` when empty/no match).
pub(super) fn sample_conjunct_selectivity(
    relation: &SampleRelation,
    conjunct: Expr,
    ctx: &QueryContext,
) -> Option<f64> {
    let predicate = translate_predicate(conjunct, ctx)?;
    match_fraction(relation, &predicate).ok()
}

fn scan_pipeline_has_samples(
    operator: Operator,
    ctx: &QueryContext,
    analyses: &AnalysisContext,
) -> AnalysisResult<bool> {
    match operator.get(ctx) {
        OperatorData::Scan(scan) => Ok(scan_samples(scan, analyses)?.is_some()),
        OperatorData::Projection(data) => scan_pipeline_has_samples(data.input, ctx, analyses),
        OperatorData::Output(data) => scan_pipeline_has_samples(data.input, ctx, analyses),
        OperatorData::Sort(data) => scan_pipeline_has_samples(data.input, ctx, analyses),
        OperatorData::Rename(data) => scan_pipeline_has_samples(data.input, ctx, analyses),
        OperatorData::Selection(data) => scan_pipeline_has_samples(data.input, ctx, analyses),
        _ => Ok(false),
    }
}

fn evaluate_sample_tree_no_filter(
    operator: Operator,
    ctx: &QueryContext,
    analyses: &AnalysisContext,
) -> AnalysisResult<Option<SampleRelation>> {
    match operator.get(ctx) {
        OperatorData::Scan(scan) => scan_relation(scan, analyses),
        OperatorData::Selection(data) => evaluate_sample_tree_no_filter(data.input, ctx, analyses),
        OperatorData::Projection(data) => evaluate_sample_tree_no_filter(data.input, ctx, analyses),
        OperatorData::Output(data) => evaluate_sample_tree_no_filter(data.input, ctx, analyses),
        OperatorData::Sort(data) => evaluate_sample_tree_no_filter(data.input, ctx, analyses),
        OperatorData::Rename(data) => evaluate_sample_tree_no_filter(data.input, ctx, analyses),
        _ => Ok(None),
    }
}

fn scan_relation(
    scan: &Scan,
    analyses: &AnalysisContext,
) -> AnalysisResult<Option<SampleRelation>> {
    let Some(sample) = scan_samples(scan, analyses)? else {
        return Ok(None);
    };
    match SampleRelation::from_table_sample(&sample, &[]) {
        Ok(relation) => Ok(Some(relation)),
        Err(_) => Ok(None),
    }
}

fn scan_samples(
    scan: &Scan,
    analyses: &AnalysisContext,
) -> AnalysisResult<Option<optd_sampling::TableSample>> {
    analyses
        .catalog()
        .table_samples(&scan.table)
        .map_err(|error| AnalysisError::Catalog(error.to_string()))
}

fn translate_predicate(expr: Expr, ctx: &QueryContext) -> Option<SamplePredicate> {
    match expr.get(ctx) {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => {
            let mut parts = Vec::with_capacity(exprs.len());
            for arg in exprs {
                parts.push(translate_predicate(*arg, ctx)?);
            }
            Some(SamplePredicate::And(parts))
        }
        ExprData::Nary {
            op: NaryOp::Or,
            exprs,
        } => {
            let mut parts = Vec::with_capacity(exprs.len());
            for arg in exprs {
                parts.push(translate_predicate(*arg, ctx)?);
            }
            Some(SamplePredicate::Or(parts))
        }
        ExprData::Binary { op, left, right } => translate_binary(*op, *left, *right, ctx),
        ExprData::Like {
            negated,
            expr,
            pattern,
            case_insensitive,
        } => {
            let ExprData::ColumnRef(column) = expr.get(ctx) else {
                return None;
            };
            let SampleScalar::Utf8(pattern) = fold_scalar_expr(*pattern, ctx)? else {
                return None;
            };
            Some(SamplePredicate::Like {
                column: ctx.column(*column).name.clone(),
                pattern,
                negated: *negated,
                case_insensitive: *case_insensitive,
            })
        }
        _ => None,
    }
}

fn translate_binary(
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
) -> Option<SamplePredicate> {
    let compare = match op {
        BinaryOp::Eq => SampleCompareOp::Eq,
        BinaryOp::NotEq => SampleCompareOp::NotEq,
        BinaryOp::Lt => SampleCompareOp::Lt,
        BinaryOp::LtEq => SampleCompareOp::LtEq,
        BinaryOp::Gt => SampleCompareOp::Gt,
        BinaryOp::GtEq => SampleCompareOp::GtEq,
        _ => return None,
    };

    match (left.get(ctx), right.get(ctx)) {
        (ExprData::ColumnRef(column), ExprData::ColumnRef(other)) => {
            Some(SamplePredicate::CompareColumns {
                left: ctx.column(*column).name.clone(),
                op: compare,
                right: ctx.column(*other).name.clone(),
            })
        }
        (ExprData::ColumnRef(column), _) => {
            let value = fold_scalar_expr(right, ctx)?;
            Some(SamplePredicate::CompareColumnLiteral {
                column: ctx.column(*column).name.clone(),
                op: compare,
                value,
            })
        }
        (_, ExprData::ColumnRef(column)) => {
            let value = fold_scalar_expr(left, ctx)?;
            let reversed = match compare {
                SampleCompareOp::Lt => SampleCompareOp::Gt,
                SampleCompareOp::LtEq => SampleCompareOp::GtEq,
                SampleCompareOp::Gt => SampleCompareOp::Lt,
                SampleCompareOp::GtEq => SampleCompareOp::LtEq,
                other => other,
            };
            Some(SamplePredicate::CompareColumnLiteral {
                column: ctx.column(*column).name.clone(),
                op: reversed,
                value,
            })
        }
        _ => None,
    }
}

fn fold_scalar_expr(expr: Expr, ctx: &QueryContext) -> Option<SampleScalar> {
    match expr.get(ctx) {
        ExprData::Literal(value) => translate_scalar(value),
        ExprData::Binary {
            op: BinaryOp::Add,
            left,
            right,
        } => fold_date_plus_interval(*left, *right, ctx, false),
        ExprData::Binary {
            op: BinaryOp::Subtract,
            left,
            right,
        } => fold_date_plus_interval(*left, *right, ctx, true),
        _ => None,
    }
}

fn fold_date_plus_interval(
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
    subtract: bool,
) -> Option<SampleScalar> {
    match (left.get(ctx), right.get(ctx)) {
        (ExprData::Literal(ScalarValue::Date32(days)), ExprData::Literal(interval)) => {
            let (months, day_part) = interval_parts(interval)?;
            Some(SampleScalar::Date32(date32_add_interval(
                *days, months, day_part, subtract,
            )?))
        }
        (ExprData::Literal(interval), ExprData::Literal(ScalarValue::Date32(days)))
            if !subtract =>
        {
            let (months, day_part) = interval_parts(interval)?;
            Some(SampleScalar::Date32(date32_add_interval(
                *days, months, day_part, false,
            )?))
        }
        _ => {
            // Nested date arithmetic: fold children first when the date side is itself an expr.
            let left_date = fold_scalar_expr(left, ctx);
            let right_lit = right.get(ctx);
            match (left_date, right_lit) {
                (Some(SampleScalar::Date32(days)), ExprData::Literal(interval)) => {
                    let (months, day_part) = interval_parts(interval)?;
                    Some(SampleScalar::Date32(date32_add_interval(
                        days, months, day_part, subtract,
                    )?))
                }
                _ => None,
            }
        }
    }
}

fn interval_parts(value: &ScalarValue) -> Option<(i32, i32)> {
    match value {
        ScalarValue::IntervalMonthDayNano {
            months,
            days,
            nanoseconds,
        } if *nanoseconds == 0 => Some((*months, *days)),
        ScalarValue::IntervalDayTime { days, milliseconds } if *milliseconds == 0 => {
            Some((0, *days))
        }
        _ => None,
    }
}

fn translate_scalar(value: &ScalarValue) -> Option<SampleScalar> {
    match value {
        ScalarValue::Null(_) => Some(SampleScalar::Null),
        ScalarValue::Boolean(value) => Some(SampleScalar::Boolean(*value)),
        ScalarValue::Int32(value) => Some(SampleScalar::Int32(*value)),
        ScalarValue::Int64(value) => Some(SampleScalar::Int64(*value)),
        ScalarValue::Float64(value) => Some(SampleScalar::Float64(*value)),
        ScalarValue::Utf8(value) => Some(SampleScalar::Utf8(value.clone())),
        ScalarValue::Date32(value) => Some(SampleScalar::Date32(*value)),
        ScalarValue::Decimal128 { .. }
        | ScalarValue::IntervalMonthDayNano { .. }
        | ScalarValue::IntervalDayTime { .. } => None,
    }
}

/// Adds a month-day interval to a Date32 (days since UNIX epoch) using civil calendar math.
fn date32_add_interval(date_days: i32, months: i32, days: i32, subtract: bool) -> Option<i32> {
    let sign = if subtract { -1 } else { 1 };
    let (mut y, mut m, d) = civil_from_days(date_days)?;
    let total_months = i64::from(y) * 12 + i64::from(m) - 1 + i64::from(sign * months);
    y = total_months.div_euclid(12) as i32;
    m = (total_months.rem_euclid(12) as u32) + 1;
    let max_d = days_in_month(y, m)?;
    let d = d.min(max_d);
    let base = days_from_civil(y, m, d)?;
    Some(base + sign * days)
}

fn civil_from_days(days: i32) -> Option<(i32, u32, u32)> {
    let z = i64::from(days) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    Some((y as i32, m as u32, d))
}

fn days_from_civil(y: i32, m: u32, d: u32) -> Option<i32> {
    if !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m)? {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = i64::from(y).div_euclid(400);
    let yoe = (i64::from(y) - era * 400) as u64;
    let mp = u64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + u64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some((era * 146_097 + doe as i64 - 719_468) as i32)
}

fn days_in_month(y: i32, m: u32) -> Option<u32> {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 => Some(if is_leap(y) { 29 } else { 28 }),
        _ => None,
    }
}

fn is_leap(y: i32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[cfg(test)]
mod tests {
    use super::date32_add_interval;

    #[test]
    fn date32_adds_three_months_across_month_lengths() {
        // 1993-07-01 is Date32 days since epoch.
        let jul1 = days_since_epoch(1993, 7, 1);
        let oct1 = date32_add_interval(jul1, 3, 0, false).unwrap();
        assert_eq!(oct1, days_since_epoch(1993, 10, 1));
    }

    fn days_since_epoch(y: i32, m: u32, d: u32) -> i32 {
        super::days_from_civil(y, m, d).unwrap()
    }
}
