//! Sample-based cardinality estimation helpers used by [`super::CardinalityEstimationV1`].
//!
//! When every leaf scan under an operator has catalog samples and the operator tree is in the
//! supported fragment (scan / selection / projection / rename / output / sort / inner equijoin),
//! estimates are produced by evaluating Arrow batches and scaling with Umbra-style factors.

use std::collections::BTreeMap;

use optd_sampling::{
    SamplePredicate, SampleRelation, SampleScalar, equijoin_relations, filter_relation,
};

use crate::{
    BinaryOp, Column, Expr, ExprData, JoinType, NaryOp, Operator, OperatorData, QueryContext,
    ScalarValue, Scan,
};

use super::{
    AnalysisContext, AnalysisError, AnalysisResult, AvailableColumns, CardinalityProfile,
    ColumnProfile, Estimate,
};

/// Attempts sample-based estimation for `operator`.
///
/// Returns `Ok(None)` when the operator tree is outside the supported sample fragment or any leaf
/// scan lacks catalog samples, so the caller can fall back to classic estimation.
pub(super) fn try_sample_cardinality(
    operator: Operator,
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
) -> AnalysisResult<Option<CardinalityProfile>> {
    if !sample_eligible(operator, ctx, analyses)? {
        return Ok(None);
    }
    let Some(relation) = evaluate_sample_tree(operator, ctx, analyses)? else {
        return Ok(None);
    };
    let columns = analyses.get::<AvailableColumns>(ctx, operator)?;
    Ok(Some(profile_from_relation(
        relation.estimated_rows(),
        columns,
        analyses
            .cardinality_estimation_config()
            .unknown_column_ndv_cap,
    )))
}

fn profile_from_relation(
    rows: f64,
    columns: impl IntoIterator<Item = Column>,
    ndv_cap: f64,
) -> CardinalityProfile {
    let rows = Estimate::sample(rows);
    let columns = columns
        .into_iter()
        .map(|column| (column, ColumnProfile::unknown_with_ndv_cap(&rows, ndv_cap)))
        .collect::<BTreeMap<_, _>>();
    CardinalityProfile::new(rows, columns)
}

fn sample_eligible(
    operator: Operator,
    ctx: &QueryContext,
    analyses: &AnalysisContext,
) -> AnalysisResult<bool> {
    match operator.get(ctx) {
        OperatorData::Scan(scan) => Ok(scan_samples(scan, analyses)?.is_some()),
        OperatorData::ConstScan(_) | OperatorData::TableFunction(_) => Ok(false),
        OperatorData::Selection(data) => Ok(translate_predicate(data.predicate, ctx).is_some()
            && sample_eligible(data.input, ctx, analyses)?),
        OperatorData::Projection(data) => sample_eligible(data.input, ctx, analyses),
        OperatorData::Output(data) => sample_eligible(data.input, ctx, analyses),
        OperatorData::Sort(data) => sample_eligible(data.input, ctx, analyses),
        OperatorData::Rename(data) => sample_eligible(data.input, ctx, analyses),
        OperatorData::Join(data) => {
            if !matches!(data.join_type, JoinType::Inner) {
                return Ok(false);
            }
            Ok(translate_equijoin_keys(data.on, ctx).is_some()
                && sample_eligible(data.outer, ctx, analyses)?
                && sample_eligible(data.inner, ctx, analyses)?)
        }
        OperatorData::Map(_)
        | OperatorData::Aggregation(_)
        | OperatorData::Limit(_)
        | OperatorData::CrossProduct(_) => Ok(false),
    }
}

fn evaluate_sample_tree(
    operator: Operator,
    ctx: &QueryContext,
    analyses: &AnalysisContext,
) -> AnalysisResult<Option<SampleRelation>> {
    match operator.get(ctx) {
        OperatorData::Scan(scan) => {
            let Some(sample) = scan_samples(scan, analyses)? else {
                return Ok(None);
            };
            let columns = scan
                .columns
                .iter()
                .map(|column| ctx.column(*column).name.clone())
                .collect::<Vec<_>>();
            match SampleRelation::from_table_sample(&sample, &columns) {
                Ok(relation) => Ok(Some(relation)),
                Err(error) => Err(AnalysisError::Catalog(error.to_string())),
            }
        }
        OperatorData::Selection(data) => {
            let Some(input) = evaluate_sample_tree(data.input, ctx, analyses)? else {
                return Ok(None);
            };
            let Some(predicate) = translate_predicate(data.predicate, ctx) else {
                return Ok(None);
            };
            match filter_relation(&input, &predicate) {
                Ok(relation) => Ok(Some(relation)),
                Err(error) => Err(AnalysisError::Catalog(error.to_string())),
            }
        }
        OperatorData::Projection(data) => {
            let Some(input) = evaluate_sample_tree(data.input, ctx, analyses)? else {
                return Ok(None);
            };
            let columns = data
                .columns
                .iter()
                .map(|column| ctx.column(*column).name.clone())
                .collect::<Vec<_>>();
            match optd_sampling::project_batches(&input.batches, &columns) {
                Ok(batches) => {
                    let schema = batches
                        .first()
                        .map(|batch| batch.schema())
                        .unwrap_or(input.schema);
                    Ok(Some(SampleRelation {
                        schema,
                        batches,
                        scale: input.scale,
                    }))
                }
                // Renamed IR columns may not match sample field names; fall back to classic CE.
                Err(_) => Ok(None),
            }
        }
        OperatorData::Output(data) => evaluate_sample_tree(data.input, ctx, analyses),
        OperatorData::Sort(data) => evaluate_sample_tree(data.input, ctx, analyses),
        OperatorData::Rename(data) => {
            // Samples keep physical field names; rename only changes IR column handles.
            evaluate_sample_tree(data.input, ctx, analyses)
        }
        OperatorData::Join(data) => {
            let Some(left) = evaluate_sample_tree(data.outer, ctx, analyses)? else {
                return Ok(None);
            };
            let Some(right) = evaluate_sample_tree(data.inner, ctx, analyses)? else {
                return Ok(None);
            };
            let Some((left_keys, right_keys)) = translate_equijoin_keys(data.on, ctx) else {
                return Ok(None);
            };
            match equijoin_relations(&left, &right, &left_keys, &right_keys) {
                Ok(relation) => Ok(Some(relation)),
                Err(error) => Err(AnalysisError::Catalog(error.to_string())),
            }
        }
        _ => Ok(None),
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
        ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => translate_eq(*left, *right, ctx),
        _ => None,
    }
}

fn translate_eq(left: Expr, right: Expr, ctx: &QueryContext) -> Option<SamplePredicate> {
    match (left.get(ctx), right.get(ctx)) {
        (ExprData::ColumnRef(column), ExprData::Literal(value))
        | (ExprData::Literal(value), ExprData::ColumnRef(column)) => {
            Some(SamplePredicate::EqColumnLiteral {
                column: ctx.column(*column).name.clone(),
                value: translate_scalar(value)?,
            })
        }
        _ => None,
    }
}

fn translate_equijoin_keys(expr: Expr, ctx: &QueryContext) -> Option<(Vec<String>, Vec<String>)> {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    collect_equijoin_keys(expr, ctx, &mut left_keys, &mut right_keys)?;
    if left_keys.is_empty() {
        None
    } else {
        Some((left_keys, right_keys))
    }
}

fn collect_equijoin_keys(
    expr: Expr,
    ctx: &QueryContext,
    left_keys: &mut Vec<String>,
    right_keys: &mut Vec<String>,
) -> Option<()> {
    match expr.get(ctx) {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => {
            for arg in exprs {
                collect_equijoin_keys(*arg, ctx, left_keys, right_keys)?;
            }
            Some(())
        }
        ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => match (left.get(ctx), right.get(ctx)) {
            (ExprData::ColumnRef(left_col), ExprData::ColumnRef(right_col)) => {
                left_keys.push(ctx.column(*left_col).name.clone());
                right_keys.push(ctx.column(*right_col).name.clone());
                Some(())
            }
            _ => None,
        },
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
        ScalarValue::Decimal128 { .. }
        | ScalarValue::Date32(_)
        | ScalarValue::IntervalMonthDayNano { .. }
        | ScalarValue::IntervalDayTime { .. } => None,
    }
}
