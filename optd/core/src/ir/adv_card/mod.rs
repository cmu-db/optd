//! Advanced cardinality estimator using table/column statistics and HLL synopses.
//!
//! v1 scope: Scan + equality filter + equi-join (PhysicalHashJoin only).
//! Falls back to magic constants for unsupported operators.

mod selectivity;

use std::hash::BuildHasherDefault;

use crate::ir::{
    Column, IRContext, Operator, Scalar,
    operator::join::JoinType,
    operator::*,
    properties::{Cardinality, CardinalityEstimator},
    scalar::*,
    statistics::{ColumnStatistics, TableStatistics},
};
use synopses::{distinct::HyperLogLog, utils::Murmur2Hash64a};

/// Canonical HLL type for catalog storage and deserialization.
pub type StoredHLL = HyperLogLog<Vec<u8>, BuildHasherDefault<Murmur2Hash64a>, 12>;

pub struct AdvancedCardinalityEstimator;

impl AdvancedCardinalityEstimator {
    pub const FALLBACK_JOIN_SELECTIVITY: f64 = 0.4;
    pub const FALLBACK_PREDICATE_SELECTIVITY: f64 = 0.1;
    pub const FALLBACK_GROUP_BY_NDV_FACTOR: f64 = 0.2;
    pub const FALLBACK_DEFAULT_CARDINALITY: usize = 1000;
}

/// Look up TableStatistics and column offset for an IR Column.
/// Returns None if: column has no source table, table has no stats,
/// or column offset is out of bounds.
fn column_stats(ctx: &IRContext, column: Column) -> Option<(TableStatistics, usize)> {
    let meta = ctx.get_column_meta(&column);
    let source = meta.source?;
    let table_meta = ctx.cat.describe_table(source);
    let stats = table_meta.stats?;
    let first_col = ctx
        .source_to_first_column_id
        .lock()
        .unwrap()
        .get(&source)?
        .0;
    let offset = column.0.checked_sub(first_col)?;
    if offset < stats.column_statistics.len() {
        Some((stats, offset))
    } else {
        None
    }
}

/// Extract NDV for a column. Checks distinct_count first, then tries
/// deserializing HLL from advanced_stats.
fn ndv(col_stats: &ColumnStatistics) -> Option<usize> {
    // 1. Check precomputed distinct_count
    if let Some(dc) = col_stats.distinct_count {
        if dc > 0 {
            return Some(dc);
        }
    }
    // 2. Try HLL from advanced_stats
    for adv in &col_stats.advanced_stats {
        // TODO(AC): Replace with constants in AdvColStats?
        if adv.stats_type == "hll" {
            if let Ok(hll) = serde_json::from_value::<StoredHLL>(adv.data.clone()) {
                let count = hll.approx_distinct();
                if count > 0 {
                    return Some(count);
                }
            }
        }
    }
    None
}

impl CardinalityEstimator for AdvancedCardinalityEstimator {
    fn estimate(&self, op: &Operator, ctx: &IRContext) -> Cardinality {
        use crate::ir::OperatorKind;

        let join_selectivity = |join_cond: &Scalar| {
            if let Ok(literal) = join_cond.try_borrow::<Literal>() {
                match literal.value() {
                    crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                    crate::ir::ScalarValue::Boolean(_) => 0.,
                    _ => unreachable!("join condition must be boolean"),
                }
            } else {
                Self::FALLBACK_JOIN_SELECTIVITY
            }
        };

        let estimate_join =
            |join_type: &JoinType, outer: &Operator, inner: &Operator, join_cond: &Scalar| {
                let left_card = outer.cardinality(ctx);
                let right_card = inner.cardinality(ctx);
                match *join_type {
                    JoinType::Mark(_) => left_card,
                    JoinType::Single => {
                        let sel = join_selectivity(join_cond);
                        (sel * left_card * right_card).map(|value| value.min(left_card.as_f64()))
                    }
                    _ => {
                        let sel = join_selectivity(join_cond);
                        sel * left_card * right_card
                    }
                }
            };

        match &op.kind {
            OperatorKind::Group(_) => {
                panic!("right now should always be set");
            }

            // --- Stats-driven: Scan ---
            OperatorKind::LogicalGet(meta) => match ctx.cat.describe_table(meta.source).stats {
                Some(stats) => Cardinality::with_count_lossy(stats.row_count),
                None => Cardinality::with_count_lossy(Self::FALLBACK_DEFAULT_CARDINALITY),
            },
            OperatorKind::PhysicalTableScan(meta) => {
                match ctx.cat.describe_table(meta.source).stats {
                    Some(stats) => Cardinality::with_count_lossy(stats.row_count),
                    None => Cardinality::with_count_lossy(Self::FALLBACK_DEFAULT_CARDINALITY),
                }
            }

            // --- Stats-driven: Filter ---
            OperatorKind::LogicalSelect(meta) => {
                let filter = LogicalSelect::borrow_raw_parts(meta, &op.common);
                let input_card = filter.input().cardinality(ctx);

                // Get base table row count for null-fraction computation
                let base_row_count = input_card.as_f64() as usize;
                let sel = selectivity::filter_selectivity(filter.predicate(), ctx, base_row_count);
                sel * input_card
            }
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilter::borrow_raw_parts(meta, &op.common);
                let input_card = filter.input().cardinality(ctx);
                let base_row_count = input_card.as_f64() as usize;
                let sel = selectivity::filter_selectivity(filter.predicate(), ctx, base_row_count);
                sel * input_card
            }

            // --- Stats-driven: PhysicalHashJoin ---
            OperatorKind::PhysicalHashJoin(meta) => {
                let join = PhysicalHashJoin::borrow_raw_parts(meta, &op.common);
                let build_card = join.build_side().cardinality(ctx);
                let probe_card = join.probe_side().cardinality(ctx);

                selectivity::equijoin_cardinality(join.keys(), build_card, probe_card, ctx)
            }

            // TODO(AC): For all the other joins, use our estimator

            // --- Fallback: other joins ---
            OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoin::borrow_raw_parts(meta, &op.common);
                estimate_join(
                    join.join_type(),
                    join.outer().as_ref(),
                    join.inner().as_ref(),
                    join.join_cond().as_ref(),
                )
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                let join = LogicalDependentJoin::borrow_raw_parts(meta, &op.common);
                estimate_join(
                    join.join_type(),
                    join.outer().as_ref(),
                    join.inner().as_ref(),
                    join.join_cond().as_ref(),
                )
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoin::borrow_raw_parts(meta, &op.common);
                let sel = if let Ok(literal) = join.join_cond().try_borrow::<Literal>() {
                    match literal.value() {
                        crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
                        crate::ir::ScalarValue::Boolean(_) => 0.,
                        _ => unreachable!("join condition must be boolean"),
                    }
                } else {
                    Self::FALLBACK_JOIN_SELECTIVITY
                };
                let left_card = join.outer().cardinality(ctx);
                let right_card = join.inner().cardinality(ctx);
                sel * left_card * right_card
            }

            // --- Fallback: aggregates ---
            OperatorKind::LogicalAggregate(meta) => {
                let agg = LogicalAggregate::borrow_raw_parts(meta, &op.common);
                let len = agg.keys().borrow::<List>().members().len();
                Cardinality::new(
                    Self::FALLBACK_GROUP_BY_NDV_FACTOR.powi(i32::try_from(len).unwrap()),
                )
            }
            OperatorKind::PhysicalHashAggregate(meta) => {
                let agg = PhysicalHashAggregate::borrow_raw_parts(meta, &op.common);
                let len = agg.keys().borrow::<List>().members().len();
                Cardinality::new(
                    Self::FALLBACK_GROUP_BY_NDV_FACTOR.powi(i32::try_from(len).unwrap()),
                )
            }

            // --- Pass-through ---
            OperatorKind::LogicalOrderBy(meta) => {
                LogicalOrderBy::borrow_raw_parts(meta, &op.common)
                    .input()
                    .cardinality(ctx)
            }
            OperatorKind::LogicalSubquery(meta) => {
                LogicalSubquery::borrow_raw_parts(meta, &op.common)
                    .input()
                    .cardinality(ctx)
            }
            OperatorKind::EnforcerSort(meta) => EnforcerSort::borrow_raw_parts(meta, &op.common)
                .input()
                .cardinality(ctx),
            OperatorKind::LogicalProject(meta) => {
                LogicalProject::borrow_raw_parts(meta, &op.common)
                    .input()
                    .cardinality(ctx)
            }
            OperatorKind::PhysicalProject(meta) => {
                PhysicalProject::borrow_raw_parts(meta, &op.common)
                    .input()
                    .cardinality(ctx)
            }
            OperatorKind::LogicalRemap(meta) => LogicalRemap::borrow_raw_parts(meta, &op.common)
                .input()
                .cardinality(ctx),

            OperatorKind::MockScan(meta) => meta.spec.mocked_card,
        }
    }
}

#[cfg(test)]
mod tests;
