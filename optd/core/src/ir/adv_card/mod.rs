//! Advanced cardinality estimator using table/column statistics and HLL synopses.
//!
//! # Scope (v1)
//!
//! Statistics-driven estimation for: **Scan**, **equality filter**, and
//! **equi-join** (PhysicalHashJoin only). All other operators fall back to
//! hardcoded "magic" constants identical to [`crate::magic::MagicCardinalityEstimator`].
//!
//! # High-level approach
//!
//! * **Scan**: `|R| = table_stats.row_count`. Falls back to 1 000 when no stats.
//! * **Filter**: `|σ_p(R)| = sel(p) · |R|`. See [`selectivity::filter_selectivity`].
//! * **Hash Join**: `|R ⋈ S| = |R| · |S| · Π sel(k_i) · sel(non_equi_conds)`
//!   over equi-key pairs, refined by non-equi condition selectivity.
//!   See [`selectivity::equijoin_cardinality`].
//! * **Other joins / aggregates / projections**: magic-constant fallbacks.
//!
//! # Column-to-statistics lookup
//!
//! Each IR `Column` carries a `ColumnMeta.source: Option<DataSourceId>` that
//! records which base table the column originated from (set in
//! `add_base_table_columns`; `None` for derived columns). The [`column_stats`]
//! helper uses this provenance chain:
//!
//! ```text
//! Column → ColumnMeta.source → Catalog.describe_table → TableStatistics
//!        → (column.id - first_column_id) → ColumnStatistics
//! ```
//!
//! **Assumption:** column provenance is only valid for non-nested queries.
//! Derived columns (expressions, subqueries) have `source = None` and will
//! always miss the stats lookup, causing a fallback.

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
    /// Fallback selectivity when a join condition is a non-literal expression
    /// that we cannot decompose into equi-keys. Mirrors the constant from
    /// `MagicCardinalityEstimator`.
    ///
    /// Formula (fallback): `|R ⋈ S| = 0.4 · |R| · |S|`
    pub const FALLBACK_JOIN_SELECTIVITY: f64 = 0.4;

    /// Fallback selectivity for any predicate we cannot analyze (e.g. range,
    /// LIKE, function calls). Mirrors the constant from
    /// `MagicCardinalityEstimator`.
    ///
    /// Formula (fallback): `|σ_p(R)| = 0.1 · |R|`
    pub const FALLBACK_PREDICATE_SELECTIVITY: f64 = 0.1;

    /// Fraction of input rows assumed to survive a GROUP BY per grouping key.
    /// Applied exponentially: `sel = 0.2 ^ num_keys`.
    ///
    /// **Assumption:** each additional grouping key multiplicatively reduces
    /// the number of distinct groups by this factor.
    pub const FALLBACK_GROUP_BY_NDV_FACTOR: f64 = 0.2;

    /// Default table cardinality when no statistics are available at all.
    pub const FALLBACK_DEFAULT_CARDINALITY: usize = 1000;
}

/// Resolve an IR [`Column`] to its owning [`TableStatistics`] and the offset
/// of the column within that table's `column_statistics` vector.
///
/// Returns `None` (triggering a fallback in the caller) when any of:
/// - The column has no `source` (i.e. it is a derived/computed column).
/// - The source table has no statistics in the catalog.
/// - The column offset exceeds the statistics vector length.
///
/// **Assumption:** Column IDs are allocated contiguously per table starting
/// from `first_column_id`, so `offset = column.0 - first_column_id` gives the
/// position in `column_statistics`.
fn column_stats(ctx: &IRContext, column: Column) -> Option<(TableStatistics, usize)> {
    let meta = ctx.get_column_meta(&column);
    // Provenance lookup: only base-table columns carry a source.
    // Derived columns (expressions, subqueries) have source = None.
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

/// Extract the number of distinct values (NDV) for a column.
///
/// Resolution order:
/// 1. `distinct_count` — precomputed exact or approximate count stored
///    directly in [`ColumnStatistics`].
/// 2. HyperLogLog synopsis — deserialized from `advanced_stats` entries
///    with `stats_type == "hll"`. Provides an approximate count with
///    ~1.04 / √(2^P) standard error (P = 12 → ~1.6% error).
///
/// Returns `None` if neither source yields a positive count, which causes
/// callers to fall back to magic constants.
fn ndv(col_stats: &ColumnStatistics) -> Option<usize> {
    // 1. Check precomputed distinct_count
    if let Some(dc) = col_stats.distinct_count {
        if dc > 0 {
            return Some(dc);
        }
    }
    // 2. Try HLL from advanced_stats
    for adv in &col_stats.advanced_stats {
        // TODO(AC): Replace with constants (enum?) in AdvColStats
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

        // Selectivity for non-hash joins where we only inspect the join condition
        // as a whole (no key decomposition). If the condition is a boolean literal
        // we know the exact selectivity; otherwise we fall back to 0.4.
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

        // Fallback join estimator for join types we don't have stats-driven
        // estimation for (LogicalJoin, DependentJoin, NLJoin).
        //
        // Formula by join type:
        //   Mark(col)  → |outer|           (adds a boolean column, no row change)
        //   Single     → min(sel · |L| · |R|, |L|)  (at-most-one match per left row)
        //   Otherwise  → sel · |L| · |R|            (standard cross-product model)
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
            // Formula: |Scan(T)| = T.row_count   (exact from catalog stats)
            // Fallback: 1000 when no stats are available.
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
            // Formula: |σ_p(R)| = sel(p) · |R|
            // where sel(p) is computed by selectivity::filter_selectivity.
            // base_row_count is used to compute null_fraction = null_count / row_count.
            OperatorKind::LogicalSelect(meta) => {
                let filter = LogicalSelect::borrow_raw_parts(meta, &op.common);
                let input_card = filter.input().cardinality(ctx);

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

            // --- Stats-driven: PhysicalHashJoin (equi-join + non-equi conditions) ---
            // Formula: |R ⋈ S| = |R| · |S| · Π_i 1/max(NDV_R(k_i), NDV_S(k_i)) · sel(non_equi)
            // See selectivity::equijoin_cardinality for full details.
            //
            // Non-equi conditions (e.g. x.b < y.b in "x.a = y.a AND x.b < y.b") are
            // applied as a post-join filter using filter_selectivity.
            //
            // Assumption: inner-join semantics. Other join types are not yet handled.
            OperatorKind::PhysicalHashJoin(meta) => {
                let join = PhysicalHashJoin::borrow_raw_parts(meta, &op.common);
                let build_card = join.build_side().cardinality(ctx);
                let probe_card = join.probe_side().cardinality(ctx);

                // TODO(AC/Yuchen): This assumes inner-join. Handle other join types by
                // passing in the entire `join` variable (or the join_type field.)
                selectivity::equijoin_cardinality(
                    join.keys(),
                    join.non_equi_conds(),
                    build_card,
                    probe_card,
                    ctx,
                )
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
            // Formula: |γ_{k1,...,kn}(R)| = 0.2^n  (fraction of input rows)
            //
            // Assumption: each grouping key independently reduces the group
            // count by a factor of 0.2. This does NOT use actual NDV from
            // statistics — it is a pure heuristic.
            //
            // Example: GROUP BY (a, b) → 0.2^2 = 0.04 → 4% of input rows.
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

            // --- Pass-through: these operators do not change row count ---
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
