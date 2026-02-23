//! Advanced cardinality estimator using table/column statistics and HLL synopses.
//!
//! # Scope (v1)
//!
//! Statistics-driven estimation for: **Scan**, **equality filter**,
//! **range filter** (Lt/Le/Gt/Ge with min/max stats), **equi-join**
//! (PhysicalHashJoin only), and **aggregate** (GROUP BY using column NDV).
//! Range filters use continuous uniform distribution over `[min, max]`.
//! AND/OR conjunctions detect same-column range contradictions (→ 0) and
//! tautologies (→ 1). All other operators fall back to hardcoded "magic"
//! constants identical to [`crate::magic::MagicCardinalityEstimator`].
//!
//! # High-level approach
//!
//! * **Scan**: `|R| = table_stats.row_count`. Falls back to 1 000 when no stats.
//! * **Filter**: `|σ_p(R)| = sel(p) · |R|`. See [`selectivity::filter_selectivity`].
//! * **Hash Join**: `|R ⋈ S| = |R| · |S| · Π sel(k_i) · sel(non_equi_conds)`
//!   over equi-key pairs, refined by non-equi condition selectivity.
//!   See [`selectivity::equijoin_cardinality`].
//! * **Aggregate**: `min(|input|, Π NDV(k_i))` over grouping keys; falls back
//!   to `|input| × 0.2^k` per key when column stats are unavailable. Scalar
//!   aggregates (0 keys) always return 1.
//! * **Other joins / projections**: magic-constant fallbacks.
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
use std::sync::Arc;

use crate::ir::{
    Column, IRContext, Operator, Scalar,
    catalog::DataSourceId,
    convert::IntoScalar,
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

// ---------------------------------------------------------------------------
// Shared estimation helpers (deduplicated across logical/physical variants)
// ---------------------------------------------------------------------------

/// Estimate scan cardinality from catalog statistics.
///
/// Returns `table_stats.row_count` if available, otherwise
/// [`AdvancedCardinalityEstimator::FALLBACK_DEFAULT_CARDINALITY`].
fn estimate_scan(ctx: &IRContext, source: DataSourceId) -> Cardinality {
    match ctx.cat.describe_table(source).stats {
        Some(stats) => Cardinality::with_count_lossy(stats.row_count),
        None => Cardinality::with_count_lossy(
            AdvancedCardinalityEstimator::FALLBACK_DEFAULT_CARDINALITY,
        ),
    }
}

/// Estimate filter cardinality: `sel(predicate) · |input|`.
fn estimate_filter(input: &Operator, predicate: &Scalar, ctx: &IRContext) -> Cardinality {
    let input_card = input.cardinality(ctx);
    let sel = selectivity::filter_selectivity(predicate, ctx);
    sel * input_card
}

/// Estimate aggregate cardinality using column NDV statistics where available.
///
/// - **0 keys (scalar aggregate):** always returns 1 output row.
/// - **k ≥ 1 keys:** computes `min(input_card, Π NDV(k_i))` where each key's NDV is:
///   - looked up from column statistics when the key is a bare `ColumnRef` with provenance, or
///   - estimated as `input_card × FALLBACK_GROUP_BY_NDV_FACTOR` when stats are unavailable.
///
/// The result is floored at 1 to avoid sub-row estimates from pure-magic fallbacks on
/// tiny inputs.
fn estimate_aggregate(keys: &[Arc<Scalar>], input: &Operator, ctx: &IRContext) -> Cardinality {
    if keys.is_empty() {
        // Scalar aggregate (SELECT agg(...) FROM t with no GROUP BY) always produces 1 row.
        return Cardinality::UNIT;
    }

    let input_card = input.cardinality(ctx);

    let mut ndv_product: f64 = 1.0;
    for key in keys {
        let key_ndv = key
            .try_borrow::<ColumnRef>()
            .ok()
            .and_then(|col_ref| column_stats(ctx, *col_ref.column()))
            .and_then(|(table_stats, offset)| ndv(&table_stats.column_statistics[offset]))
            .map(|n| n as f64)
            .unwrap_or_else(|| {
                input_card.as_f64() * AdvancedCardinalityEstimator::FALLBACK_GROUP_BY_NDV_FACTOR
            });
        ndv_product *= key_ndv;
    }

    // Cap at input cardinality; floor at 1 to avoid sub-row estimates.
    Cardinality::new(ndv_product.min(input_card.as_f64()).max(1.0))
}

/// Compute selectivity for a join condition as a whole (no key decomposition).
///
/// If the condition is a boolean literal we know the exact selectivity;
/// otherwise we fall back to [`AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY`].
fn join_cond_selectivity(join_cond: &Scalar) -> f64 {
    if let Ok(literal) = join_cond.try_borrow::<Literal>() {
        match literal.value() {
            crate::ir::ScalarValue::Boolean(Some(true)) => 1.,
            crate::ir::ScalarValue::Boolean(_) => 0.,
            _ => unreachable!("join condition must be boolean"),
        }
    } else {
        AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY
    }
}

/// Fallback join estimator for join types without stats-driven estimation.
///
/// Formula by join type:
///   Mark(col)  → |outer|           (adds a boolean column, no row change)
///   Single     → min(sel · |L| · |R|, |L|)  (at-most-one match per left row)
///   Otherwise  → sel · |L| · |R|            (standard cross-product model)
fn estimate_fallback_join(
    join_type: &JoinType,
    outer: &Operator,
    inner: &Operator,
    join_cond: &Scalar,
    ctx: &IRContext,
) -> Cardinality {
    let left_card = outer.cardinality(ctx);
    let right_card = inner.cardinality(ctx);
    match *join_type {
        JoinType::Mark(_) => left_card,
        JoinType::Single => {
            let sel = join_cond_selectivity(join_cond);
            (sel * left_card * right_card).map(|value| value.min(left_card.as_f64()))
        }
        _ => {
            let sel = join_cond_selectivity(join_cond);
            sel * left_card * right_card
        }
    }
}

// ---------------------------------------------------------------------------
// Statistics lookup helpers
// ---------------------------------------------------------------------------

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
// TODO(perf): Replace `Arc<Mutex<HashMap>>` on `source_to_first_column_id`
// with a `RwLock` or freeze the map into a plain `Arc<HashMap>` before
// optimization begins, since `add_base_table_columns` is the only writer and
// runs during plan construction (before Cascades optimization). This would
// eliminate mutex contention when `column_stats()` is called many times during
// optimization.
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

/// Extract the deserialized HLL sketch from a column's advanced statistics.
///
/// Returns `None` if no HLL entry exists or deserialization fails.
// TODO(perf): Cache deserialized HLLs to avoid re-parsing the register array
// from `serde_json::Value` on every call. During Cascades optimization the
// same join operator may be estimated dozens of times. A
// `HashMap<(DataSourceId, usize), StoredHLL>` in `IRContext` or at the
// estimator level would eliminate O(2^P) = O(4096) deserialization per call.
fn extract_hll(col_stats: &ColumnStatistics) -> Option<StoredHLL> {
    for adv in &col_stats.advanced_stats {
        if adv.stats_type == "hll"
            && let Ok(hll) = serde_json::from_value::<StoredHLL>(adv.data.clone())
        {
            return Some(hll);
        }
    }
    None
}

/// Compute null fraction for a column: `null_count / table_row_count`.
fn null_fraction(col_stats: &ColumnStatistics, table_row_count: usize) -> f64 {
    if table_row_count > 0 {
        col_stats
            .null_count
            .map(|nc| nc as f64 / table_row_count as f64)
            .unwrap_or(0.0)
    } else {
        0.0
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
    if let Some(dc) = col_stats.distinct_count
        && dc > 0
    {
        return Some(dc);
    }

    // 2. Try HLL from advanced_stats
    if let Some(hll) = extract_hll(col_stats) {
        let count = hll.approx_distinct();
        if count > 0 {
            return Some(count);
        }
    }
    None
}

impl CardinalityEstimator for AdvancedCardinalityEstimator {
    fn estimate(&self, op: &Operator, ctx: &IRContext) -> Cardinality {
        use crate::ir::OperatorKind;

        match &op.kind {
            OperatorKind::Group(_) => {
                unreachable!("Group nodes must have cardinality set before estimation")
            }

            // --- Stats-driven: Scan ---
            OperatorKind::LogicalGet(meta) => estimate_scan(ctx, meta.source),
            OperatorKind::PhysicalTableScan(meta) => estimate_scan(ctx, meta.source),

            // --- Stats-driven: Filter ---
            OperatorKind::LogicalSelect(meta) => {
                let filter = LogicalSelect::borrow_raw_parts(meta, &op.common);
                estimate_filter(filter.input(), filter.predicate(), ctx)
            }
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilter::borrow_raw_parts(meta, &op.common);
                estimate_filter(filter.input(), filter.predicate(), ctx)
            }

            // --- Stats-driven: PhysicalHashJoin (equi-join + non-equi conditions) ---
            OperatorKind::PhysicalHashJoin(meta) => {
                let join = PhysicalHashJoin::borrow_raw_parts(meta, &op.common);
                let build_card = join.build_side().cardinality(ctx);
                let probe_card = join.probe_side().cardinality(ctx);

                selectivity::equijoin_cardinality(
                    join.keys(),
                    join.non_equi_conds(),
                    join.join_type(),
                    build_card,
                    probe_card,
                    ctx,
                )
            }

            // --- Fallback: other joins ---
            OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoin::borrow_raw_parts(meta, &op.common);
                estimate_fallback_join(
                    join.join_type(),
                    join.outer().as_ref(),
                    join.inner().as_ref(),
                    join.join_cond().as_ref(),
                    ctx,
                )
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                let join = LogicalDependentJoin::borrow_raw_parts(meta, &op.common);
                estimate_fallback_join(
                    join.join_type(),
                    join.outer().as_ref(),
                    join.inner().as_ref(),
                    join.join_cond().as_ref(),
                    ctx,
                )
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoin::borrow_raw_parts(meta, &op.common);
                estimate_fallback_join(
                    join.join_type(),
                    join.outer().as_ref(),
                    join.inner().as_ref(),
                    join.join_cond().as_ref(),
                    ctx,
                )
            }

            // --- Fallback: aggregates ---
            OperatorKind::LogicalAggregate(meta) => {
                let agg = LogicalAggregate::borrow_raw_parts(meta, &op.common);
                estimate_aggregate(agg.keys().borrow::<List>().members().len())
            }
            OperatorKind::PhysicalHashAggregate(meta) => {
                let agg = PhysicalHashAggregate::borrow_raw_parts(meta, &op.common);
                estimate_aggregate(agg.keys().borrow::<List>().members().len())
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
