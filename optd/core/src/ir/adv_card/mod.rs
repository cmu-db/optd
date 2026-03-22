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

use itertools::{Either, Itertools};

use crate::ir::{
    Column, ColumnSet, IRContext, Operator, Scalar,
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
    match ctx
        .catalog
        .table(source)
        .ok()
        .and_then(|table| table.statistics)
    {
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
            .and_then(|(table_stats, offset)| {
                let col_stats = table_stats.column_statistics.get(offset)?;
                ndv(col_stats)
            })
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
    ctx: &IRContext,
    join_type: &JoinType,
    outer: &Operator,
    inner: &Operator,
    join_cond: &Scalar,
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

/// Decompose a join condition into equi-key pairs and a non-equi remainder.
///
/// Returns `(keys, non_equi_cond)` where `keys` lists `(outer_col, inner_col)`
/// equi-join pairs and `non_equi_cond` is `None` when no non-equi conditions
/// remain (i.e. the remainder is trivially TRUE) and `Some(scalar)` otherwise.
///
/// Recognized patterns:
/// - `col_outer = col_inner` → single equi-key, non_equi = None
/// - `(col_outer = col_inner) AND ...` → multiple equi-keys, non-equi remainder
/// - anything else → no equi-keys, full condition as non_equi
///
/// Mirrors `LogicalJoinAsPhysicalHashJoinRule::split_equi_and_non_equi_conditions`
/// but operates directly on the join_cond scalar without creating a new operator.
fn extract_equi_and_non_equi(
    join_cond: &Arc<Scalar>,
    outer_cols: &ColumnSet,
    inner_cols: &ColumnSet,
) -> (Vec<(Column, Column)>, Option<Arc<Scalar>>) {
    // If `eq` is a `col_outer = col_inner` equality, return `(outer_col, inner_col)`.
    let try_get_equi_key = |eq: BinaryOpBorrowed<'_>| -> Option<(Column, Column)> {
        let lhs = eq.lhs().try_borrow::<ColumnRef>().ok()?;
        let rhs = eq.rhs().try_borrow::<ColumnRef>().ok()?;
        match (
            outer_cols.contains(lhs.column()),
            outer_cols.contains(rhs.column()),
            inner_cols.contains(lhs.column()),
            inner_cols.contains(rhs.column()),
        ) {
            (true, false, false, true) => Some((*lhs.column(), *rhs.column())),
            (false, true, true, false) => Some((*rhs.column(), *lhs.column())),
            _ => None,
        }
    };

    // Case 1: Single `col = col` equality.
    if let Ok(binop) = join_cond.try_borrow::<BinaryOp>()
        && binop.is_eq()
    {
        return match try_get_equi_key(binop) {
            Some(key) => (vec![key], None),
            None => (vec![], Some(join_cond.clone())),
        };
    }

    // Case 2: AND of terms — partition into equi-keys and non-equi remainder.
    if let Ok(nary) = join_cond.try_borrow::<NaryOp>()
        && nary.is_and()
    {
        let (keys, non_equi): (Vec<_>, Vec<Arc<Scalar>>) =
            nary.terms().iter().partition_map(|term| {
                if let Ok(binop) = term.try_borrow::<BinaryOp>()
                    && binop.is_eq()
                    && let Some(key) = try_get_equi_key(binop)
                {
                    return Either::Left(key);
                }
                Either::Right(term.clone())
            });

        let non_equi_scalar = match &non_equi[..] {
            [] => None,
            [singleton] => Some(singleton.clone()),
            terms => Some(NaryOp::new(NaryOpKind::And, terms.into()).into_scalar()),
        };
        return (keys, non_equi_scalar);
    }

    // Case 3: Anything else (non-equi condition, literal, etc.) — no keys.
    (vec![], Some(join_cond.clone()))
}

// ---------------------------------------------------------------------------
// Statistics lookup helpers
// ---------------------------------------------------------------------------

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

/// Look up the [`TableStatistics`] and column offset for a given [`Column`].
///
/// Delegates to [`IRContext::column_stats`] which caches results per
/// `table_index`, so repeated lookups for columns from the same table
/// avoid re-hitting the binder and catalog.
fn column_stats(ctx: &IRContext, column: Column) -> Option<(TableStatistics, usize)> {
    ctx.column_stats(&column)
}

fn extract_skip_bound(skip: &Scalar) -> Option<u64> {
    let literal = skip.try_borrow::<Literal>().ok()?;
    match literal.value() {
        crate::ir::ScalarValue::Int8(Some(v)) => u64::try_from(*v).ok(),
        crate::ir::ScalarValue::Int16(Some(v)) => u64::try_from(*v).ok(),
        crate::ir::ScalarValue::Int32(Some(v)) => u64::try_from(*v).ok(),
        crate::ir::ScalarValue::Int64(Some(v)) => u64::try_from(*v).ok(),
        crate::ir::ScalarValue::UInt8(Some(v)) => Some(u64::from(*v)),
        crate::ir::ScalarValue::UInt16(Some(v)) => Some(u64::from(*v)),
        crate::ir::ScalarValue::UInt32(Some(v)) => Some(u64::from(*v)),
        crate::ir::ScalarValue::UInt64(Some(v)) => Some(*v),
        _ => None,
    }
}

fn extract_fetch_bound(fetch: &Scalar) -> Option<u64> {
    let literal = fetch.try_borrow::<Literal>().ok()?;
    match literal.value() {
        crate::ir::ScalarValue::Int8(Some(v)) => u64::try_from(*v).ok(),
        crate::ir::ScalarValue::Int16(Some(v)) => u64::try_from(*v).ok(),
        crate::ir::ScalarValue::Int32(Some(v)) => u64::try_from(*v).ok(),
        crate::ir::ScalarValue::Int64(Some(v)) => u64::try_from(*v).ok(),
        crate::ir::ScalarValue::UInt8(Some(v)) => Some(u64::from(*v)),
        crate::ir::ScalarValue::UInt16(Some(v)) => Some(u64::from(*v)),
        crate::ir::ScalarValue::UInt32(Some(v)) => Some(u64::from(*v)),
        crate::ir::ScalarValue::UInt64(Some(v)) => Some(*v),
        crate::ir::ScalarValue::Int8(None)
        | crate::ir::ScalarValue::Int16(None)
        | crate::ir::ScalarValue::Int32(None)
        | crate::ir::ScalarValue::Int64(None) => None,
        _ => None,
    }
}

impl CardinalityEstimator for AdvancedCardinalityEstimator {
    fn estimate(&self, op: &Operator, ctx: &IRContext) -> Cardinality {
        use crate::ir::OperatorKind;

        match &op.kind {
            OperatorKind::Group(_) => {
                unreachable!("Group nodes must have cardinality set before estimation")
            }
            OperatorKind::Get(meta) => estimate_scan(ctx, meta.data_source_id),
            OperatorKind::Select(meta) => {
                let filter = Select::borrow_raw_parts(meta, &op.common);
                estimate_filter(filter.input(), filter.predicate(), ctx)
            }
            OperatorKind::Join(meta) => {
                let join = Join::borrow_raw_parts(meta, &op.common);
                let join_type = join.join_type();
                let join_cond = join.join_cond();
                let outer = join.outer();
                let inner = join.inner();

                // TODO(AC): Fix this unwrap.
                let outer_cols = outer.output_columns(ctx).unwrap();
                let inner_cols = inner.output_columns(ctx).unwrap();

                let (keys, non_equi) =
                    extract_equi_and_non_equi(join_cond, &outer_cols, &inner_cols);

                if keys.is_empty() {
                    estimate_fallback_join(ctx, join_type, outer, inner, join_cond)
                } else {
                    let outer_card = outer.cardinality(ctx);
                    let inner_card = inner.cardinality(ctx);
                    selectivity::equijoin_cardinality(
                        &keys,
                        non_equi.as_deref(),
                        join_type,
                        outer.cardinality(ctx),
                        inner.cardinality(ctx),
                        ctx,
                    )
                }
            }
            OperatorKind::DependentJoin(meta) => {
                let join = DependentJoin::borrow_raw_parts(meta, &op.common);
                estimate_fallback_join(
                    ctx,
                    join.join_type(),
                    join.outer().as_ref(),
                    join.inner().as_ref(),
                    join.join_cond().as_ref(),
                )
            }
            OperatorKind::Aggregate(meta) => {
                let agg = Aggregate::borrow_raw_parts(meta, &op.common);
                estimate_aggregate(agg.keys().borrow::<List>().members(), agg.input(), ctx)
            }
            OperatorKind::OrderBy(meta) => OrderBy::borrow_raw_parts(meta, &op.common)
                .input()
                .cardinality(ctx),
            OperatorKind::Subquery(meta) => Subquery::borrow_raw_parts(meta, &op.common)
                .input()
                .cardinality(ctx),
            OperatorKind::EnforcerSort(meta) => EnforcerSort::borrow_raw_parts(meta, &op.common)
                .input()
                .cardinality(ctx),
            OperatorKind::Project(meta) => Project::borrow_raw_parts(meta, &op.common)
                .input()
                .cardinality(ctx),
            OperatorKind::Remap(meta) => Remap::borrow_raw_parts(meta, &op.common)
                .input()
                .cardinality(ctx),
            OperatorKind::MockScan(meta) => meta.spec.mocked_card,
            OperatorKind::Limit(meta) => {
                let limit = Limit::borrow_raw_parts(meta, &op.common);
                let input_card = limit.input().cardinality(ctx);
                let remaining = extract_skip_bound(limit.skip().as_ref())
                    .map(|skip| (input_card.as_f64() - skip as f64).max(0.0))
                    .unwrap_or(input_card.as_f64());

                extract_fetch_bound(limit.fetch().as_ref())
                    .map(|fetch| remaining.min(fetch as f64))
                    .map(Cardinality::new)
                    .unwrap_or_else(|| Cardinality::new(remaining))
            }
        }
    }
}

#[cfg(test)]
mod tests;
