//! Selectivity estimation helpers for filters and equi-joins.
//!
//! # Filter selectivity ([`filter_selectivity`])
//!
//! Recursively decomposes a predicate scalar tree and computes a selectivity
//! factor in `[0.0, 1.0]`.
//!
//! | Predicate shape           | Formula                                          |
//! |---------------------------|--------------------------------------------------|
//! | `TRUE`                    | 1.0                                              |
//! | `FALSE` / `NULL`          | 0.0                                              |
//! | `col = literal`           | `(1 - null_frac) / NDV(col)`                     |
//! | `p1 AND p2 AND ...`       | `Π sel(p_i)`  (independence assumption)          |
//! | `p1 OR  p2 OR  ...`       | `1 - Π (1 - sel(p_i))` (independence assumption) |
//! | anything else             | `FALLBACK_PREDICATE_SELECTIVITY` (0.1)           |
//!
//! # Equi-join cardinality ([`equijoin_cardinality`])
//!
//! For a hash join on key pairs `(k1_L, k1_R), ..., (kn_L, kn_R)`:
//!
//! **Preferred (HLL intersection):** when HyperLogLog sketches are available
//! on both sides of a key pair, we compute the intersection size via
//! inclusion-exclusion (`|A ∩ B| = |A| + |B| - |A ∪ B|`) and derive:
//!
//! ```text
//! sel(k_i) = |intersect(k_i)| / (NDV_R(k_i) · NDV_S(k_i))
//! ```
//!
//! **Fallback (containment assumption):** when HLLs are not available:
//!
//! ```text
//! sel(k_i) = 1 / max(NDV_R(k_i), NDV_S(k_i))
//! ```
//!
//! With a domain-overlap pre-check per key pair: if `[min, max]` ranges are
//! disjoint the result is 0 immediately.

use crate::ir::{
    Column, IRContext, Scalar, properties::Cardinality, scalar::*, statistics::ColumnStatistics,
};

use super::{AdvancedCardinalityEstimator, column_stats, extract_hll, ndv};

/// Compute filter selectivity for a predicate tree.
///
/// Returns a factor in `[0.0, 1.0]`. The caller multiplies by input cardinality
/// to get the estimated output cardinality: `|σ_p(R)| = sel(p) · |R|`.
///
/// `base_row_count` is the input row count, used to derive null fraction:
/// `null_frac = null_count / base_row_count`.
pub fn filter_selectivity(predicate: &Scalar, ctx: &IRContext, base_row_count: usize) -> f64 {
    match &predicate.kind {
        // Literal boolean
        ScalarKind::Literal(meta) => {
            let lit = Literal::borrow_raw_parts(meta, &predicate.common);
            match lit.value() {
                crate::ir::ScalarValue::Boolean(Some(true)) => 1.0,
                crate::ir::ScalarValue::Boolean(Some(false))
                | crate::ir::ScalarValue::Boolean(None) => 0.0,
                _ => AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY,
            }
        }

        // Equality: col = literal or literal = col
        ScalarKind::BinaryOp(meta) if meta.op_kind == BinaryOpKind::Eq => {
            let binop = BinaryOp::borrow_raw_parts(meta, &predicate.common);
            let lhs = binop.lhs();
            let rhs = binop.rhs();

            // Try col = literal
            if let Some(sel) = equality_selectivity_col_lit(lhs, rhs, ctx, base_row_count) {
                return sel;
            }
            // Try literal = col (commuted)
            if let Some(sel) = equality_selectivity_col_lit(rhs, lhs, ctx, base_row_count) {
                return sel;
            }
            AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY
        }

        // -- Future Optimization Idea: Domain Range Analysis --
        // TODO(AC): If we encounter equality or range filters that essentially leave
        // the intersection as NULL, maybe we should set the selectivity to 0 here?
        // For example "A > 10 and A < -10" or "A = 10 and A = 11"
        //
        // Also consider how to optimize cases like
        //  - "A < 10 or A > 10" -> "A != 10"
        //  - "A < 10 or A > 9" -> "True"
        // ------------------------------

        // N-ary AND: sel(p1 AND p2 AND ...) = Π sel(p_i)
        //
        // Assumption: all predicates are statistically independent.
        // This can under-estimate when predicates are correlated (e.g.
        // "age > 18 AND age < 25"). A decay function could model correlation
        // but is not yet implemented.
        ScalarKind::NaryOp(meta) if meta.op_kind == NaryOpKind::And => {
            let nary = NaryOp::borrow_raw_parts(meta, &predicate.common);
            nary.terms()
                .iter()
                .map(|t| filter_selectivity(t, ctx, base_row_count))
                .product()
        }

        // N-ary OR: sel(p1 OR p2 OR ...) = 1 - Π (1 - sel(p_i))
        //
        // Derived from inclusion-exclusion under the independence assumption:
        //   P(A ∪ B) = P(A) + P(B) - P(A)·P(B) = 1 - (1-P(A))·(1-P(B))
        // Generalized to n terms.
        //
        // Assumption: all predicates are statistically independent (same
        // caveat as AND above).
        ScalarKind::NaryOp(meta) if meta.op_kind == NaryOpKind::Or => {
            let nary = NaryOp::borrow_raw_parts(meta, &predicate.common);
            let product_complement: f64 = nary
                .terms()
                .iter()
                .map(|t| 1.0 - filter_selectivity(t, ctx, base_row_count))
                .product();
            1.0 - product_complement
        }

        // Everything else: fallback
        _ => AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY,
    }
}

/// Compute selectivity for an equality predicate `col = literal`.
///
/// # Formula
///
/// ```text
/// sel(col = v) = (1 - null_fraction) / NDV(col)
/// ```
///
/// where:
/// - `null_fraction = null_count / base_row_count` (0 if unknown)
/// - `NDV` = number of distinct non-null values from stats or HLL
///
/// # Assumptions
///
/// - **Uniform distribution:** every distinct value is equally likely. This is
///   the standard textbook assumption (Selinger et al., 1979). Skewed data
///   (e.g. Zipfian) will be under- or over-estimated unless histograms are used.
/// - **Literal value exists in the column domain.** We do not check whether the
///   literal actually falls within `[min, max]`. A value outside the domain
///   would ideally yield selectivity 0, but we don't implement that yet.
/// - **Null values never match equality.** SQL semantics: `NULL = x` is always
///   `NULL`/false, so we exclude the null fraction from the result.
///
/// Returns `None` if `col_expr` is not a `ColumnRef` or stats are unavailable.
fn equality_selectivity_col_lit(
    col_expr: &Scalar,
    _lit_expr: &Scalar,
    ctx: &IRContext,
    base_row_count: usize,
) -> Option<f64> {
    let ScalarKind::ColumnRef(col_meta) = &col_expr.kind else {
        return None;
    };

    let (stats, offset) = column_stats(ctx, col_meta.column)?;
    let col_stats = stats.column_statistics.get(offset)?;
    let distinct = ndv(col_stats).unwrap_or(base_row_count);

    // Null adjustment: null rows never satisfy an equality predicate.
    // null_fraction = null_count / base_row_count
    let null_fraction = if base_row_count > 0 {
        col_stats
            .null_count
            .map(|nc| nc as f64 / base_row_count as f64)
            .unwrap_or(0.0)
    } else {
        0.0
    };

    // sel = (1 - null_fraction) / NDV
    Some((1.0 - null_fraction) / distinct as f64)
}

/// Estimate cardinality for a PhysicalHashJoin on pre-extracted equi-join keys,
/// optionally refined by non-equi conditions.
///
/// # Formula
///
/// ```text
/// |R ⋈ S| = |R| · |S| · Π_i sel(k_i) · sel(non_equi_conds)
/// ```
///
/// where for each key pair `(k_i_R, k_i_S)`:
///
/// **Preferred (HLL intersection):** when both sides have HLL sketches:
/// ```text
/// sel(k_i) = |intersect(k_i)| / (NDV_R(k_i) · NDV_S(k_i))
/// ```
/// where `|intersect| = |A| + |B| - |A ∪ B|` via inclusion-exclusion on
/// merged HLL registers. This avoids the containment assumption and
/// directly estimates actual domain overlap.
///
/// **Fallback (containment assumption):** when HLLs are unavailable:
/// ```text
/// sel(k_i) = 1 / max(NDV_R(k_i), NDV_S(k_i))
/// ```
/// This is the standard equi-join selectivity from Selinger et al. (1979).
///
/// # Non-equi condition handling
///
/// After computing the equi-join cardinality, we apply the selectivity of any
/// non-equi conditions (e.g. `x.b < y.b` in `x.a = y.a AND x.b < y.b`).
/// These are treated as a post-join filter using [`filter_selectivity`], with the
/// equi-join result as the base row count for null-fraction computation.
///
/// When `non_equi_conds` is a literal `TRUE` (i.e. no non-equi conditions),
/// `filter_selectivity` returns 1.0 and the estimate is unchanged.
///
/// # Domain-overlap optimization
///
/// Before computing selectivity for a key pair, we check whether the `[min, max]`
/// ranges overlap. If they are provably disjoint (e.g. `[0, 99]` vs `[200, 299]`),
/// the join produces zero rows and we short-circuit immediately.
///
/// # Assumptions
///
/// - **Inner-join semantics only.** Other join types (LEFT, SEMI, etc.) are not
///   yet accounted for; they would need adjustment (e.g. LEFT JOIN ≥ |L|).
/// - **Independence across key pairs.** Composite keys `(k1, k2)` are treated as
///   independent: `sel = sel(k1) · sel(k2)`.
/// - **Uniform value distribution** within each column.
/// - **HLL intersection preferred:** when HLL sketches are available on both
///   sides, we use inclusion-exclusion to estimate the actual intersection
///   size, avoiding the containment assumption entirely. When HLLs are not
///   available, we fall back to the containment assumption (`1/max(NDV)`).
/// - **Independence between equi-keys and non-equi conditions.** The non-equi
///   selectivity is multiplied in independently. Correlated conditions (e.g.
///   `x.a = y.a AND x.a < 10`) may lead to under-estimation.
pub fn equijoin_cardinality(
    keys: &[(Column, Column)],
    non_equi_conds: &Scalar,
    build_card: Cardinality,
    probe_card: Cardinality,
    ctx: &IRContext,
) -> Cardinality {
    let mut selectivity: f64 = 1.0;
    let mut any_key_had_stats = false;

    for (build_col, probe_col) in keys {
        let build_info = column_stats(ctx, *build_col)
            .and_then(|(s, off)| s.column_statistics.get(off).cloned().map(|c| (s, c)));
        let probe_info = column_stats(ctx, *probe_col)
            .and_then(|(s, off)| s.column_statistics.get(off).cloned().map(|c| (s, c)));

        match (&build_info, &probe_info) {
            (Some((build_ts, build_cs)), Some((probe_ts, probe_cs))) => {
                // Domain-overlap check: if [min, max] ranges are disjoint,
                // no rows can match on this key → entire join produces 0 rows.
                if !domains_overlap(build_cs, probe_cs) {
                    return Cardinality::new(0.0);
                }

                // Prefer HLL intersection when both sketches are available.
                // Falls back to the containment assumption (1/max(NDV))
                // when HLLs are absent.
                if let Some(sel) = hll_join_selectivity(build_cs, probe_cs) {
                    selectivity *= sel;
                    any_key_had_stats = true;
                } else {
                    match (ndv(build_cs), ndv(probe_cs)) {
                        (Some(build_ndv), Some(probe_ndv)) => {
                            // sel(k_i) = 1 / max(NDV_build, NDV_probe)
                            let max_ndv = build_ndv.max(probe_ndv) as f64;
                            selectivity *= 1.0 / max_ndv;
                            any_key_had_stats = true;
                        }
                        // We could also check the case where at least one of the sides has some cardinality
                        // statistics available. For example, we can assume that ndv of the other table is
                        // <= n or we can use it's row-count as the NDV if available.
                        // However, we want to avoid mixing statistics in potentially un-explainable ways
                        // at the moment for the sake of simplicity.
                        // TODO(AC): Benchmark the case where:
                        // a. the ndv of other table is assumed <= n
                        // b. the ndv of other table is assumed = row_count if available
                        // (Some(n), None) | (None, Some(n)) => {
                        //     selectivity *= 1.0 / n as f64;
                        //     any_key_had_stats = true;
                        // }
                        (_, _) => {
                            // No NDV on either side: use max(row_count) as an
                            // upper-bound NDV estimate. NDV can never exceed the
                            // number of rows, so this is the most conservative
                            // assumption without distinct-value stats.
                            let br = build_ts.row_count;
                            let pr = probe_ts.row_count;
                            if br > 0 && pr > 0 {
                                selectivity *= 1.0 / br.max(pr) as f64;
                                any_key_had_stats = true;
                            } else {
                                selectivity *=
                                    AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
                            }
                        }
                    }
                }
            }
            // At least one side has no column_stats at all, so we don't
            // have access to its TableStatistics (row_count) either — the
            // current column_stats() lookup bundles table-level and
            // column-level stats together, so a missing column entry means
            // the entire TableStatistics is unavailable here.
            // TODO(AC): Propagate TableStatistics independently of
            // column-level stats so we can fall back to row_count even
            // when column stats are absent.
            (_, _) => {
                selectivity *= AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
            }
        }
    }

    if keys.is_empty() && !any_key_had_stats {
        selectivity = AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
    }

    // Apply selectivity of non-equi conditions (e.g. range predicates like x.b < y.b).
    // These are treated as a post-join filter on the equi-join result.
    // When non_equi_conds is literal TRUE, filter_selectivity returns 1.0 (no effect).
    let equijoin_card = build_card.as_f64() * probe_card.as_f64() * selectivity;
    let non_equi_sel = filter_selectivity(non_equi_conds, ctx, equijoin_card as usize);

    Cardinality::new(equijoin_card * non_equi_sel)
}

/// Compute equi-join selectivity using HLL set intersection.
///
/// Uses inclusion-exclusion on the HLL sketches:
/// `|A ∩ B| = |A| + |B| - |A ∪ B|`
///
/// Returns `sel = |intersect| / (NDV_A × NDV_B)`, or `None` if HLLs are
/// unavailable on either side.
fn hll_join_selectivity(build_cs: &ColumnStatistics, probe_cs: &ColumnStatistics) -> Option<f64> {
    let build_hll = extract_hll(build_cs)?;
    let probe_hll = extract_hll(probe_cs)?;

    let build_ndv = build_hll.approx_distinct();
    let probe_ndv = probe_hll.approx_distinct();
    if build_ndv == 0 || probe_ndv == 0 {
        return Some(0.0);
    }

    // Union via merge (takes max of registers)
    let mut union_hll = build_hll.clone();
    union_hll.merge(&probe_hll);
    let union_ndv = union_hll.approx_distinct();

    // Inclusion-exclusion: |A ∩ B| = |A| + |B| - |A ∪ B|
    // Clamped to 0 to handle HLL approximation error.
    let intersect = (build_ndv + probe_ndv).saturating_sub(union_ndv);
    if intersect == 0 {
        return Some(0.0);
    }

    Some(intersect as f64 / (build_ndv as f64 * probe_ndv as f64))
}

/// Check if two columns have overlapping value domains using min/max statistics.
///
/// Two numeric ranges `[a_lo, a_hi]` and `[b_lo, b_hi]` overlap iff
/// `NOT (a_hi < b_lo OR b_hi < a_lo)`.
///
/// Returns `true` (assumes overlap) when min/max are unavailable or non-numeric,
/// since we cannot prove disjointness.
fn domains_overlap(a: &ColumnStatistics, b: &ColumnStatistics) -> bool {
    let (a_min, a_max) = match (&a.min_value, &a.max_value) {
        (Some(min), Some(max)) => (min, max),
        _ => return true, // Can't determine, assume overlap
    };
    let (b_min, b_max) = match (&b.min_value, &b.max_value) {
        (Some(min), Some(max)) => (min, max),
        _ => return true,
    };

    // Try to parse as f64 for numeric comparison
    match (
        a_min.parse::<f64>(),
        a_max.parse::<f64>(),
        b_min.parse::<f64>(),
        b_max.parse::<f64>(),
    ) {
        (Ok(a_lo), Ok(a_hi), Ok(b_lo), Ok(b_hi)) => {
            // Ranges overlap if NOT (a_hi < b_lo OR b_hi < a_lo)
            !(a_hi < b_lo || b_hi < a_lo)
        }
        _ => true, // Non-numeric, assume overlap
    }
}
