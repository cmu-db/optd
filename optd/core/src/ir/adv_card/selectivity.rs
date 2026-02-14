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
//! ```text
//! |R ⋈ S| = |R| · |S| · Π_i  1 / max(NDV_R(k_i), NDV_S(k_i))
//! ```
//!
//! With a domain-overlap pre-check per key pair: if `[min, max]` ranges are
//! disjoint the result is 0 immediately.

use crate::ir::{
    Column, IRContext, Scalar, properties::Cardinality, scalar::*, statistics::ColumnStatistics,
};

use super::{AdvancedCardinalityEstimator, column_stats, ndv};

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
    let distinct = ndv(col_stats)?;

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

/// Estimate cardinality for a PhysicalHashJoin on pre-extracted equi-join keys.
///
/// # Formula
///
/// ```text
/// |R ⋈ S| = |R| · |S| · Π_i sel(k_i)
/// ```
///
/// where for each key pair `(k_i_R, k_i_S)`:
///
/// ```text
/// sel(k_i) = 1 / max(NDV_R(k_i), NDV_S(k_i))
/// ```
///
/// This is the standard equi-join selectivity from Selinger et al. (1979).
/// Using `max(NDV)` rather than `min(NDV)` gives a more conservative (lower)
/// estimate, which avoids over-counting when the smaller-NDV side has values
/// not present in the larger-NDV side.
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
/// - **Containment assumption:** the values of the smaller-NDV column are a
///   subset of the larger-NDV column. This justifies using `max(NDV)`.
/// - **Non-equi conditions are ignored.** E.g. `x.a = y.a AND x.b < y.b` only
///   uses the `x.a = y.a` key; the range condition on `b` is not factored in.
pub fn equijoin_cardinality(
    keys: &[(Column, Column)],
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
            (Some((_, build_cs)), Some((_, probe_cs))) => {
                // Domain-overlap check: if [min, max] ranges are disjoint,
                // no rows can match on this key → entire join produces 0 rows.
                if !domains_overlap(build_cs, probe_cs) {
                    return Cardinality::new(0.0);
                }

                let build_ndv = ndv(build_cs);
                let probe_ndv = ndv(probe_cs);

                match (build_ndv, probe_ndv) {
                    // sel(k_i) = 1 / max(NDV_build, NDV_probe)
                    (Some(bn), Some(pn)) => {
                        let max_ndv = bn.max(pn) as f64;
                        selectivity *= 1.0 / max_ndv;
                        any_key_had_stats = true;
                    }
                    // (Some(n), None) | (None, Some(n)) => {
                    //     // This uses ONLY the selectivity
                    //     // of the only table. We assume that the ndv of the other table is
                    //     // <= n.
                    //     // TODO(AC): Document the assumption.
                    //     // TODO(AC): Can we assume that the NDV of other column is row_count (i.e. primary key)
                    //     //          -- No don't. Test if it is better, probably not because row_count will dominate ndv usually.
                    //     // TODO(AC): low priority -- check if min(1/n, fallback) is better than using just the fallback?
                    //     selectivity *= 1.0 / n as f64;
                    //     any_key_had_stats = true;
                    // }
                    (_, _) => {
                        // TODO(AC): Try row counts here.
                        selectivity *= AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
                    }
                }
            }
            // TODO(AC): Document why commenting out.
            // // At least one side has stats with NDV
            // (Some((_, cs)), None) | (None, Some((_, cs))) => {
            //     if let Some(n) = ndv(cs) {
            //         selectivity *= 1.0 / n as f64;
            //         any_key_had_stats = true;
            //     } else {
            //         selectivity *= AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
            //     }
            // }
            // No stats on either side
            (_, _) => {
                selectivity *= AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
            }
        }
    }

    if keys.is_empty() && !any_key_had_stats {
        selectivity = AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
    }

    // TODO(AC): Also call card estimation on non-equi conditions obtained via the predicates.

    Cardinality::new(build_card.as_f64() * probe_card.as_f64() * selectivity)
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
