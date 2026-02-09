//! Selectivity estimation helpers for filters and equi-joins.

use crate::ir::{
    Column, IRContext, Scalar, properties::Cardinality, scalar::*, statistics::ColumnStatistics,
};

use super::{AdvancedCardinalityEstimator, column_stats, ndv};

/// Compute filter selectivity for a predicate.
///
/// Returns a factor in [0.0, 1.0]. The caller multiplies by input cardinality.
/// `base_row_count` is the row count of the base table (for null-fraction computation).
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

        // N-ary AND: product of selectivities
        ScalarKind::NaryOp(meta) if meta.op_kind == NaryOpKind::And => {
            let nary = NaryOp::borrow_raw_parts(meta, &predicate.common);
            nary.terms()
                .iter()
                .map(|t| filter_selectivity(t, ctx, base_row_count))
                // TODO(AC): Do we want to to the diminishing effect of each subsequent sel?
                .product()
        }

        // N-ary OR: 1 - product(1 - sel_i) (inclusion-exclusion generalized)
        ScalarKind::NaryOp(meta) if meta.op_kind == NaryOpKind::Or => {
            let nary = NaryOp::borrow_raw_parts(meta, &predicate.common);
            let product_complement: f64 = nary
                .terms()
                .iter()
                .map(|t| 1.0 - filter_selectivity(t, ctx, base_row_count))
                // TODO(AC): Do we want to to the diminishing effect of each subsequent sel?
                .product();
            1.0 - product_complement
        }

        // Everything else: fallback
        _ => AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY,
    }
}

/// Try to compute selectivity for `col_expr = lit_expr`.
/// Returns `Some(selectivity)` if col_expr is a ColumnRef and we can look up stats.
fn equality_selectivity_col_lit(
    col_expr: &Scalar,
    _lit_expr: &Scalar,
    ctx: &IRContext,
    base_row_count: usize,
) -> Option<f64> {
    // col_expr must be a ColumnRef
    let ScalarKind::ColumnRef(col_meta) = &col_expr.kind else {
        return None;
    };

    let (stats, offset) = column_stats(ctx, col_meta.column)?;
    let col_stats = stats.column_statistics.get(offset)?;
    let distinct = ndv(col_stats)?;

    // Null adjustment: selectivity applies only to non-null rows
    let null_fraction = if base_row_count > 0 {
        col_stats
            .null_count
            .map(|nc| nc as f64 / base_row_count as f64)
            .unwrap_or(0.0)
    } else {
        0.0
    };

    Some((1.0 - null_fraction) / distinct as f64)
}

/// Estimate cardinality for a PhysicalHashJoin given pre-extracted equi-join keys.
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
                // Domain-overlap check
                if !domains_overlap(build_cs, probe_cs) {
                    // Disjoint domains
                    return Cardinality::new(0.0);
                }

                let build_ndv = ndv(build_cs);
                let probe_ndv = ndv(probe_cs);

                match (build_ndv, probe_ndv) {
                    (Some(bn), Some(pn)) => {
                        let max_ndv = bn.max(pn) as f64;
                        selectivity *= 1.0 / max_ndv;
                        any_key_had_stats = true;
                    }
                    (Some(n), None) | (None, Some(n)) => {
                        selectivity *= 1.0 / n as f64;
                        any_key_had_stats = true;
                    }
                    (None, None) => {
                        selectivity *= AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
                    }
                }
            }
            // At least one side has stats with NDV
            (Some((_, cs)), None) | (None, Some((_, cs))) => {
                if let Some(n) = ndv(cs) {
                    selectivity *= 1.0 / n as f64;
                    any_key_had_stats = true;
                } else {
                    selectivity *= AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
                }
            }
            // No stats on either side
            (None, None) => {
                selectivity *= AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
            }
        }
    }

    if keys.is_empty() && !any_key_had_stats {
        selectivity = AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
    }

    Cardinality::new(build_card.as_f64() * probe_card.as_f64() * selectivity)
}

/// Check if two columns have overlapping domains using min/max values.
/// Returns true if domains overlap or if min/max are unavailable/non-numeric.
fn domains_overlap(a: &ColumnStatistics, b: &ColumnStatistics) -> bool {
    let (a_min, a_max) = match (&a.min_value, &a.max_value) {
        (Some(min), Some(max)) => (min, max),
        _ => return true, // Can't determine, assume overlap
    };
    let (b_min, b_max) = match (&b.min_value, &b.max_value) {
        (Some(min), Some(max)) => (min, max),
        _ => return true,
    };

    // Only compare numeric values
    match (
        a_min.as_f64(),
        a_max.as_f64(),
        b_min.as_f64(),
        b_max.as_f64(),
    ) {
        (Some(a_lo), Some(a_hi), Some(b_lo), Some(b_hi)) => {
            // Ranges overlap if NOT (a_hi < b_lo OR b_hi < a_lo)
            !(a_hi < b_lo || b_hi < a_lo)
        }
        _ => true, // Non-numeric, assume overlap
    }
}
