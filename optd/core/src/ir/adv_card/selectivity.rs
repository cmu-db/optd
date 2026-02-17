//! Selectivity estimation helpers for filters and equi-joins.
//!
//! # Filter selectivity ([`filter_selectivity`])
//!
//! Recursively decomposes a predicate scalar tree and computes a selectivity
//! factor in `[0.0, 1.0]`.
//!
//! | Predicate shape                  | Formula                                                          |
//! |----------------------------------|------------------------------------------------------------------|
//! | `TRUE`                           | 1.0                                                              |
//! | `FALSE` / `NULL`                 | 0.0                                                              |
//! | `col = literal`                  | `(1 - null_frac) / NDV(col)`                                     |
//! | `col < v` / `col <= v`           | `clamp((v - min)/(max - min), 0, 1) · (1 - null_frac)`          |
//! | `col > v` / `col >= v`           | `clamp((max - v)/(max - min), 0, 1) · (1 - null_frac)`          |
//! | `p1 AND p2 AND ...`              | 0 if same-col ranges contradict; else `Π sel(p_i)`              |
//! | `p1 OR  p2 OR  ...`             | `(1 - null_frac)` if same-col ranges cover domain; else `1-Π(1-sel)` |
//! | anything else                    | `FALLBACK_PREDICATE_SELECTIVITY` (0.1)                           |
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
    Column, DataType, IRContext, Scalar, ScalarValue, operator::join::JoinType,
    properties::Cardinality, scalar::*, statistics::ColumnStatistics,
};

use super::{AdvancedCardinalityEstimator, column_stats, extract_hll, ndv, null_fraction};

/// Compute filter selectivity for a predicate tree.
///
/// Returns a factor in `[0.0, 1.0]`. The caller multiplies by input cardinality
/// to get the estimated output cardinality: `|σ_p(R)| = sel(p) · |R|`.
///
/// Null fractions and NDV fallbacks are derived from each column's own
/// `TableStatistics.row_count` (looked up via [`column_stats`]), not from
/// the filtered input cardinality.
pub fn filter_selectivity(predicate: &Scalar, ctx: &IRContext) -> f64 {
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
            if let Some(sel) = equality_selectivity_col_lit(lhs, rhs, ctx) {
                return sel;
            }
            // Try literal = col (commuted)
            if let Some(sel) = equality_selectivity_col_lit(rhs, lhs, ctx) {
                return sel;
            }
            AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY
        }

        // Range: col {<, <=, >, >=} literal  or  literal {<, <=, >, >=} col
        //
        // Uses continuous uniform distribution over [min, max] from column
        // statistics. See `range_selectivity_col_lit` for formula and assumptions.
        ScalarKind::BinaryOp(meta)
            if matches!(
                meta.op_kind,
                BinaryOpKind::Lt | BinaryOpKind::Le | BinaryOpKind::Gt | BinaryOpKind::Ge
            ) =>
        {
            let binop = BinaryOp::borrow_raw_parts(meta, &predicate.common);
            let lhs = binop.lhs();
            let rhs = binop.rhs();

            // Try col op literal
            if let Some(sel) = range_selectivity_col_lit(lhs, rhs, meta.op_kind, ctx) {
                return sel;
            }
            // Try literal op col → flip operator direction
            let flipped = flip_range_op(meta.op_kind);
            if let Some(sel) = range_selectivity_col_lit(rhs, lhs, flipped, ctx) {
                return sel;
            }
            AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY
        }

        // N-ary AND: sel(p1 AND p2 AND ...) = Π sel(p_i)
        //
        // Before applying the independence assumption, we check for
        // **same-column range contradictions**: if all range predicates on a
        // single column define an empty interval (e.g. `col > 50 AND col < 30`),
        // the conjunction is unsatisfiable → selectivity 0.
        //
        // # Algorithm (contradiction detection)
        //
        // 1. For each AND term, extract range bounds via `extract_range_bound`.
        // 2. Per column, track a running interval [lo, hi], initialized to
        //    (-∞, +∞).
        // 3. Tighten: `col < v` / `col <= v` → `hi = min(hi, v)`;
        //    `col > v` / `col >= v` → `lo = max(lo, v)`.
        // 4. If for ANY column `lo >= hi`, the range is empty → return 0.0.
        //
        // Complexity: O(n) where n = number of AND terms; single linear scan.
        //
        // Assumption: only detects contradictions from range predicates
        // (Lt/Le/Gt/Ge) on literal values. Equality contradictions
        // (e.g. `col = 5 AND col = 6`) are NOT detected here.
        //
        // When non-contradictory, falls through to the independence assumption
        // (Π sel(p_i)), which can under-estimate correlated predicates like
        // `age > 18 AND age < 25`.
        //
        // TODO(AC): Also detect equality contradictions (col = 5 AND col = 6 → 0).
        // TODO(AC): When non-contradictory, use the tightened range to compute a
        //   more accurate selectivity for the conjunction instead of independence
        //   assumption. E.g. `col > 10 AND col < 50` with domain [0, 100] should
        //   give sel = 0.4, not 0.5 * 0.5 = 0.25.
        // TODO(AC): Use histogram bucket boundaries for more accurate conjunction
        //   selectivity on skewed distributions.
        ScalarKind::NaryOp(meta) if meta.op_kind == NaryOpKind::And => {
            let nary = NaryOp::borrow_raw_parts(meta, &predicate.common);
            let terms = nary.terms();

            // Contradiction check: accumulate range bounds per column.
            if and_ranges_contradict(terms) {
                return 0.0;
            }

            // Fall through to independence assumption.
            terms.iter().map(|t| filter_selectivity(t, ctx)).product()
        }

        // N-ary OR: sel(p1 OR p2 OR ...) = 1 - Π (1 - sel(p_i))
        //
        // Before applying the independence assumption, we check for
        // **same-column range tautologies**: if all OR terms are range
        // predicates on the same column and their union covers the entire
        // domain [min, max], the disjunction is a tautology for non-null
        // values → selectivity ≈ (1 - null_frac).
        //
        // # Algorithm (tautology detection)
        //
        // 1. Extract range bounds from all OR terms.
        // 2. All terms must reference the same column; otherwise skip.
        // 3. Collect the implied intervals, sort by lower bound, and merge
        //    overlapping intervals.
        // 4. If the merged result covers [min, max], return (1 - null_frac).
        //
        // Complexity: O(n log n) due to sort; for typical queries (2-5 OR
        // terms) this is negligible.
        //
        // Assumption: all terms must be on the same column. Mixed-column ORs
        // fall through to the independence assumption (correct behavior).
        //
        // TODO(AC): For partial coverage (union covers fraction f of [min, max]),
        //   return f * (1 - null_frac) instead of falling through to independence.
        //   This would give exact results under uniform distribution.
        ScalarKind::NaryOp(meta) if meta.op_kind == NaryOpKind::Or => {
            let nary = NaryOp::borrow_raw_parts(meta, &predicate.common);
            let terms = nary.terms();

            // Tautology check: see if OR covers entire domain.
            if let Some(sel) = or_ranges_tautology(terms, ctx) {
                return sel;
            }

            // Fall through to independence assumption.
            let product_complement: f64 = terms
                .iter()
                .map(|t| 1.0 - filter_selectivity(t, ctx))
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
/// - `null_fraction = null_count / table_row_count` (0 if unknown)
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
) -> Option<f64> {
    let ScalarKind::ColumnRef(col_meta) = &col_expr.kind else {
        return None;
    };

    let (stats, offset) = column_stats(ctx, col_meta.column)?;
    let col_stats = stats.column_statistics.get(offset)?;
    let table_row_count = stats.row_count;
    let distinct = ndv(col_stats).unwrap_or(table_row_count);

    let nf = null_fraction(col_stats, table_row_count);
    Some((1.0 - nf) / distinct as f64)
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
/// # Join-type adjustment
///
/// The base inner-join estimate is adjusted for the requested join type:
/// - **Inner**: no adjustment.
/// - **Left**: `max(result, |left|)` — every left row appears at least once.
/// - **Mark(col)**: `|left|` — adds a boolean column, no row change.
/// - **Single**: `min(result, |left|)` — at-most-one match per left row.
///
/// # Domain-overlap optimization
///
/// Before computing selectivity for a key pair, we check whether the `[min, max]`
/// ranges overlap. If they are provably disjoint (e.g. `[0, 99]` vs `[200, 299]`),
/// the join produces zero rows and we short-circuit immediately.
///
/// # Assumptions
///
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
// TODO(AC/Yuchen): Make `describe_table` return `Arc<TableStatistics>` -- the
// per-key-pair `column_stats` calls will become cheap (Arc clone instead of
// deep clone of `Vec<ColumnStatistics>`). Until then, composite keys on the
// same table pay repeated deep-clone costs.
pub fn equijoin_cardinality(
    keys: &[(Column, Column)],
    non_equi_conds: &Scalar,
    join_type: &JoinType,
    build_card: Cardinality,
    probe_card: Cardinality,
    ctx: &IRContext,
) -> Cardinality {
    let mut selectivity: f64 = 1.0;
    let mut any_key_had_stats = false;

    for (build_col, probe_col) in keys {
        match (
            &column_stats(ctx, *build_col),
            &column_stats(ctx, *probe_col),
        ) {
            (Some((build_ts, build_off)), Some((probe_ts, probe_off))) => {
                let Some(build_cs) = build_ts.column_statistics.get(*build_off) else {
                    selectivity *= AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
                    continue;
                };
                let Some(probe_cs) = probe_ts.column_statistics.get(*probe_off) else {
                    selectivity *= AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
                    continue;
                };

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
                        // Sample Code:
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
    let inner_card = build_card.as_f64() * probe_card.as_f64() * selectivity;
    let non_equi_sel = filter_selectivity(non_equi_conds, ctx);
    let inner_result = inner_card * non_equi_sel;

    // Adjust for join type.
    let left = build_card.as_f64();
    let adjusted = match *join_type {
        JoinType::Inner => inner_result,
        JoinType::Left => inner_result.max(left),
        JoinType::Mark(_) => left,
        JoinType::Single => inner_result.min(left),
    };

    Cardinality::new(adjusted)
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

/// Parse a column statistic's min/max string value to f64, respecting the
/// column's [`DataType`].
///
/// # Supported types
///
/// - Integer types (Int8..Int64, UInt8..UInt64): parsed as integer, cast to f64.
/// - Float16/Float32/Float64: parsed as f64 directly.
/// - Decimal128/Decimal256: parsed as f64 (string is already in decimal form).
/// - Date32: parsed as i32 (days since epoch), cast to f64.
/// - Date64: parsed as i64 (milliseconds since epoch), cast to f64.
///
/// TODO(AC/Yuchen): What to do?
/// # Unsupported types (returns `None`)
///
/// Boolean, Utf8, Binary, and any other non-numeric type.
///
/// # Precision
///
/// Casting i64/u64 to f64 can lose precision beyond 2^53 (~9 × 10^15).
/// For selectivity estimation (inherently approximate), this is negligible.
fn parse_stat_value(value: &str, data_type: &DataType) -> Option<f64> {
    match data_type {
        DataType::Int8 => value.parse::<i8>().ok().map(|v| v as f64),
        DataType::Int16 => value.parse::<i16>().ok().map(|v| v as f64),
        DataType::Int32 => value.parse::<i32>().ok().map(|v| v as f64),
        DataType::Int64 => value.parse::<i64>().ok().map(|v| v as f64),
        DataType::UInt8 => value.parse::<u8>().ok().map(|v| v as f64),
        DataType::UInt16 => value.parse::<u16>().ok().map(|v| v as f64),
        DataType::UInt32 => value.parse::<u32>().ok().map(|v| v as f64),
        DataType::UInt64 => value.parse::<u64>().ok().map(|v| v as f64),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => value.parse::<f64>().ok(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            // String representation is already in decimal form (e.g. "123.45").
            value.parse::<f64>().ok()
        }
        DataType::Date32 => {
            // Days since epoch, stored as integer string.
            value.parse::<i32>().ok().map(|v| v as f64)
        }
        DataType::Date64 => {
            // Milliseconds since epoch, stored as integer string.
            value.parse::<i64>().ok().map(|v| v as f64)
        }
        _ => None,
    }
}

/// Convert a [`ScalarValue`] to f64 for use in range selectivity calculations.
///
/// # Supported types
///
/// Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Decimal32/64/128.
///
/// # Unsupported types (returns `None`)
///
/// Boolean, Utf8, Utf8View, Date32, Date64, and any null variant.
///
/// # Precision
///
/// Casting i64/u64 to f64 can lose precision beyond 2^53 (~9 × 10^15).
/// For selectivity estimation (inherently approximate), this is negligible.
/// Decimal types are converted via `value as f64 / 10^scale`.
///
/// TODO: Support Date32/Date64 by converting to days-since-epoch f64,
/// enabling range queries on date columns.
fn scalar_value_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt16(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        ScalarValue::Decimal32(Some(v), _, scale) => Some(*v as f64 / 10f64.powi(*scale as i32)),
        ScalarValue::Decimal64(Some(v), _, scale) => Some(*v as f64 / 10f64.powi(*scale as i32)),
        ScalarValue::Decimal128(Some(v), _, scale) => Some(*v as f64 / 10f64.powi(*scale as i32)),
        _ => None, // Null variants, Boolean, Utf8, Date types
    }
}

/// Flip a comparison operator for commuted operands.
///
/// When we encounter `literal < col`, we rewrite it as `col > literal`.
/// This allows [`range_selectivity_col_lit`] to always expect the column
/// on the left-hand side.
fn flip_range_op(op: BinaryOpKind) -> BinaryOpKind {
    match op {
        BinaryOpKind::Lt => BinaryOpKind::Gt,
        BinaryOpKind::Le => BinaryOpKind::Ge,
        BinaryOpKind::Gt => BinaryOpKind::Lt,
        BinaryOpKind::Ge => BinaryOpKind::Le,
        other => other, // Eq and arithmetic ops are symmetric or N/A
    }
}

/// Compute selectivity for a range predicate `col op literal` where
/// op ∈ {<, <=, >, >=}.
///
/// # Formula (continuous uniform distribution approximation)
///
/// ```text
/// sel(col < v)  = clamp((v - min) / (max - min), 0, 1) · (1 - null_frac)
/// sel(col <= v) = clamp((v - min) / (max - min), 0, 1) · (1 - null_frac)
/// sel(col > v)  = clamp((max - v) / (max - min), 0, 1) · (1 - null_frac)
/// sel(col >= v) = clamp((max - v) / (max - min), 0, 1) · (1 - null_frac)
/// ```
///
/// # Assumptions
///
/// - **Continuous uniform distribution:** Values are spread uniformly over
///   `[min, max]`. This is the standard textbook assumption (Selinger et al.,
///   1979). Under this model, `<=` and `<` produce the same selectivity since
///   the probability of hitting any single point in a continuous distribution
///   is 0. For discrete integer domains, the true difference is
///   `1/(max - min + 1)`, which is negligible for domains of size > 100.
///
/// - **min/max are tight bounds:** We assume `min_value` and `max_value` from
///   column statistics are the actual minimum and maximum in the column. Stale
///   statistics (e.g. after inserts) may cause over- or under-estimation.
///
/// - **Literal value may be outside `[min, max]`:** Clamping handles this:
///   `col < value` where `value < min` yields 0; `col < value` where
///   `value > max` yields `(1 - null_frac)`.
///
/// - **Null values never satisfy range predicates:** SQL semantics:
///   comparisons with NULL yield NULL/false, so we exclude the null fraction.
///
/// # Fallback
///
/// Returns `None` when:
/// - `col_expr` is not a `ColumnRef`
/// - Column statistics are unavailable
/// - `min_value` or `max_value` are absent or cannot be parsed for the column's type
/// - `min == max` (zero-width domain; degenerate case)
/// - `lit_expr` is not a `Literal` or its value is non-numeric
///
/// TODO: For discrete integer columns, use the more precise formula:
///   `sel(col < v) = floor(v - min) / (max - min + 1)`. This requires
///   checking `ColumnStatistics.column_type` for integrality.
///
/// TODO: When histograms are available, use histogram bucket boundaries for
///   much more accurate selectivity on skewed distributions. The uniform
///   assumption can be 10-100× off for highly skewed data (e.g. Zipfian).
fn range_selectivity_col_lit(
    col_expr: &Scalar,
    lit_expr: &Scalar,
    op: BinaryOpKind,
    ctx: &IRContext,
) -> Option<f64> {
    let ScalarKind::ColumnRef(col_meta) = &col_expr.kind else {
        return None;
    };

    let (stats, offset) = column_stats(ctx, col_meta.column)?;
    let col_stats = stats.column_statistics.get(offset)?;
    let table_row_count = stats.row_count;

    // Parse min/max using the column's DataType.
    let min = parse_stat_value(col_stats.min_value.as_ref()?, &col_stats.column_type)?;
    let max = parse_stat_value(col_stats.max_value.as_ref()?, &col_stats.column_type)?;
    let domain = max - min;
    if domain <= 0.0 {
        // Zero-width or inverted domain; degenerate case — fall back.
        return None;
    }

    // Extract literal value as f64.
    let ScalarKind::Literal(lit_meta) = &lit_expr.kind else {
        return None;
    };
    let lit = Literal::borrow_raw_parts(lit_meta, &lit_expr.common);
    let value = scalar_value_to_f64(lit.value())?;

    let nf = null_fraction(col_stats, table_row_count);

    // Compute range fraction under continuous uniform distribution.
    let fraction = match op {
        BinaryOpKind::Lt | BinaryOpKind::Le => (value - min) / domain,
        BinaryOpKind::Gt | BinaryOpKind::Ge => (max - value) / domain,
        _ => return None, // Not a range op
    };

    Some(fraction.clamp(0.0, 1.0) * (1.0 - nf))
}

/// Attempt to extract a range bound from a scalar predicate.
///
/// Returns `Some((column, op, value))` if the predicate is of the form
/// `col op literal` or `literal op col` (with op flipped appropriately),
/// where op ∈ {<, <=, >, >=}.
///
/// Returns `None` for non-range predicates (equality, non-literal RHS,
/// complex expressions, etc.).
///
/// Time complexity: O(1) per predicate.
fn extract_range_bound(term: &Scalar) -> Option<(Column, BinaryOpKind, f64)> {
    let ScalarKind::BinaryOp(meta) = &term.kind else {
        return None;
    };
    if !matches!(
        meta.op_kind,
        BinaryOpKind::Lt | BinaryOpKind::Le | BinaryOpKind::Gt | BinaryOpKind::Ge
    ) {
        return None;
    }

    let binop = BinaryOp::borrow_raw_parts(meta, &term.common);
    let lhs = binop.lhs();
    let rhs = binop.rhs();

    // Try col op literal
    if let ScalarKind::ColumnRef(col_meta) = &lhs.kind {
        if let ScalarKind::Literal(lit_meta) = &rhs.kind {
            let lit = Literal::borrow_raw_parts(lit_meta, &rhs.common);
            if let Some(v) = scalar_value_to_f64(lit.value()) {
                return Some((col_meta.column, meta.op_kind, v));
            }
        }
    }

    // Try literal op col → flip operator
    if let ScalarKind::Literal(lit_meta) = &lhs.kind {
        if let ScalarKind::ColumnRef(col_meta) = &rhs.kind {
            let lit = Literal::borrow_raw_parts(lit_meta, &lhs.common);
            if let Some(v) = scalar_value_to_f64(lit.value()) {
                return Some((col_meta.column, flip_range_op(meta.op_kind), v));
            }
        }
    }

    None
}

/// Check whether AND-ed range predicates on the same column contradict.
///
/// Scans the terms for range bounds (via [`extract_range_bound`]) and
/// accumulates a running interval `[lo, hi]` per column. If for any column
/// the lower bound meets or exceeds the upper bound, the conjunction is
/// unsatisfiable.
///
/// Returns `true` (contradiction) only when at least one column has an empty
/// range. Non-range terms (equality, complex expressions) are ignored — they
/// do not prevent the independence-assumption fallback.
///
/// # Complexity
///
/// O(n) where n = number of AND terms. Uses a `HashMap<Column, (f64, f64)>`
/// with expected O(1) per lookup. Typical queries have 2-5 AND terms and
/// 1-2 distinct columns, so the map is trivially small.
fn and_ranges_contradict(terms: &[std::sync::Arc<Scalar>]) -> bool {
    use std::collections::HashMap;

    // Per-column running interval: (lo, hi).
    // Initialized lazily on first range bound for each column.
    // TODO(perf): Replace the `HashMap<Column, (f64, f64)>` allocation with a
    // `SmallVec` or sorted `Vec<(Column, f64, f64)>` for the common case (<=4
    // columns). During Cascades optimization the same predicate may be evaluated
    // many times.
    let mut intervals: HashMap<Column, (f64, f64)> = HashMap::new();

    for term in terms {
        if let Some((col, op, value)) = extract_range_bound(term) {
            let entry = intervals
                .entry(col)
                .or_insert((f64::NEG_INFINITY, f64::INFINITY));
            match op {
                BinaryOpKind::Lt | BinaryOpKind::Le => {
                    // col < v / col <= v → upper bound tightens
                    entry.1 = entry.1.min(value);
                }
                BinaryOpKind::Gt | BinaryOpKind::Ge => {
                    // col > v / col >= v → lower bound tightens
                    entry.0 = entry.0.max(value);
                }
                _ => {}
            }
        }
    }

    // Check for empty intervals.
    intervals.values().any(|(lo, hi)| lo >= hi)
}

/// Check whether OR-ed range predicates on a single column form a tautology.
///
/// If all OR terms are range predicates on the **same** column, and their
/// union covers the column's entire domain `[min, max]`, returns the
/// tautology selectivity `(1 - null_frac)`.
///
/// # Algorithm
///
/// 1. Extract range bounds from all terms. If any term is not a range
///    predicate, or terms reference different columns, return `None`.
/// 2. Convert each range bound to an interval on the number line:
///    - `col < v` / `col <= v` → `[min, v]`
///    - `col > v` / `col >= v` → `[v, max]`
///    where `min` and `max` come from column statistics.
/// 3. Sort intervals by lower bound and merge overlapping ones in-place.
/// 4. If the single merged interval covers `[min, max]`, it's a tautology.
///
/// # Complexity
///
/// O(n log n) due to the sort step, where n = number of OR terms.
/// For typical queries (2-5 OR terms), this is negligible.
///
/// # Assumptions
///
/// - All terms must reference the same column. Mixed-column ORs return `None`.
/// - Only handles range predicates (Lt/Le/Gt/Ge). Equality or complex terms
///   cause early return `None`.
/// - Uses the same continuous uniform assumption as range selectivity.
///
/// TODO(AC): For partial coverage (union covers fraction f of [min, max]),
///   return f * (1 - null_frac) instead of returning `None`.
fn or_ranges_tautology(terms: &[std::sync::Arc<Scalar>], ctx: &IRContext) -> Option<f64> {
    if terms.is_empty() {
        return None;
    }

    // Extract bounds; bail if any term isn't a range predicate.
    let mut bounds = Vec::with_capacity(terms.len());
    for term in terms {
        bounds.push(extract_range_bound(term)?);
    }

    // All bounds must reference the same column.
    let target_col = bounds[0].0;
    if !bounds.iter().all(|(col, _, _)| *col == target_col) {
        return None;
    }

    // Look up column stats for min/max.
    let (stats, offset) = column_stats(ctx, target_col)?;
    let col_stats = stats.column_statistics.get(offset)?;
    let table_row_count = stats.row_count;
    let min = parse_stat_value(col_stats.min_value.as_ref()?, &col_stats.column_type)?;
    let max = parse_stat_value(col_stats.max_value.as_ref()?, &col_stats.column_type)?;
    if min >= max {
        return None;
    }

    // Convert each bound to an interval [lo, hi], then sort and merge in-place.
    let mut intervals: Vec<(f64, f64)> = bounds
        .iter()
        .map(|(_, op, v)| match op {
            BinaryOpKind::Lt | BinaryOpKind::Le => (min, *v),
            BinaryOpKind::Gt | BinaryOpKind::Ge => (*v, max),
            _ => (min, min), // shouldn't happen; extract_range_bound filters
        })
        .collect();

    intervals.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    // Merge overlapping intervals in-place.
    let mut write = 0;
    for read in 1..intervals.len() {
        if intervals[read].0 <= intervals[write].1 {
            // Overlapping or adjacent — extend.
            intervals[write].1 = intervals[write].1.max(intervals[read].1);
        } else {
            write += 1;
            intervals[write] = intervals[read];
        }
    }
    let merged_count = write + 1;

    // Check if the single merged interval covers [min, max].
    if merged_count == 1 && intervals[0].0 <= min && intervals[0].1 >= max {
        // Tautology for non-null values.
        let nf = null_fraction(col_stats, table_row_count);
        return Some(1.0 - nf);
    }

    None
}

/// Check if two columns have overlapping value domains using min/max statistics.
///
/// Two numeric ranges `[a_lo, a_hi]` and `[b_lo, b_hi]` overlap iff
/// `NOT (a_hi < b_lo OR b_hi < a_lo)`.
///
/// Returns `true` (assumes overlap) when min/max are unavailable or non-numeric,
/// since we cannot prove disjointness.
fn domains_overlap(a: &ColumnStatistics, b: &ColumnStatistics) -> bool {
    let a_min = match a
        .min_value
        .as_ref()
        .and_then(|v| parse_stat_value(v, &a.column_type))
    {
        Some(v) => v,
        None => return true, // Can't determine, assume overlap
    };
    let a_max = match a
        .max_value
        .as_ref()
        .and_then(|v| parse_stat_value(v, &a.column_type))
    {
        Some(v) => v,
        None => return true,
    };
    let b_min = match b
        .min_value
        .as_ref()
        .and_then(|v| parse_stat_value(v, &b.column_type))
    {
        Some(v) => v,
        None => return true,
    };
    let b_max = match b
        .max_value
        .as_ref()
        .and_then(|v| parse_stat_value(v, &b.column_type))
    {
        Some(v) => v,
        None => return true,
    };

    // Ranges overlap if NOT (a_hi < b_lo OR b_hi < a_lo)
    !(a_max < b_min || b_max < a_min)
}
