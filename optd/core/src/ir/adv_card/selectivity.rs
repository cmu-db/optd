//! Selectivity estimation helpers for filters and equi-joins.
//!
//! # Shared primitive: [`ColumnConstraintGraph`]
//!
//! Both filter contradiction detection and join-key de-duplication share the
//! same underlying pattern: a set of `(Column, Column)` equality pairs builds
//! equivalence classes (Union-Find), and constraints on any column in a class
//! propagate to the class representative.  [`ColumnConstraintGraph`] encapsulates
//! this: [`ColumnConstraintGraph::add_equality`] returns `true` only when the
//! two columns were previously in different components (spanning-tree edge),
//! which drives Kruskal's algorithm in the join path.
//!
//! # Filter selectivity ([`filter_selectivity`])
//!
//! Recursively decomposes a predicate scalar tree and computes a selectivity
//! factor in `[0.0, 1.0]`.
//!
//! | Predicate shape                       | Formula / Result                                                       |
//! |---------------------------------------|------------------------------------------------------------------------|
//! | `TRUE`                                | 1.0                                                                    |
//! | `FALSE` / `NULL`                      | 0.0                                                                    |
//! | `col = literal`                       | `(1 - null_frac) / NDV(col)`                                           |
//! | `col < v` / `col <= v`               | `clamp((v - min)/(max - min), 0, 1) · (1 - null_frac)`                |
//! | `col > v` / `col >= v`               | `clamp((max - v)/(max - min), 0, 1) · (1 - null_frac)`                |
//! | `p1 AND p2 AND ...`                   | 0 if ranges/literals contradict (see below); else `Π sel(p_i)`        |
//! | `p1 OR  p2 OR  ...`                  | `(1 - null_frac)` if same-col ranges cover domain; else `1-Π(1-sel)`  |
//! | anything else                         | `FALLBACK_PREDICATE_SELECTIVITY` (0.1)                                 |
//!
//! ## AND contradiction detection
//!
//! Before applying the independence assumption, [`and_ranges_contradict`] runs
//! a two-pass scan over the AND terms:
//!
//! 1. **Pass 1 — equivalence classes**: extract `col_a = col_b` pairs and
//!    `col = literal` constraints; build a [`ColumnConstraintGraph`].
//! 2. **Pass 2 — range propagation**: extract range bounds (`col op v`) and
//!    accumulate them per equivalence-class representative.
//!
//! Contradiction if any representative's accumulated `[lo, hi]` is empty
//! (`lo ≥ hi`), or if two different literals are assigned to the same class.
//!
//! This detects both same-column contradictions (`col > 50 AND col < 30`) and
//! cross-column contradictions propagated through equalities:
//!
//! ```text
//! A = B AND A > 50 AND B < 30  →  class {A,B}: lo=50, hi=30  →  contradiction → 0
//! A = 5 AND B = 3 AND A = B    →  class {A,B}: literals 5 ≠ 3 → contradiction → 0
//! ```
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
//! ## MST-based de-duplication (Kruskal's algorithm)
//!
//! Key pairs can form cycles in the column equality graph (e.g. `A=B, B=C, A=C`
//! — a triangle). Multiplying all three selectivities over-constrains the
//! estimate: only two independent constraints are needed for three columns.
//!
//! [`equijoin_cardinality`] uses **Kruskal's algorithm** to select a minimum
//! spanning forest:
//!
//! 1. Compute selectivity for every key pair (Phase A).
//! 2. Check domain overlap for **all** key pairs — even those that will be
//!    skipped for selectivity still represent real join conditions: if their
//!    `[min, max]` ranges are disjoint the join returns 0.
//! 3. Sort edges ascending by selectivity (most constraining first).
//! 4. Use [`ColumnConstraintGraph`] to include an edge only when it connects
//!    two previously-separate components; skip cycle-forming edges (Phase B).
//!
//! ```text
//! Triangle A=B, B=C, A=C (all sel = 1/N):
//!   naïve:  sel = (1/N)³          ← over-constrained
//!   MST:    sel = (1/N)²          ← correct: 2 independent constraints
//! ```

use std::collections::HashMap;

use crate::ir::{
    Column, DataType, IRContext, Scalar, ScalarValue, operator::join::JoinType,
    properties::{
        Cardinality, GroupPredicate, PredicateSummary, RangeConstraint, ValueRef,
        date_days_to_year, date_millis_to_year, derive_value_ref,
    },
    scalar::*, statistics::ColumnStatistics,
};
use crate::utility::union_find::UnionFind;

use super::{AdvancedCardinalityEstimator, column_stats, extract_hll, ndv, null_fraction};

// ---------------------------------------------------------------------------
// Shared primitive: column constraint graph
// ---------------------------------------------------------------------------

/// Tracks column equivalence classes and per-class range constraints.
///
/// Both join-key MST and filter contradiction detection use the same underlying
/// pattern: a set of `(Column, Column)` equality pairs defines equivalence
/// classes (via Union-Find), and range constraints on any column in a class
/// propagate to the class representative.
///
/// # Usage
///
/// - **Join (Kruskal's MST):** call [`add_equality`] in ascending-selectivity
///   order; include a key pair's selectivity in the product only when
///   [`add_equality`] returns `true` (spanning-tree edge).
/// - **Filter (contradiction detection):** call [`add_equality`] for every
///   `col = col` term (ignore return value — want full classes, not just
///   spanning tree), then call [`add_range`] for every range bound, then check
///   [`has_range_contradiction`].
///
/// [`add_equality`]: ColumnConstraintGraph::add_equality
/// [`add_range`]: ColumnConstraintGraph::add_range
/// [`has_range_contradiction`]: ColumnConstraintGraph::has_range_contradiction
struct ColumnConstraintGraph {
    uf: UnionFind<Column>,
    /// Running interval `[lo, hi]` per equivalence-class representative.
    ///
    /// Initialized lazily: the first range bound for a representative inserts
    /// `(-∞, +∞)` and immediately tightens it.
    intervals: HashMap<Column, (f64, f64)>,
    /// Literal equality value seen per representative (`col = constant`).
    ///
    /// Used to detect the contradiction `col = 5 AND col = 3` (two different
    /// literals for the same equivalence class).
    literal_eq: HashMap<Column, f64>,
}

impl ColumnConstraintGraph {
    fn new() -> Self {
        Self {
            uf: UnionFind::default(),
            intervals: HashMap::new(),
            literal_eq: HashMap::new(),
        }
    }

    /// Add a column-equality constraint `a = b`.
    ///
    /// Returns `true` if `a` and `b` were previously in **different** components
    /// (i.e. this edge is a spanning-tree / Kruskal's edge).
    /// Returns `false` if they were already in the same component (cycle — edge
    /// is transitively implied and should be skipped for selectivity).
    fn add_equality(&mut self, a: Column, b: Column) -> bool {
        let root_a = self.uf.find(&a);
        let root_b = self.uf.find(&b);
        if root_a == root_b {
            return false; // already same component
        }
        // Merge: root_a becomes the representative of the combined class.
        self.uf.merge(&a, &b);
        let new_root = self.uf.find(&a); // always root_a after merge(&a, &b)

        // Merge intervals: if both classes had intervals, intersect them.
        let interval_b = self.intervals.remove(&root_b);
        if let Some((lo_b, hi_b)) = interval_b {
            let entry = self
                .intervals
                .entry(new_root)
                .or_insert((f64::NEG_INFINITY, f64::INFINITY));
            entry.0 = entry.0.max(lo_b);
            entry.1 = entry.1.min(hi_b);
        }

        // Merge literal equalities: if both classes had a literal, check consistency.
        let lit_b = self.literal_eq.remove(&root_b);
        if let Some(v_b) = lit_b {
            match self.literal_eq.get(&new_root) {
                Some(&v_a) => {
                    // Two different literals on the same class → mark contradiction
                    // by collapsing the interval to empty.
                    if (v_a - v_b).abs() > f64::EPSILON {
                        let entry = self
                            .intervals
                            .entry(new_root)
                            .or_insert((f64::NEG_INFINITY, f64::INFINITY));
                        // Set lo > hi to signal contradiction.
                        entry.0 = 1.0;
                        entry.1 = 0.0;
                    }
                }
                None => {
                    self.literal_eq.insert(new_root, v_b);
                }
            }
        }

        true
    }

    /// Add a range constraint `col op value` where op ∈ {<, <=, >, >=}.
    ///
    /// Internally resolves `col` to its equivalence-class representative and
    /// tightens the representative's `[lo, hi]` interval.
    fn add_range(&mut self, col: Column, op: BinaryOpKind, value: f64) {
        let rep = self.uf.find(&col);
        let entry = self
            .intervals
            .entry(rep)
            .or_insert((f64::NEG_INFINITY, f64::INFINITY));
        match op {
            BinaryOpKind::Lt | BinaryOpKind::Le => entry.1 = entry.1.min(value),
            BinaryOpKind::Gt | BinaryOpKind::Ge => entry.0 = entry.0.max(value),
            _ => {}
        }
    }

    /// Add a literal-equality constraint `col = value`.
    ///
    /// Records the literal for the representative. If a different literal was
    /// already recorded for the same representative, marks the interval as empty
    /// (contradiction).
    fn add_literal_eq(&mut self, col: Column, value: f64) {
        let rep = self.uf.find(&col);
        match self.literal_eq.get(&rep) {
            Some(&existing) => {
                if (existing - value).abs() > f64::EPSILON {
                    // Contradiction: col = v1 AND col = v2, v1 ≠ v2.
                    let entry = self
                        .intervals
                        .entry(rep)
                        .or_insert((f64::NEG_INFINITY, f64::INFINITY));
                    entry.0 = 1.0;
                    entry.1 = 0.0;
                }
            }
            None => {
                self.literal_eq.insert(rep, value);
            }
        }
    }

    /// Returns `true` if any equivalence class has an empty interval (`lo >= hi`),
    /// indicating an unsatisfiable conjunction of constraints.
    fn has_range_contradiction(&self) -> bool {
        self.intervals.values().any(|(lo, hi)| lo >= hi)
    }
}

// ---------------------------------------------------------------------------
// Column-equality extraction from scalar predicates
// ---------------------------------------------------------------------------

/// Attempt to extract a `col_a = col_b` column-equality from a scalar term.
///
/// Returns `Some((col_a, col_b))` iff the term is a `BinaryOp(Eq)` with both
/// operands being `ColumnRef` nodes. Returns `None` for `col = literal`,
/// `literal = col`, or any other shape.
///
/// This is used by the filter AND contradiction detector to build equivalence
/// classes from within-predicate column equalities, enabling cross-column range
/// propagation (e.g. `A = B AND A > 50 AND B < 30` → contradiction).
fn extract_column_equality(term: &Scalar) -> Option<(Column, Column)> {
    let ScalarKind::BinaryOp(meta) = &term.kind else {
        return None;
    };
    if meta.op_kind != BinaryOpKind::Eq {
        return None;
    }
    let binop = BinaryOp::borrow_raw_parts(meta, &term.common);
    match (&binop.lhs().kind, &binop.rhs().kind) {
        (ScalarKind::ColumnRef(a), ScalarKind::ColumnRef(b)) => Some((a.column, b.column)),
        _ => None,
    }
}

/// Attempt to extract a `col = literal_value` literal-equality from a scalar
/// term, returning `Some((column, f64_value))`.
///
/// Returns `None` if the term is not an equality, if either side is not a
/// column or literal, or if the literal cannot be converted to f64.
fn extract_literal_equality(term: &Scalar) -> Option<(Column, f64)> {
    let ScalarKind::BinaryOp(meta) = &term.kind else {
        return None;
    };
    if meta.op_kind != BinaryOpKind::Eq {
        return None;
    }
    let binop = BinaryOp::borrow_raw_parts(meta, &term.common);
    let lhs = binop.lhs();
    let rhs = binop.rhs();

    // col = literal
    if let (ScalarKind::ColumnRef(col_meta), ScalarKind::Literal(lit_meta)) = (&lhs.kind, &rhs.kind)
    {
        let lit = Literal::borrow_raw_parts(lit_meta, &rhs.common);
        if let Some(v) = scalar_value_to_f64(lit.value()) {
            return Some((col_meta.column, v));
        }
    }
    // literal = col
    if let (ScalarKind::Literal(lit_meta), ScalarKind::ColumnRef(col_meta)) = (&lhs.kind, &rhs.kind)
    {
        let lit = Literal::borrow_raw_parts(lit_meta, &lhs.common);
        if let Some(v) = scalar_value_to_f64(lit.value()) {
            return Some((col_meta.column, v));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Filter selectivity
// ---------------------------------------------------------------------------

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
        // **contradictions** via [`and_ranges_contradict`], which uses a
        // two-pass algorithm:
        //
        // Pass 1 — build equivalence classes:
        //   - `col_a = col_b` terms → merge into [`ColumnConstraintGraph`].
        //   - `col = literal` terms → record per-representative literal value.
        //   - Two different literals on the same class → contradiction.
        //
        // Pass 2 — accumulate range bounds per representative:
        //   - `col op v` → `add_range(col, op, v)` normalises to representative.
        //   - If any representative's [lo, hi] is empty (lo ≥ hi) → contradiction.
        //
        // This detects both single-column contradictions (`col > 50 AND col < 30`)
        // and cross-column contradictions propagated through equalities
        // (`A = B AND A > 50 AND B < 30`).
        //
        // Complexity: O(n) with two linear scans and O(1) amortised UF/HashMap.
        //
        // When non-contradictory, falls through to the independence assumption
        // (Π sel(p_i)), which can under-estimate correlated predicates like
        // `age > 18 AND age < 25`.
        //
        // TODO(AC): When non-contradictory, use the tightened range to compute a
        //   more accurate selectivity for the conjunction instead of independence
        //   assumption. E.g. `col > 10 AND col < 50` with domain [0, 100] should
        //   give sel = 0.4, not 0.5 * 0.5 = 0.25.
        // TODO(AC): Use histogram bucket boundaries for more accurate conjunction
        //   selectivity on skewed distributions.
        ScalarKind::NaryOp(meta) if meta.op_kind == NaryOpKind::And => {
            let nary = NaryOp::borrow_raw_parts(meta, &predicate.common);
            let terms = nary.terms();

            // Contradiction check: two-pass equivalence-class + range analysis.
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

/// Incremental filter selectivity given the input's predicate summary.
///
/// Rather than multiplying the base-table selectivity of `predicate` (which
/// double-counts predicates already reflected in the input summary), this
/// folds the filter into a cloned summary and returns the ratio of
/// post-filter to pre-filter summary selectivity. A contradictory merged
/// summary short-circuits to `0.0`.
///
/// See the "Ratio-based `summary_selectivity`" design note in
/// `docs/predicate-summary-lineage.md.memo` for why this is the preferred
/// entry point over `filter_selectivity`.
pub fn filter_selectivity_with_summary(
    predicate: &Scalar,
    summary: &PredicateSummary,
    ctx: &IRContext,
) -> f64 {
    let mut merged = summary.clone();
    merged.apply_predicate(&std::sync::Arc::new(predicate.clone()), ctx);
    if merged.is_contradictory {
        return 0.0;
    }

    let before = summary_selectivity(summary, ctx);
    let after = summary_selectivity(&merged, ctx);
    if before <= 0.0 {
        return after.clamp(0.0, 1.0);
    }
    (after / before).clamp(0.0, 1.0)
}

/// NDV for a GROUP BY / aggregate key, resolved through lineage.
///
/// Resolves `key` to a `ValueRef`, canonicalizes it (so an aliased key
/// shares NDV with its source column), then returns the NDV of the
/// representative. Returns `None` when the key resolves to a `Derived`
/// value or no stats are available; callers then fall back to a magic
/// constant.
pub fn key_ndv_from_summary(
    key: &Scalar,
    summary: &PredicateSummary,
    ctx: &IRContext,
) -> Option<usize> {
    let value = derive_value_ref(key, summary, ctx)?;
    value_ndv(&summary.canonical_value(&value), ctx)
}

/// Overall selectivity implied by a summary.
///
/// Factored into two pieces:
///   - per-`table_index` group selectivity via [`group_selectivity`],
///   - cross-table residual predicates, each charged the fallback
///     `0.1` softened by `0.5^idx` so later residuals contribute less.
///
/// The `0.5^idx` decay is the "soft independence" compromise described in
/// the README: raw multiplication would underestimate correlated
/// predicates, and no compounding would overestimate truly independent
/// ones.
fn summary_selectivity(summary: &PredicateSummary, ctx: &IRContext) -> f64 {
    if summary.is_contradictory {
        return 0.0;
    }

    let group_product: f64 = summary
        .selectivity_groups
        .keys()
        .copied()
        .map(|group| group_selectivity(summary, group, ctx))
        .product();

    let residual_product: f64 = summary
        .residual_predicates
        .iter()
        .enumerate()
        .map(|(idx, _)| {
            AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY.powf(0.5_f64.powi(idx as i32))
        })
        .product();

    (group_product * residual_product).clamp(0.0, 1.0)
}

/// Selectivity contribution of a single `table_index` group.
///
/// Gathers one factor per constrained canonical value (literal equality or
/// range), plus one fallback factor per distinct `ColumnEquality` and
/// `Residual` in the group. Factors are sorted ascending (most selective
/// first) and combined with an exponential decay so the primary predicate
/// dominates and additional same-table predicates have diminishing effect.
/// This reflects the common case that predicates on the same table
/// correlate more strongly than predicates on unrelated tables.
fn group_selectivity(summary: &PredicateSummary, group: i64, ctx: &IRContext) -> f64 {
    let mut factors = Vec::new();
    let mut seen_classes = std::collections::HashSet::new();
    for value in summary.values_in_group(group) {
        let canonical = summary.canonical_value(&value);
        if !seen_classes.insert(canonical.clone()) {
            continue;
        }
        if let Some(literal) = summary.literal_equalities.get(&canonical) {
            factors.push(
                value_equality_selectivity(&canonical, *literal, ctx)
                    .unwrap_or(AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY),
            );
        } else if let Some(range) = summary.range_constraints.get(&canonical) {
            factors.push(
                value_range_selectivity(&canonical, range, ctx)
                    .unwrap_or(AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY),
            );
        }
    }

    let mut seen_equalities = std::collections::HashSet::new();
    let mut residual_count = 0usize;
    for predicate in summary.selectivity_groups.get(&group).into_iter().flatten() {
        match predicate {
            GroupPredicate::ColumnEquality(lhs, rhs) => {
                let lhs = summary.canonical_value(lhs);
                let rhs = summary.canonical_value(rhs);
                if lhs == rhs {
                    continue;
                }
                let pair = if format!("{lhs:?}") <= format!("{rhs:?}") {
                    (lhs, rhs)
                } else {
                    (rhs, lhs)
                };
                if seen_equalities.insert(pair) {
                    factors.push(AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY);
                }
            }
            GroupPredicate::Residual(_) => residual_count += 1,
            GroupPredicate::EqLiteral(_, _) | GroupPredicate::Range(_, _, _) => {}
        }
    }
    for _ in 0..residual_count {
        factors.push(AdvancedCardinalityEstimator::FALLBACK_PREDICATE_SELECTIVITY);
    }

    factors.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut exponent = 1.0;
    let mut combined = 1.0;
    for factor in factors {
        combined *= factor.powf(exponent);
        exponent *= 0.5;
    }
    combined
}

/// Equality-against-literal selectivity for a `ValueRef`.
///
/// `(1 - null_fraction) / NDV`. `ExtractYear` gets a year-domain NDV
/// derived from the base column's min/max. `Derived` has no recoverable
/// NDV and returns `None`.
fn value_equality_selectivity(value: &ValueRef, _literal: f64, ctx: &IRContext) -> Option<f64> {
    let (distinct, null_fraction) = match value {
        ValueRef::Base(column) => {
            let (stats, offset) = column_stats(ctx, *column)?;
            let col_stats = stats.column_statistics.get(offset)?;
            let distinct = ndv(col_stats).unwrap_or(stats.row_count);
            (distinct, null_fraction(col_stats, stats.row_count))
        }
        ValueRef::ExtractYear(column) => {
            let (stats, offset) = column_stats(ctx, *column)?;
            let col_stats = stats.column_statistics.get(offset)?;
            let distinct = year_ndv(col_stats, &ctx.get_column_meta(column).data_type)? as usize;
            (distinct, null_fraction(col_stats, stats.row_count))
        }
        ValueRef::Derived(_) => return None,
    };
    Some((1.0 - null_fraction) / distinct as f64)
}

/// Uniform-distribution range selectivity:
/// `clamp((upper - lower) / (max - min), 0, 1) * (1 - null_fraction)`.
///
/// Missing bounds default to the corresponding end of the value's domain.
/// Returns `None` for `Derived` values or when the domain is degenerate.
fn value_range_selectivity(
    value: &ValueRef,
    constraint: &RangeConstraint,
    ctx: &IRContext,
) -> Option<f64> {
    let (min, max, null_fraction) = value_domain(value, ctx)?;
    let domain = max - min;
    if domain <= 0.0 {
        return None;
    }
    let lower = constraint.lower.map(|(value, _)| value).unwrap_or(min);
    let upper = constraint.upper.map(|(value, _)| value).unwrap_or(max);
    if lower > upper {
        return Some(0.0);
    }
    Some(((upper - lower) / domain).clamp(0.0, 1.0) * (1.0 - null_fraction))
}

/// Return `(min, max, null_fraction)` for a `ValueRef`, reinterpreting the
/// underlying base-column statistics when the value is `ExtractYear`.
/// Returns `None` for `Derived` values.
fn value_domain(value: &ValueRef, ctx: &IRContext) -> Option<(f64, f64, f64)> {
    match value {
        ValueRef::Base(column) => {
            let (stats, offset) = column_stats(ctx, *column)?;
            let col_stats = stats.column_statistics.get(offset)?;
            let column_type = &ctx.get_column_meta(column).data_type;
            let min = parse_stat_value(col_stats.min_value.as_ref()?, column_type)?;
            let max = parse_stat_value(col_stats.max_value.as_ref()?, column_type)?;
            Some((min, max, null_fraction(col_stats, stats.row_count)))
        }
        ValueRef::ExtractYear(column) => {
            let (stats, offset) = column_stats(ctx, *column)?;
            let col_stats = stats.column_statistics.get(offset)?;
            // Reinterpret the base column's min/max as year bounds via
            // `parse_year_value`, which handles Date32 (days since epoch)
            // and Date64 (millis since epoch).
            let min = parse_year_value(col_stats.min_value.as_ref()?, &ctx.get_column_meta(column).data_type)? as f64;
            let max = parse_year_value(col_stats.max_value.as_ref()?, &ctx.get_column_meta(column).data_type)? as f64;
            Some((min, max, null_fraction(col_stats, stats.row_count)))
        }
        ValueRef::Derived(_) => None,
    }
}

/// NDV of a `ValueRef`. For `ExtractYear` we use the integer year range
/// (`max_year - min_year + 1`) because the catalog has no direct NDV for a
/// year projection; for `Base` we read the column's stored NDV / HLL.
fn value_ndv(value: &ValueRef, ctx: &IRContext) -> Option<usize> {
    match value {
        ValueRef::Base(column) => {
            let (stats, offset) = column_stats(ctx, *column)?;
            let col_stats = stats.column_statistics.get(offset)?;
            ndv(col_stats)
        }
        ValueRef::ExtractYear(column) => {
            let (stats, offset) = column_stats(ctx, *column)?;
            let col_stats = stats.column_statistics.get(offset)?;
            year_ndv(col_stats, &ctx.get_column_meta(column).data_type).map(|value| value as usize)
        }
        ValueRef::Derived(_) => None,
    }
}

/// Approximate NDV for the year projection of a date column: number of
/// distinct calendar years covered by `[min_value, max_value]`. Always
/// at least 1.
fn year_ndv(col_stats: &ColumnStatistics, data_type: &DataType) -> Option<i32> {
    let min = parse_year_value(col_stats.min_value.as_ref()?, data_type)?;
    let max = parse_year_value(col_stats.max_value.as_ref()?, data_type)?;
    Some((max - min + 1).max(1))
}

/// Parse a catalog min/max string and convert to a calendar year.
/// `Date32` values are days since the UNIX epoch; `Date64` values are
/// milliseconds since the UNIX epoch. Non-date types return `None`.
fn parse_year_value(value: &str, data_type: &DataType) -> Option<i32> {
    match data_type {
        DataType::Date32 => value.parse::<i32>().ok().and_then(date_days_to_year),
        DataType::Date64 => value.parse::<i64>().ok().and_then(date_millis_to_year),
        _ => None,
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

/// A join key pair with its pre-computed selectivity.
///
/// Collected in Phase A of [`equijoin_cardinality`] so that Phase B (Kruskal's
/// MST) can sort them and process them in order of ascending selectivity.
struct JoinEdge {
    build_col: Column,
    probe_col: Column,
    /// Estimated selectivity for this key pair.
    selectivity: f64,
    /// Whether column statistics were available for this pair (vs. pure fallback).
    had_stats: bool,
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
/// where the product is taken only over **spanning-tree edges** of the join-key
/// graph (see MST section below). For each included key pair `(k_i_R, k_i_S)`:
///
/// **Preferred (HLL intersection):** when both sides have HLL sketches:
/// ```text
/// sel(k_i) = |intersect(k_i)| / (NDV_R(k_i) · NDV_S(k_i))
/// ```
/// where `|intersect| = |A| + |B| - |A ∪ B|` via inclusion-exclusion on
/// merged HLL registers.
///
/// **Fallback (containment assumption):** when HLLs are unavailable:
/// ```text
/// sel(k_i) = 1 / max(NDV_R(k_i), NDV_S(k_i))
/// ```
///
/// # MST-based de-duplication (Kruskal's algorithm)
///
/// Multiple key pairs can form cycles (equivalence classes). Example: `A=B,
/// B=C, C=A` — only two independent constraints are needed for three columns.
/// Multiplying all three selectivities would over-constrain the estimate.
///
/// We use **Kruskal's algorithm** on the bipartite column graph:
/// - Nodes: all `Column` identifiers in `keys`.
/// - Edges: each `(build_col, probe_col)` pair, weighted by its selectivity.
/// - Sort edges ascending by selectivity (most constraining first).
/// - Use [`ColumnConstraintGraph`] (Union-Find) to include an edge only when it
///   connects two previously-separate components (spanning-tree edge).
/// - Skip edges whose endpoints are already in the same component (cycles /
///   transitively-implied conditions).
///
/// Self-join keys `(A, A)` are naturally skipped (both endpoints already in the
/// same component after the first `add_equality` call), which correctly avoids
/// applying a spurious `1/NDV` factor for a tautology.
///
/// # Domain-overlap check (all key pairs, including non-spanning-tree ones)
///
/// Even a key pair that would be skipped for selectivity still represents a real
/// join condition: if its `[min, max]` ranges are disjoint, the join must return
/// 0 rows. Domain disjointness is therefore checked in Phase A for **every** key
/// pair, before Kruskal's sorting.
///
/// Example: A=[0,99], B=[0,200], C=[100,200] with keys (A,B),(B,C),(A,C).
/// Spanning tree = {(A,B),(B,C)}; (A,C) is skipped for selectivity but its
/// domain check (A=[0,99] vs C=[100,200]) returns 0 — correctly.
///
/// # Non-equi condition handling
///
/// After computing the equi-join cardinality, we apply the selectivity of any
/// non-equi conditions (e.g. `x.b < y.b` in `x.a = y.a AND x.b < y.b`).
/// These are treated as a post-join filter using [`filter_selectivity`].
///
/// When `non_equi_conds` is a literal `TRUE`, `filter_selectivity` returns 1.0.
///
/// # Join-type adjustment
///
/// - **Inner**: no adjustment.
/// - **Left**: `max(result, |left|)` — every left row appears at least once.
/// - **Mark(col)**: `|left|` — adds a boolean column, no row change.
/// - **Single**: `min(result, |left|)` — at-most-one match per left row.
///
/// # Assumptions
///
/// - **Independent spanning-tree key pairs.** The selectivities of MST edges
///   are treated as independent: `sel = Π sel(k_i)`.
/// - **Uniform value distribution** within each column.
/// - **Independence between equi-keys and non-equi conditions.**
// TODO(AC/Yuchen): Make `describe_table` return `Arc<TableStatistics>` -- the
// per-key-pair `column_stats` calls will become cheap (Arc clone instead of
// deep clone of `Vec<ColumnStatistics>`). Until then, composite keys on the
// same table pay repeated deep-clone costs.
pub fn equijoin_cardinality(
    keys: &[(Column, Column)],
    non_equi_conds: Option<&Scalar>,
    join_type: &JoinType,
    outer_card: Cardinality,
    inner_card: Cardinality,
    outer_summary: &PredicateSummary,
    inner_summary: &PredicateSummary,
    join_summary: &PredicateSummary,
    ctx: &IRContext,
) -> Cardinality {
    // -----------------------------------------------------------------------
    // Phase A: collect per-key selectivities and domain-overlap check.
    //
    // Domain overlap is checked for ALL key pairs (not just spanning-tree ones)
    // because a non-spanning-tree edge still represents a real join condition.
    // -----------------------------------------------------------------------
    let mut edges: Vec<JoinEdge> = Vec::with_capacity(keys.len());

    for (outer_col, inner_col) in keys {
        let outer_value = resolve_join_value(*outer_col, outer_summary);
        let inner_value = resolve_join_value(*inner_col, inner_summary);
        match (
            value_column_stats(&outer_value, ctx),
            value_column_stats(&inner_value, ctx),
        ) {
            (Some((outer_ts, outer_off)), Some((inner_ts, inner_off))) => {
                let Some(build_cs) = outer_ts.column_statistics.get(outer_off) else {
                    edges.push(JoinEdge {
                        build_col: *outer_col,
                        probe_col: *inner_col,
                        selectivity: AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY,
                        had_stats: false,
                    });
                    continue;
                };
                let Some(probe_cs) = inner_ts.column_statistics.get(inner_off) else {
                    edges.push(JoinEdge {
                        build_col: *outer_col,
                        probe_col: *inner_col,
                        selectivity: AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY,
                        had_stats: false,
                    });
                    continue;
                };

                // Domain-overlap check: if [min, max] ranges are disjoint,
                // no rows can match on this key → entire join produces 0 rows.
                // Check ALL key pairs, including those that will become
                // non-spanning-tree edges in Phase B.
                if let (Some((build_min, build_max, _)), Some((probe_min, probe_max, _))) = (
                    constrained_value_domain(&outer_value, join_summary, ctx),
                    constrained_value_domain(&inner_value, join_summary, ctx),
                ) {
                    if build_max < probe_min || probe_max < build_min {
                        return Cardinality::new(0.0);
                    }
                }

                // Compute per-edge selectivity: prefer HLL, fall back to
                // containment assumption, then row-count upper bound.
                let join_constrained = has_join_constraints(&outer_value, join_summary)
                    || has_join_constraints(&inner_value, join_summary);
                let (sel, had_stats) = if !join_constrained
                    && let Some(s) = hll_join_selectivity(build_cs, probe_cs)
                {
                    (s, true)
                } else {
                    match (
                        constrained_value_ndv(&outer_value, join_summary, ctx),
                        constrained_value_ndv(&inner_value, join_summary, ctx),
                    ) {
                        (Some(build_ndv), Some(probe_ndv))
                            if build_ndv > 0 && probe_ndv > 0 =>
                        {
                            let max_ndv = build_ndv.max(probe_ndv) as f64;
                            (1.0 / max_ndv, true)
                        }
                        // We could also check the case where at least one of the sides has some cardinality
                        // statistics available. For example, we can assume that ndv of the other table is
                        // <= n or we can use it's row-count as the NDV if available.
                        // However, we want to avoid mixing statistics in potentially un-explainable ways
                        // at the moment for the sake of simplicity.
                        // TODO(AC): Benchmark the case where:
                        // a. the ndv of other table is assumed <= n
                        // b. the ndv of other table is assumed = row_count if available
                        (_, _) => {
                            let br = outer_ts.row_count;
                            let pr = inner_ts.row_count;
                            if br > 0 && pr > 0 {
                                (1.0 / br.max(pr) as f64, true)
                            } else {
                                (
                                    AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY,
                                    false,
                                )
                            }
                        }
                    }
                };

                edges.push(JoinEdge {
                    build_col: *outer_col,
                    probe_col: *inner_col,
                    selectivity: sel,
                    had_stats,
                });
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
                edges.push(JoinEdge {
                    build_col: *outer_col,
                    probe_col: *inner_col,
                    selectivity: AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY,
                    had_stats: false,
                });
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase B: Kruskal's MST — sort edges by ascending selectivity and include
    // only spanning-tree edges (those connecting different components).
    //
    // Sorting by ascending selectivity picks the most constraining edges first,
    // giving the tightest (most accurate) cardinality estimate.
    //
    // Cycle-forming edges (transitively implied by earlier edges) are skipped,
    // avoiding over-constraining the estimate for cyclic equi-conditions.
    // -----------------------------------------------------------------------
    edges.sort_unstable_by(|a, b| {
        a.selectivity
            .partial_cmp(&b.selectivity)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut graph = ColumnConstraintGraph::new();
    let mut selectivity: f64 = 1.0;
    let mut any_key_had_stats = false;

    for edge in &edges {
        // add_equality returns true iff this edge is a spanning-tree edge.
        if graph.add_equality(edge.build_col, edge.probe_col) {
            selectivity *= edge.selectivity;
            any_key_had_stats |= edge.had_stats;
        }
        // else: cycle-forming edge — transitively implied, skip.
    }

    if keys.is_empty() && !any_key_had_stats {
        selectivity = AdvancedCardinalityEstimator::FALLBACK_JOIN_SELECTIVITY;
    }

    // Apply selectivity of non-equi conditions (e.g. range predicates like x.b < y.b).
    // These are treated as a post-join filter on the equi-join result.
    // When non_equi_conds is None or literal TRUE, filter_selectivity returns 1.0 (no effect).
    let inner_card = outer_card.as_f64() * inner_card.as_f64() * selectivity;
    let non_equi_sel = non_equi_conds
        .map_or(1.0, |cond| filter_selectivity_with_summary(cond, join_summary, ctx));
    let inner_result = inner_card * non_equi_sel;

    // Adjust for join type.
    let left = outer_card.as_f64();
    let adjusted = match *join_type {
        JoinType::Inner => inner_result,
        JoinType::LeftOuter => inner_result.max(left),
        JoinType::Mark(_) => left,
        JoinType::Single => inner_result.min(left),
        JoinType::LeftSemi => todo!(),
        JoinType::LeftAnti => todo!(),
    };

    Cardinality::new(adjusted)
}

/// Route a join-side column through its local summary's lineage and
/// canonicalize. The returned `ValueRef` is what we use to look up stats
/// and constraints for that side — so an alias or equi-joined pair reads
/// from the same source column on both sides.
fn resolve_join_value(column: Column, summary: &PredicateSummary) -> ValueRef {
    let value = summary
        .lineage
        .get(&column)
        .cloned()
        .unwrap_or(ValueRef::Base(column));
    summary.canonical_value(&value)
}

/// Fetch `(TableStatistics, column_offset)` for any `ValueRef` that is
/// backed by a base column. `Derived` values return `None`, forcing
/// callers to fall back to magic-constant selectivity.
fn value_column_stats(value: &ValueRef, ctx: &IRContext) -> Option<(crate::ir::statistics::TableStatistics, usize)> {
    match value {
        ValueRef::Base(column) | ValueRef::ExtractYear(column) => column_stats(ctx, *column),
        ValueRef::Derived(_) => None,
    }
}

/// True iff the join summary has learned a literal or range constraint on
/// this value's class. Used to disable HLL-intersection selectivity when
/// constraints are present — HLL sketches describe the full column domain,
/// not the constrained sub-domain, so mixing them with a tightened NDV
/// double-counts the filter.
fn has_join_constraints(value: &ValueRef, summary: &PredicateSummary) -> bool {
    let canonical = summary.canonical_value(value);
    summary.literal_equalities.contains_key(&canonical)
        || summary.range_constraints.contains_key(&canonical)
}

/// Domain of `value`, tightened by any literal/range constraint recorded
/// in `summary`. A literal equality collapses the domain to a single
/// point; a range constraint clamps `min`/`max` to the tightened window.
fn constrained_value_domain(
    value: &ValueRef,
    summary: &PredicateSummary,
    ctx: &IRContext,
) -> Option<(f64, f64, f64)> {
    let (mut min, mut max, null_fraction) = value_domain(value, ctx)?;
    let canonical = summary.canonical_value(value);
    if let Some(literal) = summary.literal_equalities.get(&canonical) {
        return Some((*literal, *literal, null_fraction));
    }
    if let Some(range) = summary.range_constraints.get(&canonical) {
        if let Some((lower, _)) = range.lower {
            min = min.max(lower);
        }
        if let Some((upper, _)) = range.upper {
            max = max.min(upper);
        }
    }
    Some((min, max, null_fraction))
}

/// NDV scaled to a constrained domain.
///
/// A literal equality collapses to `1`. A range shrinks NDV proportionally
/// to `constrained_width / base_width`, under the usual uniform-
/// distribution assumption. If the constrained window is a single inclusive
/// point (`[v, v]`), we also collapse to `1` — this makes
/// `col = v` (expressed as a range) behave the same as a literal
/// equality. Returns at least `1` to avoid sub-row NDV estimates.
fn constrained_value_ndv(
    value: &ValueRef,
    summary: &PredicateSummary,
    ctx: &IRContext,
) -> Option<usize> {
    let canonical = summary.canonical_value(value);
    if summary.literal_equalities.contains_key(&canonical) {
        return Some(1);
    }

    let base_ndv = value_ndv(value, ctx)? as f64;
    let Some(range) = summary.range_constraints.get(&canonical) else {
        return Some(base_ndv as usize);
    };
    let (base_min, base_max, _) = value_domain(value, ctx)?;
    let domain_width = (base_max - base_min).abs();
    if domain_width <= 0.0 {
        return Some(base_ndv.max(1.0) as usize);
    }

    let (constrained_min, constrained_max, _) = constrained_value_domain(value, summary, ctx)?;
    let constrained_width = (constrained_max - constrained_min).max(0.0);
    let lower_inclusive = range.lower.map(|(_, inclusive)| inclusive).unwrap_or(true);
    let upper_inclusive = range.upper.map(|(_, inclusive)| inclusive).unwrap_or(true);
    let exact_point = constrained_width <= f64::EPSILON
        && lower_inclusive
        && upper_inclusive
        && range.lower.is_some()
        && range.upper.is_some();
    if exact_point {
        return Some(1);
    }

    let scaled = (base_ndv * (constrained_width / domain_width))
        .ceil()
        .clamp(1.0, base_ndv);
    Some(scaled as usize)
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
    let col_type = &ctx.get_column_meta(&col_meta.column).data_type;

    // Parse min/max using the column's DataType.
    let min = parse_stat_value(col_stats.min_value.as_ref()?, col_type)?;
    let max = parse_stat_value(col_stats.max_value.as_ref()?, col_type)?;
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
    if let ScalarKind::ColumnRef(col_meta) = &lhs.kind
        && let ScalarKind::Literal(lit_meta) = &rhs.kind
    {
        let lit = Literal::borrow_raw_parts(lit_meta, &rhs.common);
        if let Some(v) = scalar_value_to_f64(lit.value()) {
            return Some((col_meta.column, meta.op_kind, v));
        }
    }

    // Try literal op col → flip operator
    if let ScalarKind::Literal(lit_meta) = &lhs.kind
        && let ScalarKind::ColumnRef(col_meta) = &rhs.kind
    {
        let lit = Literal::borrow_raw_parts(lit_meta, &lhs.common);
        if let Some(v) = scalar_value_to_f64(lit.value()) {
            return Some((col_meta.column, flip_range_op(meta.op_kind), v));
        }
    }

    None
}

/// Check whether AND-ed predicates contain a contradiction.
///
/// Detects two classes of contradiction:
///
/// 1. **Range contradiction on equivalent columns**: range bounds on columns
///    that are related by equality (`col_a = col_b`) are merged into the same
///    equivalence class. If the accumulated interval for any class is empty
///    (lo ≥ hi), the conjunction is unsatisfiable.
///    - Example: `A = B AND A > 50 AND B < 30` → class {A,B} gets lo=50,
///      hi=30 → contradiction.
///    - Subsumes the previous single-column detection (`col > 50 AND col < 30`).
///
/// 2. **Literal-equality contradiction**: two different literal values for the
///    same equivalence class (`col = 5 AND col = 3`, or via transitivity
///    `A = B AND A = 5 AND B = 3`).
///
/// Returns `true` (contradiction) only when at least one class has an empty
/// range or conflicting literals. Non-range, non-equality terms are ignored.
///
/// # Algorithm
///
/// Two-pass over the AND terms:
/// 1. **Pass 1 — build equivalence classes**: extract `col_a = col_b` pairs
///    via [`extract_column_equality`] and add them to a [`ColumnConstraintGraph`].
///    Also record `col = literal` constraints via [`extract_literal_equality`].
/// 2. **Pass 2 — accumulate range bounds**: extract range bounds via
///    [`extract_range_bound`] and call [`ColumnConstraintGraph::add_range`],
///    which automatically normalises to the class representative.
///
/// # Complexity
///
/// O(n) where n = number of AND terms; two linear scans with O(1) amortized
/// Union-Find and HashMap operations. Typical queries have 2-5 AND terms.
///
/// # Unchanged behaviour for existing predicates
///
/// When no `col = col` equalities are present, the Union-Find degenerates to
/// identity mapping and the result is identical to the old single-pass
/// per-column interval check.
fn and_ranges_contradict(terms: &[std::sync::Arc<Scalar>]) -> bool {
    let mut graph = ColumnConstraintGraph::new();

    // Pass 1: build equivalence classes from col=col equalities and record
    // col=literal constraints.
    for term in terms {
        if let Some((a, b)) = extract_column_equality(term) {
            graph.add_equality(a, b);
        }
        if let Some((col, val)) = extract_literal_equality(term) {
            graph.add_literal_eq(col, val);
        }
    }

    // Pass 2: accumulate range bounds, normalised to representatives.
    for term in terms {
        if let Some((col, op, value)) = extract_range_bound(term) {
            graph.add_range(col, op, value);
        }
    }

    graph.has_range_contradiction()
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
///      where `min` and `max` come from column statistics.
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
    let col_type = &ctx.get_column_meta(&target_col).data_type;
    let min = parse_stat_value(col_stats.min_value.as_ref()?, col_type)?;
    let max = parse_stat_value(col_stats.max_value.as_ref()?, col_type)?;
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
fn domains_overlap(
    a: &ColumnStatistics,
    a_type: &DataType,
    b: &ColumnStatistics,
    b_type: &DataType,
) -> bool {
    let a_min = match a
        .min_value
        .as_ref()
        .and_then(|v| parse_stat_value(v, a_type))
    {
        Some(v) => v,
        None => return true, // Can't determine, assume overlap
    };
    let a_max = match a
        .max_value
        .as_ref()
        .and_then(|v| parse_stat_value(v, a_type))
    {
        Some(v) => v,
        None => return true,
    };
    let b_min = match b
        .min_value
        .as_ref()
        .and_then(|v| parse_stat_value(v, b_type))
    {
        Some(v) => v,
        None => return true,
    };
    let b_max = match b
        .max_value
        .as_ref()
        .and_then(|v| parse_stat_value(v, b_type))
    {
        Some(v) => v,
        None => return true,
    };

    // Ranges overlap if NOT (a_hi < b_lo OR b_hi < a_lo)
    !(a_max < b_min || b_max < a_min)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::SchemaRef;

    use crate::ir::{
        Column, DataType, IRContext, ScalarValue,
        builder::{column_ref, int32},
        catalog::{Catalog, Field, Schema},
        convert::IntoScalar,
        operator::{Remap, join::JoinType},
        statistics::TableStatistics,
        scalar::{Cast, Function, Literal},
        statistics::ColumnStatistics,
        table_ref::TableRef,
    };
    use crate::magic::{MagicCostModel, MemoryCatalog};

    use super::super::AdvancedCardinalityEstimator;

    fn adv_ctx_with_catalog(cat: MemoryCatalog) -> IRContext {
        IRContext::new(
            Arc::new(cat),
            Arc::new(AdvancedCardinalityEstimator),
            Arc::new(MagicCostModel),
        )
    }

    fn col_stats(
        distinct_count: usize,
        min_value: Option<&str>,
        max_value: Option<&str>,
    ) -> ColumnStatistics {
        ColumnStatistics {
            advanced_stats: vec![],
            min_value: min_value.map(str::to_string),
            max_value: max_value.map(str::to_string),
            null_count: Some(0),
            distinct_count: Some(distinct_count),
        }
    }

    fn int_schema(columns: usize) -> SchemaRef {
        Arc::new(Schema::new(
            (0..columns)
                .map(|idx| Field::new(format!("c{idx}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn stacked_filters_backoff_through_project_and_remap() -> crate::error::Result<()> {
        let catalog = MemoryCatalog::new("optd", "public");
        let table_ref = TableRef::bare("t1");
        let schema = int_schema(2);
        catalog.create_table(table_ref.clone(), schema.clone()).unwrap();
        catalog.set_table_statistics(
            table_ref.clone(),
            TableStatistics {
                row_count: 1000,
                column_statistics: vec![
                    col_stats(10, Some("0"), Some("9")),
                    col_stats(5, Some("0"), Some("4")),
                ],
                size_bytes: None,
            },
        )
        .unwrap();

        let ctx = adv_ctx_with_catalog(catalog);
        let get = ctx.logical_get(table_ref.clone(), None)?.build();
        let c0 = ctx.col(Some(&table_ref), "c0")?;
        let c1 = ctx.col(Some(&table_ref), "c1")?;

        let filtered = get
            .with_ctx(&ctx)
            .select(column_ref(c0).eq(int32(1)))
            .build();
        let projected = ctx
            .project(filtered, [column_ref(c0), column_ref(c1)])?
            .build();
        let aliased = ctx.remap(projected, TableRef::bare("t2"))?.build();
        let t2_c1 = ctx.col(Some(&TableRef::bare("t2")), "c1")?;
        let result = aliased
            .with_ctx(&ctx)
            .select(column_ref(t2_c1).eq(int32(2)))
            .build();

        let card = result.cardinality(&ctx).as_f64();
        assert!((card - 44.72135955).abs() < 0.01, "cardinality = {card}");
        Ok(())
    }

    #[test]
    fn transitive_contradiction_across_nested_filters_returns_zero() -> crate::error::Result<()> {
        let catalog = MemoryCatalog::new("optd", "public");
        let table_ref = TableRef::bare("t1");
        let schema = int_schema(2);
        catalog.create_table(table_ref.clone(), schema.clone()).unwrap();
        catalog.set_table_statistics(
            table_ref.clone(),
            TableStatistics {
                row_count: 1000,
                column_statistics: vec![
                    col_stats(10, Some("0"), Some("9")),
                    col_stats(10, Some("0"), Some("9")),
                ],
                size_bytes: None,
            },
        )
        .unwrap();

        let ctx = adv_ctx_with_catalog(catalog);
        let get = ctx.logical_get(table_ref.clone(), None)?.build();
        let c0 = ctx.col(Some(&table_ref), "c0")?;
        let c1 = ctx.col(Some(&table_ref), "c1")?;
        let inner = get
            .with_ctx(&ctx)
            .select(column_ref(c0).eq(column_ref(c1)).and(column_ref(c1).eq(int32(3))))
            .build();
        let outer = inner.with_ctx(&ctx).select(column_ref(c0).lt(int32(2))).build();

        assert_eq!(outer.cardinality(&ctx).as_f64(), 0.0);
        Ok(())
    }

    #[test]
    fn tautology_over_alias_preserves_input_cardinality() -> crate::error::Result<()> {
        let catalog = MemoryCatalog::new("optd", "public");
        let table_ref = TableRef::bare("t1");
        let schema = int_schema(1);
        catalog.create_table(table_ref.clone(), schema.clone()).unwrap();
        catalog.set_table_statistics(
            table_ref.clone(),
            TableStatistics {
                row_count: 1000,
                column_statistics: vec![col_stats(101, Some("0"), Some("100"))],
                size_bytes: None,
            },
        )
        .unwrap();

        let ctx = adv_ctx_with_catalog(catalog);
        let get = ctx.logical_get(table_ref.clone(), None)?.build();
        let projected = ctx.project(get, [column_ref(ctx.col(Some(&table_ref), "c0")?)])?.build();
        let aliased = ctx.remap(projected, TableRef::bare("t2"))?.build();
        let t2_c0 = ctx.col(Some(&TableRef::bare("t2")), "c0")?;
        let predicate = column_ref(t2_c0).lt(int32(0)).or(column_ref(t2_c0).ge(int32(0)));
        let result = aliased.clone().with_ctx(&ctx).select(predicate).build();

        assert_eq!(result.cardinality(&ctx).as_f64(), aliased.cardinality(&ctx).as_f64());
        Ok(())
    }

    #[test]
    fn aggregate_uses_year_lineage_for_grouping_ndv() -> crate::error::Result<()> {
        let catalog = MemoryCatalog::new("optd", "public");
        let table_ref = TableRef::bare("orders");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "order_date",
            DataType::Date32,
            false,
        )]));
        catalog.create_table(table_ref.clone(), schema.clone()).unwrap();
        catalog.set_table_statistics(
            table_ref.clone(),
            TableStatistics {
                row_count: 1000,
                column_statistics: vec![ColumnStatistics {
                    advanced_stats: vec![],
                    min_value: Some("9131".to_string()),
                    max_value: Some("9861".to_string()),
                    null_count: Some(0),
                    distinct_count: Some(731),
                }],
                size_bytes: None,
            },
        )
        .unwrap();

        let ctx = adv_ctx_with_catalog(catalog);
        let get = ctx.logical_get(table_ref.clone(), None)?.build();
        let order_date = ctx.col(Some(&table_ref), "order_date")?;
        let projected = ctx
            .project(
                get,
                [Function::new_scalar(
                    "date_part",
                    Arc::new([
                        Literal::new(ScalarValue::Utf8(Some("YEAR".to_string()))).into_scalar(),
                        column_ref(order_date),
                    ]),
                    DataType::Int32,
                )
                .into_scalar()],
            )?
            .build();
        let year_col = crate::ir::Column(*projected.borrow::<crate::ir::operator::Project>().table_index(), 0);
        let aggregate = projected
            .with_ctx(&ctx)
            .logical_aggregate(std::iter::empty(), [column_ref(year_col)])?
            .build();

        assert_eq!(aggregate.cardinality(&ctx).as_f64(), 2.0);
        Ok(())
    }

    #[test]
    fn aggregate_uses_year_lineage_for_casted_date_part_argument() -> crate::error::Result<()> {
        let catalog = MemoryCatalog::new("optd", "public");
        let table_ref = TableRef::bare("orders");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "order_date",
            DataType::Date32,
            false,
        )]));
        catalog.create_table(table_ref.clone(), schema.clone()).unwrap();
        catalog
            .set_table_statistics(
                table_ref.clone(),
                TableStatistics {
                    row_count: 1000,
                    column_statistics: vec![ColumnStatistics {
                        advanced_stats: vec![],
                        min_value: Some("9131".to_string()),
                        max_value: Some("9861".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(731),
                    }],
                    size_bytes: None,
                },
            )
            .unwrap();

        let ctx = adv_ctx_with_catalog(catalog);
        let get = ctx.logical_get(table_ref.clone(), None)?.build();
        let order_date = ctx.col(Some(&table_ref), "order_date")?;
        let projected = ctx
            .project(
                get,
                [Function::new_scalar(
                    "date_part",
                    Arc::new([
                        Cast::new(
                            DataType::Utf8,
                            Literal::new(ScalarValue::Utf8(Some("YEAR".to_string())))
                                .into_scalar(),
                        )
                        .into_scalar(),
                        column_ref(order_date),
                    ]),
                    DataType::Int32,
                )
                .into_scalar()],
            )?
            .build();
        let year_col = Column(*projected.borrow::<crate::ir::operator::Project>().table_index(), 0);
        let aggregate = projected
            .with_ctx(&ctx)
            .logical_aggregate(std::iter::empty(), [column_ref(year_col)])?
            .build();

        assert_eq!(aggregate.cardinality(&ctx).as_f64(), 2.0);
        Ok(())
    }

    #[test]
    fn join_uses_lineage_through_alias_for_key_stats() -> crate::error::Result<()> {
        let catalog = MemoryCatalog::new("optd", "public");
        let left_ref = TableRef::bare("t1");
        let right_ref = TableRef::bare("t2");
        let schema = int_schema(1);
        catalog.create_table(left_ref.clone(), schema.clone()).unwrap();
        catalog.create_table(right_ref.clone(), schema.clone()).unwrap();
        catalog
            .set_table_statistics(
                left_ref.clone(),
                TableStatistics {
                    row_count: 1000,
                    column_statistics: vec![col_stats(1000, Some("0"), Some("999"))],
                    size_bytes: None,
                },
            )
            .unwrap();
        catalog
            .set_table_statistics(
                right_ref.clone(),
                TableStatistics {
                    row_count: 1000,
                    column_statistics: vec![col_stats(1000, Some("0"), Some("999"))],
                    size_bytes: None,
                },
            )
            .unwrap();

        let ctx = adv_ctx_with_catalog(catalog);
        let left = ctx
            .remap(
                ctx.project(
                    ctx.logical_get(left_ref.clone(), None)?.build(),
                    [column_ref(ctx.col(Some(&left_ref), "c0")?)],
                )?
                .build(),
                TableRef::bare("t1_alias"),
            )?
            .build();
        let right = ctx.logical_get(right_ref.clone(), None)?.build();
        let left_col = ctx.col(Some(&TableRef::bare("t1_alias")), "c0")?;
        let right_col = ctx.col(Some(&right_ref), "c0")?;
        let joined = left
            .with_ctx(&ctx)
            .logical_join(
                right,
                column_ref(left_col).eq(column_ref(right_col)),
                JoinType::Inner,
            )
            .build();

        assert_eq!(joined.cardinality(&ctx).as_f64(), 1000.0);
        Ok(())
    }

    #[test]
    fn aggregate_preserves_lineage_through_join_project_and_remap() -> crate::error::Result<()> {
        let catalog = MemoryCatalog::new("optd", "public");
        let orders_ref = TableRef::bare("orders");
        let nation_ref = TableRef::bare("nation");
        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("custkey", DataType::Int32, false),
            Field::new("order_date", DataType::Date32, false),
        ]));
        let nation_schema = Arc::new(Schema::new(vec![
            Field::new("custkey", DataType::Int32, false),
            Field::new("nation", DataType::Utf8View, false),
        ]));
        catalog
            .create_table(orders_ref.clone(), orders_schema.clone())
            .unwrap();
        catalog
            .create_table(nation_ref.clone(), nation_schema.clone())
            .unwrap();
        catalog
            .set_table_statistics(
                orders_ref.clone(),
                TableStatistics {
                    row_count: 1000,
                    column_statistics: vec![
                        col_stats(10, Some("0"), Some("9")),
                        ColumnStatistics {
                            advanced_stats: vec![],
                            min_value: Some("9131".to_string()),
                            max_value: Some("9861".to_string()),
                            null_count: Some(0),
                            distinct_count: Some(731),
                        },
                    ],
                    size_bytes: None,
                },
            )
            .unwrap();
        catalog
            .set_table_statistics(
                nation_ref.clone(),
                TableStatistics {
                    row_count: 10,
                    column_statistics: vec![
                        col_stats(10, Some("0"), Some("9")),
                        ColumnStatistics {
                            advanced_stats: vec![],
                            min_value: None,
                            max_value: None,
                            null_count: Some(0),
                            distinct_count: Some(5),
                        },
                    ],
                    size_bytes: None,
                },
            )
            .unwrap();

        let ctx = adv_ctx_with_catalog(catalog);
        let orders = ctx.logical_get(orders_ref.clone(), None)?.build();
        let nations = ctx.logical_get(nation_ref.clone(), None)?.build();
        let order_custkey = ctx.col(Some(&orders_ref), "custkey")?;
        let order_date = ctx.col(Some(&orders_ref), "order_date")?;
        let nation_custkey = ctx.col(Some(&nation_ref), "custkey")?;
        let nation_name = ctx.col(Some(&nation_ref), "nation")?;
        let joined = orders
            .with_ctx(&ctx)
            .logical_join(
                nations,
                column_ref(order_custkey).eq(column_ref(nation_custkey)),
                JoinType::Inner,
            )
            .build();
        let projected = ctx
            .project(
                joined,
                [
                    column_ref(nation_name),
                    Function::new_scalar(
                        "date_part",
                        Arc::new([
                            Literal::new(ScalarValue::Utf8(Some("YEAR".to_string())))
                                .into_scalar(),
                            column_ref(order_date),
                        ]),
                        DataType::Int32,
                    )
                    .into_scalar(),
                ],
            )?
            .build();
        let remapped = ctx.remap(projected, TableRef::bare("profit"))?.build();
        let remap_table = *remapped.borrow::<Remap>().table_index();
        let aggregate = remapped
            .with_ctx(&ctx)
            .logical_aggregate(
                std::iter::empty::<Arc<crate::ir::Scalar>>(),
                [column_ref(Column(remap_table, 0)), column_ref(Column(remap_table, 1))],
            )?
            .build();

        assert_eq!(aggregate.cardinality(&ctx).as_f64(), 10.0);
        Ok(())
    }
}
