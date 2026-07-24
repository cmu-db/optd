//! Adjacent-sequence-interchange algebra for the `C_out` ranking surrogate.
//!
//! IKKBZ needs more than a cost comparator: it needs the preferred order of two sequences to be
//! independent of their surrounding prefix and suffix. `C_out` has that property. A sequence is
//! summarized by its cost `C` and cardinality multiplier `T`, with composition
//!
//! ```text
//! C(UV) = C(U) + T(U) C(V)
//! T(UV) = T(U) T(V)
//! ```
//!
//! Its rank is `(T - 1) / C`. [`COutSummary::rank_cmp`] compares that rank without division by
//! comparing the two possible adjacent orders directly. This also gives useful behavior for exact
//! zero-cardinality estimates, where the quotient form is undefined.

use std::cmp::Ordering;

/// Sufficient statistics for composing and ranking a `C_out` sequence.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct COutSummary {
    cost: f64,
    multiplier: f64,
}

impl COutSummary {
    /// Summary for the chosen start relation.
    ///
    /// `C_out` sums join outputs, not base scans. The root cardinality therefore scales every
    /// later join but contributes no cost by itself.
    pub(super) fn root(cardinality: f64) -> Self {
        Self {
            cost: 0.0,
            multiplier: non_negative_or_neutral(cardinality),
        }
    }

    /// Summary for a non-root relation whose incoming edge has the given selectivity.
    pub(super) fn relation(cardinality: f64, selectivity: f64) -> Self {
        let multiplier = non_negative_product(
            non_negative_or_neutral(cardinality),
            non_negative_or_neutral(selectivity),
        );
        Self {
            cost: multiplier,
            multiplier,
        }
    }

    /// Concatenates two sequences.
    pub(super) fn then(self, next: Self) -> Self {
        Self {
            cost: self.cost + non_negative_product(self.multiplier, next.cost),
            multiplier: non_negative_product(self.multiplier, next.multiplier),
        }
    }

    /// Compares the ASI ranks of two sequences in ascending order.
    ///
    /// Expanding `C(UV)` and `C(VU)` gives
    ///
    /// ```text
    /// C(UV) <= C(VU)  iff  rank(U) <= rank(V).
    /// ```
    ///
    /// Comparing the expanded costs avoids division, retains the sign of ranks below zero, and
    /// gives the natural "produce zero rows first" answer when one sequence has zero cost.
    pub(super) fn rank_cmp(self, other: Self) -> Ordering {
        self.then(other).cost.total_cmp(&other.then(self).cost)
    }

    pub(super) fn cost(self) -> f64 {
        self.cost
    }
}

/// Replaces invalid estimates with the neutral multiplicative factor.
///
/// Zero and positive infinity are meaningful extended-cardinality values and remain intact.
/// NaN or a negative estimate must not masquerade as an empty, maximally selective relation.
fn non_negative_or_neutral(value: f64) -> f64 {
    if value.is_nan() || value.is_sign_negative() {
        1.0
    } else {
        value
    }
}

/// Multiplies extended nonnegative cardinalities with zero as the absorbing value.
///
/// IEEE-754 defines `0 * infinity` as NaN. For cardinality propagation, however, a known empty
/// input keeps every later inner-join result empty, so zero is the useful deterministic result.
fn non_negative_product(left: f64, right: f64) -> f64 {
    if left == 0.0 || right == 0.0 {
        0.0
    } else {
        left * right
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(cost: f64, multiplier: f64) -> COutSummary {
        COutSummary { cost, multiplier }
    }

    fn compose(parts: impl IntoIterator<Item = COutSummary>) -> COutSummary {
        parts
            .into_iter()
            .reduce(COutSummary::then)
            .expect("test sequences are non-empty")
    }

    #[test]
    fn composition_is_associative() {
        let a = summary(3.0, 0.25);
        let b = summary(7.0, 4.0);
        let c = summary(11.0, 0.5);

        assert_eq!(a.then(b).then(c), a.then(b.then(c)));
    }

    #[test]
    fn rank_comparison_is_context_independent() {
        let contexts = [
            (summary(1.0, 1.0), summary(1.0, 1.0)),
            (summary(17.0, 0.2), summary(9.0, 13.0)),
            (summary(4.0, 8.0), summary(21.0, 0.125)),
        ];
        let sequences = [
            summary(2.0, 0.25),
            summary(3.0, 1.0),
            summary(5.0, 7.0),
            summary(11.0, 0.0),
        ];

        for u in sequences {
            for v in sequences {
                let rank_order = u.rank_cmp(v);
                for (prefix, suffix) in contexts {
                    let uv = compose([prefix, u, v, suffix]).cost();
                    let vu = compose([prefix, v, u, suffix]).cost();
                    assert_eq!(
                        rank_order,
                        uv.total_cmp(&vu),
                        "ASI preference changed in context for {u:?} and {v:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn relation_summary_matches_prefix_cardinality_cost() {
        let sequence = COutSummary::root(100.0)
            .then(COutSummary::relation(50.0, 0.01))
            .then(COutSummary::relation(20.0, 0.1));

        // Join-output cardinalities: 50, 100. The 100-row base scan is not charged.
        assert_eq!(sequence.cost(), 150.0);
    }

    #[test]
    fn zero_cardinality_sequence_sorts_first_without_division() {
        let empty = COutSummary::relation(0.0, 1.0);
        let growing = COutSummary::relation(10.0, 1.0);

        assert_eq!(empty.rank_cmp(growing), Ordering::Less);
        assert_eq!(growing.rank_cmp(empty), Ordering::Greater);
    }

    #[test]
    fn non_finite_estimates_never_create_nan_summaries() {
        let empty =
            COutSummary::root(f64::INFINITY).then(COutSummary::relation(0.0, f64::INFINITY));
        assert_eq!(empty, summary(0.0, 0.0));

        let invalid = COutSummary::root(f64::NAN).then(COutSummary::relation(-1.0, f64::NAN));
        assert_eq!(invalid, summary(1.0, 1.0));
        assert!(!invalid.cost().is_nan());
    }
}
