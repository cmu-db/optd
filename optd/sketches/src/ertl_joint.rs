//! Joint Poisson maximum likelihood for compatible HLLs (Ertl, arXiv:1702.01284 §5).
//!
//! This fits disjoint left-only, right-only, and shared populations directly, not by
//! subtracting three noisy cardinality estimates. Bounded coordinate maximization includes
//! zero-rate boundaries and uses the register-pair sufficient statistics of equations 66–71.

use crate::{HyperLogLog, HyperLogLogError};

/// Minimum estimated Jaccard index, `|A ∩ B| / |A ∪ B|`, for consuming a joint fit in join CE.
/// At small overlap, the shared population leaves a weaker signal in the HLL register pairs,
/// so intersection estimates can have large relative error even with joint maximum likelihood
/// (see [Ertl, §5](https://arxiv.org/html/1702.01284#S5)).
/// The 5% cutoff is a conservative policy guard against this weak-signal regime, not a
/// paper-derived threshold or an accuracy guarantee; reliability also depends on sketch precision
/// and the relative set sizes.
/// Fits below it are marked `LowJaccard`, causing join CE to retain its range/NDV fallback
/// rather than consume the estimated intersection.
pub const ERTL_MIN_JACCARD: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JointCardinalityEstimate {
    pub only_left: f64,
    pub only_right: f64,
    pub intersection: f64,
    pub union: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JointEstimateQuality {
    Ok,
    /// No evidence distinguishes the shared rate from a relative complement.
    UnidentifiedIntersection,
    LowJaccard {
        jaccard: f64,
    },
    /// Saturation, failed convergence, or non-finite likelihood; do not consume the estimate.
    NumericalFailure,
}

/// Estimates two sets jointly. Only `Ok` quality is recommended for join estimation.
///
/// Equal sketches return a shared population and zero complements. Strict register dominance
/// without any positive equal register is unidentified (Ertl §5.1). Empty sets are supported.
/// The finite search is bounded to eight times the sum of marginal HLL estimates; exhausting
/// that bound or the convergence budget returns `NumericalFailure`, not a usable estimate.
pub fn estimate_joint(
    left: &HyperLogLog,
    right: &HyperLogLog,
) -> Result<(JointCardinalityEstimate, JointEstimateQuality), HyperLogLogError> {
    if !left.is_compatible(right) {
        return Err(HyperLogLogError::Incompatible);
    }
    let dl = left.estimate();
    let dr = right.estimate();
    let fallback = JointCardinalityEstimate {
        only_left: dl,
        only_right: dr,
        intersection: 0.0,
        union: dl + dr,
    };
    let q = 64 - left.precision();
    // Fully saturated sketches have no finite maximum likelihood cardinality.
    if !dl.is_finite()
        || !dr.is_finite()
        || left.registers().iter().all(|&k| k > q)
        || right.registers().iter().all(|&k| k > q)
    {
        return Ok((fallback, JointEstimateQuality::NumericalFailure));
    }
    if dl == 0.0 || dr == 0.0 {
        return Ok((fallback, JointEstimateQuality::LowJaccard { jaccard: 0.0 }));
    }
    let width = usize::from(q) + 2;
    // Eq. 71: [C1<, C1>, C2<, C2>, C=] per rank; O(q), not O(q²) bins.
    let mut counts = vec![[0_u32; 5]; width];
    let mut less = false;
    let mut greater = false;
    let mut positive_equal = false;
    for (&l, &r) in left.registers().iter().zip(right.registers()) {
        if l < r {
            counts[usize::from(l)][0] += 1;
            counts[usize::from(r)][3] += 1;
        } else if r < l {
            counts[usize::from(r)][2] += 1;
            counts[usize::from(l)][1] += 1;
        } else {
            counts[usize::from(l)][4] += 1;
        }
        less |= l < r;
        greater |= l > r;
        positive_equal |= l == r && l > 0;
    }
    if !positive_equal && !(less && greater) {
        return Ok((fallback, JointEstimateQuality::UnidentifiedIntersection));
    }
    let counts: Vec<_> = counts
        .iter()
        .enumerate()
        .filter(|(_, c)| c.iter().any(|&n| n > 0))
        .map(|(k, c)| (k as u8, c.map(f64::from)))
        .collect();
    let mut linear = [0.0; 3];
    for &(k, [al, ag, bl, bg, eq]) in &counts {
        if k <= q {
            let w = 2_f64.powi(-i32::from(k));
            linear[0] += (al + ag + eq) * w;
            linear[1] += (bl + bg + eq) * w;
            linear[2] += (al + bl + eq) * w;
        }
    }
    // Normalize rates to the marginal scale, avoiding precision-dependent search tolerances.
    let scale = dl.max(dr);
    let per_register = scale / left.registers().len() as f64;
    let objective = |rates: [f64; 3]| {
        let [a, b, x] = rates.map(|v| v * per_register);
        let mut result = -a * linear[0] - b * linear[1] - x * linear[2];
        for &(k, c) in &counts {
            if k == 0 {
                continue;
            }
            let w = 2_f64.powi(-i32::from(k.min(q)));
            let [za, zb, zx] = [a, b, x].map(|v| -(-v * w).exp_m1());
            let yx = (-x * w).exp();
            let probabilities = [zx + yx * za, za, zx + yx * zb, zb, zx + yx * za * zb];
            for (count, probability) in c.iter().zip(probabilities) {
                if *count > 0.0 {
                    result += count * probability.ln();
                }
            }
        }
        result
    };
    let mut rates = [
        dl / scale * 0.75,
        dr / scale * 0.75,
        dl.min(dr) / scale * 0.25,
    ];
    let bound = 8.0 * (dl + dr) / scale;
    let mut converged = false;
    for _ in 0..256 {
        let old = rates;
        for axis in 0..3 {
            let evaluate = |value| {
                let mut next = rates;
                next[axis] = value;
                objective(next)
            };
            rates[axis] = maximize_coordinate(&evaluate, bound);
        }
        if (0..3).all(|i| (old[i] - rates[i]).abs() < 1e-7) {
            converged = true;
            break;
        }
    }
    let [a, b, x] = rates.map(|v| v.max(0.0) * scale);
    let estimate = JointCardinalityEstimate {
        only_left: a,
        only_right: b,
        intersection: x,
        union: a + b + x,
    };
    let quality = if !converged
        || !objective(rates).is_finite()
        || rates.iter().any(|&v| v > bound * 0.999)
    {
        JointEstimateQuality::NumericalFailure
    } else {
        let jaccard = x / estimate.union;
        if jaccard < ERTL_MIN_JACCARD {
            JointEstimateQuality::LowJaccard { jaccard }
        } else {
            JointEstimateQuality::Ok
        }
    };
    Ok((estimate, quality))
}

// Golden-section maximization with explicit zero boundary. Each one-dimensional section of
// the joint Poisson log likelihood is unimodal. No log-rate transform hides boundary optima.
fn maximize_coordinate(f: &impl Fn(f64) -> f64, bound: f64) -> f64 {
    const RATIO: f64 = 0.618_033_988_749_894_9;
    let (mut lo, mut hi) = (0.0, bound);
    let mut a = hi - RATIO * (hi - lo);
    let mut b = lo + RATIO * (hi - lo);
    let (mut fa, mut fb) = (f(a), f(b));
    for _ in 0..48 {
        if fa > fb {
            hi = b;
            b = a;
            fb = fa;
            a = hi - RATIO * (hi - lo);
            fa = f(a);
        } else {
            lo = a;
            a = b;
            fa = fb;
            b = lo + RATIO * (hi - lo);
            fb = f(b);
        }
    }
    let best = if fa > fb { a } else { b };
    if f(0.0) >= f(best) { 0.0 } else { best }
}

#[cfg(test)]
fn log_pair_probability(l: u8, r: u8, q: u8, rates: [f64; 3]) -> f64 {
    let [a, b, x] = rates;
    let weight = |k: u8| 2_f64.powi(-i32::from(k));
    let log_mass = |rate: f64, k: u8| {
        let tail = rate * weight(k.min(q));
        let cdf = if k <= q { -tail } else { 0.0 };
        if k == 0 {
            cdf
        } else {
            cdf + (-(-tail).exp_m1()).ln()
        }
    };
    if l < r {
        log_mass(a + x, l) + log_mass(b, r)
    } else if r < l {
        log_mass(a, l) + log_mass(b + x, r)
    } else if l == 0 {
        -(a + b + x)
    } else {
        let w = weight(l.min(q));
        // Stable form of 1-exp(-(a+x)w)-exp(-(b+x)w)+exp(-(a+b+x)w).
        let shared = -(-x * w).exp_m1();
        let separate = (-x * w).exp() * (-(-a * w).exp_m1()) * (-(-b * w).exp_m1());
        let cdf = if l <= q { -(a + b + x) * w } else { 0.0 };
        cdf + (shared + separate).ln()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sketch(start: u64, end: u64, seed: u64) -> HyperLogLog {
        let mut hll = HyperLogLog::with_config(12, seed, 1).unwrap();
        for value in start..end {
            hll.insert(&value.to_le_bytes());
        }
        hll
    }

    #[test]
    fn joint_probability_normalizes_including_saturation_and_zero_rates() {
        for rates in [[1.0, 2.0, 0.5], [0.0, 1.0, 2.0], [1.0, 1.0, 0.0], [0.0; 3]] {
            let total: f64 = (0..=4)
                .flat_map(|l| (0..=4).map(move |r| (l, r)))
                .map(|(l, r)| log_pair_probability(l, r, 3, rates).exp())
                .sum();
            assert!((total - 1.0).abs() < 1e-12, "{rates:?}: {total}");
        }
    }

    #[test]
    fn identical_and_empty_sketches() {
        let h = sketch(0, 20_000, 42);
        let (e, quality) = estimate_joint(&h, &h).unwrap();
        assert_eq!(quality, JointEstimateQuality::Ok);
        assert!((e.intersection / h.estimate() - 1.0).abs() < 0.05, "{e:?}");
        assert!(e.only_left + e.only_right < 1.0, "{e:?}");
        let empty = sketch(0, 0, 42);
        let (e, _) = estimate_joint(&empty, &h).unwrap();
        assert_eq!(e.intersection, 0.0);
    }

    #[test]
    fn disjoint_and_partial_overlap() {
        let a = sketch(0, 100_000, 42);
        let b = sketch(100_000, 200_000, 42);
        let (e, q) = estimate_joint(&a, &b).unwrap();
        assert!(e.intersection < 5_000.0, "{e:?}");
        assert!(matches!(q, JointEstimateQuality::LowJaccard { .. }));
        // Fixed seeds compare aggregate absolute errors, not a promise for every random pair.
        let (mut joint_error, mut ie_error) = (0.0, 0.0);
        for seed in 0..12 {
            let a = sketch(0, 100_000, seed);
            let b = sketch(80_000, 180_000, seed);
            let (e, q) = estimate_joint(&a, &b).unwrap();
            assert_eq!(q, JointEstimateQuality::Ok, "{seed}: {e:?}");
            assert_eq!(e.union, e.only_left + e.only_right + e.intersection);
            joint_error += (e.intersection - 20_000.0).abs();
            let mut union = a.clone();
            union.merge(&b).unwrap();
            // IE is a test comparator only; never used by the estimator.
            ie_error += (a.estimate() + b.estimate() - union.estimate() - 20_000.0).abs();
        }
        assert!(joint_error < ie_error, "joint {joint_error}, IE {ie_error}");
    }

    #[test]
    fn incompatible_and_unidentified() {
        let a = sketch(0, 100, 42);
        for b in [
            HyperLogLog::with_config(10, 42, 1).unwrap(),
            HyperLogLog::with_config(12, 43, 1).unwrap(),
            HyperLogLog::new(2),
        ] {
            assert_eq!(estimate_joint(&a, &b), Err(HyperLogLogError::Incompatible));
        }
        let a = HyperLogLog::from_registers(4, 42, 1, vec![5; 16]).unwrap();
        let b = HyperLogLog::from_registers(4, 42, 1, vec![2; 16]).unwrap();
        assert_eq!(
            estimate_joint(&a, &b).unwrap().1,
            JointEstimateQuality::UnidentifiedIntersection
        );
        let saturated = HyperLogLog::from_registers(4, 42, 1, vec![61; 16]).unwrap();
        assert_eq!(
            estimate_joint(&saturated, &saturated).unwrap().1,
            JointEstimateQuality::NumericalFailure
        );
    }
}
