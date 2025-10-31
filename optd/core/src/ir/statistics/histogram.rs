use num_traits::ToPrimitive;
use std::{
    cmp::Ordering,
    fmt::Debug,
    ops::{Bound, RangeBounds},
    usize,
};

#[derive(Clone, PartialEq)]
pub struct Histogram<T> {
    buckets: Vec<Bucket<T>>,
}

impl<T: Debug> std::fmt::Debug for Histogram<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Histogram {{")?;

        for (i, bucket) in self.buckets.iter().enumerate() {
            if i == 0 {
                writeln!(
                    f,
                    "  x <= {:?}: {{ count:{:.2}, count_distinct:{:.2} }},",
                    bucket.upper, bucket.count, bucket.distinct,
                )?;
            } else {
                // Subsequent buckets: (prev_upper, upper]
                let prev_upper = &self.buckets[i - 1].upper;
                writeln!(
                    f,
                    "  {:?} < x <= {:?}: {{ count:{:.2}, count_distinct:{:.2} }},",
                    prev_upper, bucket.upper, bucket.count, bucket.distinct,
                )?;
            }
        }

        write!(f, "}}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bucket<T> {
    /// Inclusive upper bound.
    upper: T,
    count: f64,
    /// Number of distinct items in the range of (lower, upper].
    distinct: f64,
}

impl<T> Bucket<T> {
    pub fn new(upper: T, count: f64, count_distinct: f64) -> Self {
        Self {
            upper,
            count,
            distinct: count_distinct,
        }
    }

    /// Total count in this bucket (including upper bound)
    pub fn total_count(&self) -> f64 {
        self.count
    }
}

impl<T> Histogram<T> {
    pub fn new(buckets: Vec<Bucket<T>>) -> Self {
        Self { buckets }
    }

    /// Gets the total number of items in the histogram.
    pub fn total_count(&self) -> f64 {
        self.buckets.iter().map(|b| b.total_count()).sum()
    }

    /// Gets the total number of distinct values.
    pub fn total_distinct(&self) -> f64 {
        self.buckets.iter().map(|b| b.distinct).sum()
    }
}

impl<T> Histogram<T>
where
    T: Ord + ToPrimitive,
{
    fn find_bucket(&self, value: &T) -> Option<usize> {
        let mut low = 0;
        let mut high = self.buckets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            match self.buckets[mid].upper.cmp(value) {
                Ordering::Less => {
                    low = mid + 1;
                }
                _ => {
                    high = mid;
                }
            }
        }
        if low < self.buckets.len() {
            Some(low)
        } else {
            None
        }
    }

    /// Estimate the count and distinct count for a specific value.
    pub fn estimate_equal(&self, value: &T) -> [f64; 2] {
        self.find_bucket(value)
            .and_then(|index| {
                let bucket = &self.buckets[index];
                (bucket.distinct > 0.).then(|| [bucket.count / bucket.distinct, 1.])
            })
            .unwrap_or([0., 0.])
    }

    pub fn estimate_range(&self, bounds: &impl RangeBounds<T>) -> [f64; 2] {
        if self.buckets.is_empty() {
            return [0., 0.];
        }

        // Handle the start bound
        let (start_index, start_fraction) = match bounds.start_bound() {
            Bound::Unbounded => (0, 0.),
            Bound::Included(value) => {
                if let Some(index) = self.find_bucket(value) {
                    // Find the lower bound of this bucket
                    let fraction = self.compute_fraction_in_bucket(index, value, true);
                    (index, fraction)
                } else {
                    // Value is beyond all buckets
                    return [0., 0.];
                }
            }
            Bound::Excluded(value) => {
                if let Some(index) = self.find_bucket(value) {
                    let fraction = self.compute_fraction_in_bucket(index, value, false);
                    (index, fraction)
                } else {
                    return [0., 0.];
                }
            }
        };

        // Handle the end bound
        let (end_index, end_fraction) = match bounds.end_bound() {
            Bound::Unbounded => (self.buckets.len() - 1, 1.0),
            Bound::Included(value) => {
                if let Some(index) = self.find_bucket(value) {
                    let fraction = self.compute_fraction_in_bucket(index, value, true);
                    (index, fraction)
                } else {
                    // Value is beyond all buckets
                    return [0., 0.];
                }
            }

            Bound::Excluded(value) => {
                if let Some(index) = self.find_bucket(value) {
                    let fraction = self.compute_fraction_in_bucket(index, value, false);
                    (index, fraction)
                } else {
                    return [0., 0.];
                }
            }
        };

        // If the range is invalid (end before start), return 0
        if end_index < start_index {
            return [0., 0.];
        }

        let mut total_count = 0.0;
        let mut total_distinct = 0.0;

        if start_index == end_index {
            // Both bounds are in the same bucket
            let [count, distinct] =
                self.estimate_with_bucket(start_index, start_fraction, end_fraction);
            total_count += count;
            total_distinct += distinct;
        } else {
            // Start bucket (partial)
            let [count, distinct] = self.estimate_with_bucket(start_index, start_fraction, 1.0);
            total_count += count;
            total_distinct += distinct;

            // Middle buckets (full)
            for idx in (start_index + 1)..end_index {
                total_count += self.buckets[idx].count;
                total_distinct += self.buckets[idx].distinct;
            }

            // End bucket (partial)
            let [count, distinct] = self.estimate_with_bucket(end_index, 0.0, end_fraction);
            total_count += count;
            total_distinct += distinct;
        }

        [total_count, total_distinct]
    }

    /// Compute the fraction of a bucket up to a given value.
    /// Returns a value between 0.0 and 1.0.
    fn compute_fraction_in_bucket(&self, index: usize, target_value: &T, inclusive: bool) -> f64 {
        let bucket = &self.buckets[index];

        (index != 0)
            .then(|| {
                let lower = &self.buckets[index - 1].upper;
                // Interpolate between lower and upper bounds
                let lower_num = lower.to_f64().unwrap();
                let upper_num = bucket.upper.to_f64().unwrap();
                let target_num = target_value.to_f64().unwrap();

                if upper_num == lower_num {
                    // Degenerate bucket
                    if inclusive && target_num == upper_num {
                        1.0
                    } else if target_num < upper_num {
                        0.0
                    } else {
                        1.0
                    }
                } else {
                    let fraction = (target_num - lower_num) / (upper_num - lower_num);
                    fraction.clamp(0.0, 1.0)
                }
            })
            .unwrap_or_else(|| {
                // First bucket, lower bound is implicit minimum
                // Assume uniform distribution from some minimum to upper
                // For simplicity, we interpolate based on position
                match target_value.cmp(&bucket.upper) {
                    Ordering::Less => 0.5, // Rough estimate for first bucket
                    Ordering::Equal if inclusive => 1.0,
                    Ordering::Equal => 1.0, // At the boundary
                    Ordering::Greater => 1.0,
                }
            })
    }

    fn estimate_with_bucket(
        &self,
        index: usize,
        start_fraction: f64,
        end_fraction: f64,
    ) -> [f64; 2] {
        if end_fraction <= start_fraction {
            return [0., 0.];
        }
        let bucket = &self.buckets[index];
        let range_fraction = end_fraction - start_fraction;
        [
            bucket.count * range_fraction,
            bucket.distinct * range_fraction,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_histogram() -> Histogram<i32> {
        Histogram::new(vec![
            Bucket::new(0, 0., 0.),
            Bucket::new(10, 50., 5.),
            Bucket::new(25, 50., 10.),
            Bucket::new(27, 50., 2.),
            Bucket::new(30, 48., 15.),
        ])
    }

    #[test]
    fn test_total_count() {
        let hist = create_test_histogram();
        assert_eq!(hist.total_count(), 198.0);
    }

    #[test]
    fn test_total_distinct() {
        let hist = create_test_histogram();
        assert_eq!(hist.total_distinct(), 32.0);
    }

    #[test]
    fn test_empty_histogram() {
        let hist: Histogram<i32> = Histogram::new(vec![]);
        assert_eq!(hist.total_count(), 0.0);
        assert_eq!(hist.total_distinct(), 0.0);
        assert_eq!(hist.estimate_equal(&5), [0.0, 0.0]);
        assert_eq!(hist.estimate_range(&(5..10)), [0.0, 0.0]);
    }

    #[test]
    fn test_estimate_equal_in_bucket() {
        let hist = create_test_histogram();
        // Value 12 is in bucket (10, 25] with count=50 and distinct=10
        let [count, distinct] = hist.estimate_equal(&12);
        assert_eq!(count, 5.0); // 50 / 10
        assert_eq!(distinct, 1.0);
    }

    #[test]
    fn test_estimate_equal_at_boundary() {
        let hist = create_test_histogram();
        // Value 10 is at the upper bound of bucket (0, 10]
        let [count, distinct] = hist.estimate_equal(&10);
        assert_eq!(count, 10.0); // 50 / 5
        assert_eq!(distinct, 1.0);
    }

    #[test]
    fn test_estimate_equal_not_found() {
        let hist = create_test_histogram();
        // Value 100 is beyond all buckets
        let [count, distinct] = hist.estimate_equal(&100);
        assert_eq!(count, 0.0);
        assert_eq!(distinct, 0.0);
    }

    #[test]
    fn test_estimate_equal_empty_bucket() {
        let hist = create_test_histogram();
        // Value 0 is in the first bucket with count=0 and distinct=0
        let [count, distinct] = hist.estimate_equal(&0);
        assert_eq!(count, 0.0);
        assert_eq!(distinct, 0.0);
    }

    #[test]
    fn test_estimate_range_unbounded() {
        let hist = create_test_histogram();
        let [count, distinct] = hist.estimate_range(&(..));
        assert_eq!(count, 198.0);
        assert_eq!(distinct, 32.0);
    }

    #[test]
    fn test_estimate_range_inclusive() {
        let hist = create_test_histogram();
        // Range [10..=25]
        // Start: Included(10) -> finds bucket (0,10], fraction from 10 to end of bucket
        // End: Included(25) -> finds bucket (10,25], fraction from start to 25
        // The value 10 is at the boundary, so start_fraction should be 1.0 (entire bucket)
        // The value 25 is at the upper boundary, so end_fraction should be 1.0 (entire bucket)
        // But since they're in different buckets, we get:
        //   - Bucket (0,10]: partial from start_fraction=1.0 to 1.0 = 0
        //   - Bucket (10,25]: partial from 0.0 to end_fraction=1.0 = full bucket = 50
        let [count, distinct] = hist.estimate_range(&(10..=25));
        // Only the second bucket (10,25] should be fully included
        assert_eq!(count, 50.0);
        assert_eq!(distinct, 10.0);
    }

    #[test]
    fn test_estimate_range_exclusive() {
        let hist = create_test_histogram();
        // Range [10..25) should include bucket (0,10] fully and partial (10,25]
        let [count, _] = hist.estimate_range(&(10..25));
        // Should get full bucket (0,10]: 50
        // Plus partial bucket (10,25]: something less than 50
        assert!(count >= 50.0 && count < 100.0);
    }

    #[test]
    fn test_estimate_range_single_bucket() {
        let hist = create_test_histogram();
        // Range [11..=15] is entirely within bucket (10,25]
        let [count, distinct] = hist.estimate_range(&(11..=15));
        // This should be a fraction of the bucket (10,25] with count=50, distinct=10
        println!("Count: {}, Distinct: {}", count, distinct);
        assert!(count > 0.0 && count < 50.0);
        assert!(distinct > 0.0 && distinct < 10.0);
    }

    #[test]
    fn test_estimate_range_beyond_histogram() {
        let hist = create_test_histogram();
        // Range [100..=200] is beyond all buckets
        let [count, distinct] = hist.estimate_range(&(100..=200));
        assert_eq!(count, 0.0);
        assert_eq!(distinct, 0.0);
    }

    #[test]
    fn test_estimate_range_invalid_range() {
        let hist = create_test_histogram();
        // Range where end < start should return 0
        let [count, distinct] = hist.estimate_range(&(25..=10));
        assert_eq!(count, 0.0);
        assert_eq!(distinct, 0.0);
    }

    #[test]
    fn test_estimate_range_unbounded_start() {
        let hist = create_test_histogram();
        // Range (..20] should include buckets up to and partially into (10,25]
        let [count, _] = hist.estimate_range(&(..=20));
        assert!(count >= 50.0); // At least the first bucket (0,10]
    }

    #[test]
    fn test_estimate_range_unbounded_end() {
        let hist = create_test_histogram();
        // Range [20..] should include partial (10,25] and all subsequent buckets
        let [count, _] = hist.estimate_range(&(20..));
        // Should include full buckets (25,27] and (27,30]: 50 + 48 = 98
        // Plus partial (10,25]: some portion of 50
        assert!(count >= 98.0 && count <= 148.0);
    }

    #[test]
    fn test_find_bucket_basic() {
        let hist = create_test_histogram();
        assert_eq!(hist.find_bucket(&0), Some(0));
        assert_eq!(hist.find_bucket(&5), Some(1));
        assert_eq!(hist.find_bucket(&10), Some(1));
        assert_eq!(hist.find_bucket(&15), Some(2));
        assert_eq!(hist.find_bucket(&25), Some(2));
        assert_eq!(hist.find_bucket(&26), Some(3));
        assert_eq!(hist.find_bucket(&30), Some(4));
        assert_eq!(hist.find_bucket(&100), None);
    }

    #[test]
    fn test_bucket_total_count() {
        let bucket = Bucket::new(10, 42.5, 5.0);
        assert_eq!(bucket.total_count(), 42.5);
    }

    #[test]
    fn test_histogram_debug_format() {
        let hist = Histogram::new(vec![Bucket::new(5, 10., 2.), Bucket::new(10, 20., 3.)]);
        let debug_str = format!("{:?}", hist);
        assert!(debug_str.contains("Histogram"));
        assert!(debug_str.contains("x <= 5"));
        assert!(debug_str.contains("5 < x <= 10"));
    }

    #[test]
    fn test_single_bucket_histogram() {
        let hist = Histogram::new(vec![Bucket::new(100, 50., 10.)]);
        assert_eq!(hist.total_count(), 50.0);
        assert_eq!(hist.total_distinct(), 10.0);

        let [count, distinct] = hist.estimate_equal(&50);
        assert_eq!(count, 5.0);
        assert_eq!(distinct, 1.0);

        let [count, distinct] = hist.estimate_range(&(..));
        assert_eq!(count, 50.0);
        assert_eq!(distinct, 10.0);
    }

    // Tests for linear interpolation logic
    #[test]
    fn test_interpolation_midpoint() {
        // Bucket (10, 25] with count=50, distinct=10
        let hist = create_test_histogram();

        // Test value at midpoint: 17.5 (halfway between 10 and 25)
        // Fraction should be 0.5
        let fraction = hist.compute_fraction_in_bucket(2, &17, false);
        // 17 is at (17-10)/(25-10) = 7/15 ≈ 0.4667
        assert!((fraction - 0.4667).abs() < 0.001);
    }

    #[test]
    fn test_interpolation_quarter_point() {
        let hist = create_test_histogram();

        // Test value at quarter point in bucket (10, 25]
        // 13.75 is at (13.75-10)/(25-10) = 3.75/15 = 0.25
        let fraction = hist.compute_fraction_in_bucket(2, &13, false);
        // 13 is at (13-10)/(25-10) = 3/15 = 0.2
        assert!((fraction - 0.2).abs() < 0.001);
    }

    #[test]
    fn test_interpolation_at_boundaries() {
        let hist = create_test_histogram();

        // Test at lower boundary (exclusive)
        // Value 10 in bucket (10, 25] should be at position 0.0
        let fraction = hist.compute_fraction_in_bucket(2, &10, false);
        assert_eq!(fraction, 0.0);

        // Test at upper boundary (inclusive)
        // Value 25 in bucket (10, 25] should be at position 1.0
        let fraction = hist.compute_fraction_in_bucket(2, &25, true);
        assert_eq!(fraction, 1.0);
    }

    #[test]
    fn test_interpolation_narrow_bucket() {
        let hist = create_test_histogram();

        // Test narrow bucket (25, 27] with count=50, distinct=2
        // Value 26 should be at (26-25)/(27-25) = 1/2 = 0.5
        let fraction = hist.compute_fraction_in_bucket(3, &26, false);
        assert_eq!(fraction, 0.5);
    }

    #[test]
    fn test_interpolation_near_boundaries() {
        let hist = create_test_histogram();

        // Test very close to lower bound in bucket (10, 25]
        // Value 11 should be at (11-10)/(25-10) = 1/15 ≈ 0.0667
        let fraction = hist.compute_fraction_in_bucket(2, &11, false);
        assert!((fraction - 0.0667).abs() < 0.001);

        // Test very close to upper bound
        // Value 24 should be at (24-10)/(25-10) = 14/15 ≈ 0.9333
        let fraction = hist.compute_fraction_in_bucket(2, &24, false);
        assert!((fraction - 0.9333).abs() < 0.001);
    }

    #[test]
    fn test_interpolation_first_bucket() {
        let hist = create_test_histogram();

        // First bucket uses a different logic (no lower bound)
        // For first bucket (-∞, 0], it uses a heuristic
        let fraction = hist.compute_fraction_in_bucket(0, &0, true);
        // At the upper boundary with inclusive, should be 1.0
        assert_eq!(fraction, 1.0);

        // Value less than upper bound uses 0.5 heuristic
        let fraction = hist.compute_fraction_in_bucket(0, &-5, false);
        assert_eq!(fraction, 0.5);
    }

    #[test]
    fn test_interpolation_with_range_query() {
        let hist = create_test_histogram();

        // Test that interpolation works correctly in range queries
        // Range [12..20] in bucket (10, 25]
        // Start fraction: (12-10)/(25-10) = 2/15 ≈ 0.1333
        // End fraction: (20-10)/(25-10) = 10/15 ≈ 0.6667
        // Range fraction: 0.6667 - 0.1333 = 0.5333
        // Expected count: 50 * 0.5333 ≈ 26.67
        let [count, distinct] = hist.estimate_range(&(12..20));

        // The bucket has count=50, so we expect roughly 26.67
        assert!(
            (count - 26.67).abs() < 0.5,
            "Expected ~26.67, got {}",
            count
        );

        // For distinct: bucket has 10 distinct, so expect 10 * 0.5333 ≈ 5.33
        assert!(
            (distinct - 5.33).abs() < 0.5,
            "Expected ~5.33, got {}",
            distinct
        );
    }

    #[test]
    fn test_interpolation_degenerate_bucket() {
        // Create a histogram with a degenerate bucket (same upper and lower bounds)
        let hist = Histogram::new(vec![
            Bucket::new(10, 50., 5.),
            Bucket::new(10, 30., 3.), // Degenerate: same as previous upper
            Bucket::new(20, 40., 4.),
        ]);

        // In a degenerate bucket, interpolation should handle edge cases
        let fraction = hist.compute_fraction_in_bucket(1, &10, true);
        // Should return 1.0 when inclusive and value equals the bound
        assert_eq!(fraction, 1.0);

        // Value less than bound
        let fraction = hist.compute_fraction_in_bucket(1, &9, false);
        assert_eq!(fraction, 0.0);
    }

    #[test]
    fn test_interpolation_extreme_values() {
        let hist = Histogram::new(vec![
            Bucket::new(0, 100., 10.),
            Bucket::new(1000, 200., 20.),
            Bucket::new(1000000, 300., 30.),
        ]);

        // Test interpolation in a large bucket (1000, 1000000]
        // Value 500000 should be close to midpoint
        // Fraction: (500000-1000)/(1000000-1000) = 499000/999000 ≈ 0.4995
        let fraction = hist.compute_fraction_in_bucket(2, &500000, false);
        assert!((fraction - 0.4995).abs() < 0.001);

        // Test very small fraction
        // Value 2000 in bucket (1000, 1000000]
        // Fraction: (2000-1000)/(1000000-1000) = 1000/999000 ≈ 0.001
        let fraction = hist.compute_fraction_in_bucket(2, &2000, false);
        assert!((fraction - 0.001).abs() < 0.0001);
    }
}
