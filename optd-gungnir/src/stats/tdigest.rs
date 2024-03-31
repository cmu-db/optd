//! Simplified implementation of the TDigest data structure as described in
//! Ted Dunning's paper:
//! "Computing Extremely Accurate Quantiles Using t-Digests" (2019).
//! For more details, refer to: https://arxiv.org/pdf/1902.04023.pdf

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::f64::consts::PI;

pub const DEFAULT_COMPRESSION: f64 = 200.0;

/// The TDigest structure for the statistical aggregator to query quantiles.
#[derive(Clone, Serialize, Deserialize)]
pub struct TDigest {
    /// A sorted array of Centroids, according to their mean.
    centroids: Vec<Centroid>,
    /// Compression factor: higher is more precise, but has higher memory requirements.
    compression: f64,
    /// Number of values in the TDigest (sum of all centroids).
    total_weight: usize,
}

/// A Centroid is a cluster of aggregated data points.
#[derive(PartialEq, PartialOrd, Clone, Serialize, Deserialize)]
struct Centroid {
    /// Mean of all aggregated points in this cluster.
    mean: f64,
    /// The number of points in this cluster.
    weight: usize,
}

// Utility functions defined on a Centroid.
impl Centroid {
    // Merges an existing Centroid into itself.
    fn merge(&mut self, other: &Centroid) {
        let weight = self.weight + other.weight;
        self.mean =
            ((self.mean * self.weight as f64) + (other.mean * other.weight as f64)) / weight as f64;
        self.weight = weight;
    }
}

// Self-contained implementation of the TDigest data structure.
impl TDigest {
    /// Creates and initializes a new empty TDigest.
    pub fn new(compression: f64) -> Self {
        TDigest {
            centroids: Vec::new(),
            compression,
            total_weight: 0,
        }
    }

    /// Ingests an array of non-NaN f64 values into the TDigest.
    /// This is achieved by invoking the merge operation on unit Centroids.
    /// 'Values' serves as a bounded buffer utilized by the execution engine, responsible
    /// for determining when to merge and flush the accumulated values into the TDigest.
    pub fn merge_values(self, values: &mut [f64]) -> Self {
        values.sort_by(|a, b| a.partial_cmp(b).expect("Slice should not contain NaNs"));

        let centroids = values
            .iter()
            .map(|v| Centroid {
                mean: *v,
                weight: 1,
            })
            .collect_vec();
        let compression = self.compression;
        let total_weight = centroids.len();

        self.merge(TDigest {
            centroids,
            compression,
            total_weight,
        })
    }

    /// Merges two TDigests together and returns a new one.
    /// Particularly useful for parallel execution.
    /// NOTE: Takes ownership of self and other.
    pub fn merge(self, other: TDigest) -> Self {
        let mut sorted_centroids = self.centroids.iter().merge(other.centroids.iter());

        let mut centroids: Vec<Centroid> = Vec::new();
        let compression = self.compression;
        let total_weight = self.total_weight + other.total_weight;

        // Initialize the greedy merging (copy first Centroid as a starting point).
        let mut q_curr = 0.0;
        let mut q_limit = self.k_rev_scale(self.k_scale(q_curr) + 1.0);
        let mut tmp_centroid = match sorted_centroids.next() {
            Some(centroid) => centroid.clone(),
            None => {
                return self;
            }
        };

        // Iterate over ordered and merged Centroids (starting from index 1).
        for centroid in sorted_centroids {
            let q_new = (tmp_centroid.weight + centroid.weight) as f64 / total_weight as f64;
            if (q_curr + q_new) <= q_limit {
                tmp_centroid.merge(centroid)
            } else {
                q_curr += tmp_centroid.weight as f64 / total_weight as f64;
                q_limit = self.k_rev_scale(self.k_scale(q_curr) + 1.0);
                centroids.push(tmp_centroid);
                tmp_centroid = centroid.clone();
            }
        }

        // Push leftover and return.
        centroids.push(tmp_centroid);
        TDigest {
            centroids,
            compression,
            total_weight,
        }
    }

    /// Obtains a given quantile from the TDigest.
    /// Returns 0.0 if TDigest is empty.
    /// Performs a linear interpollation between two neighboring Centroids if needed.
    pub fn quantile(&self, q: f64) -> f64 {
        let target_cum = q * (self.total_weight as f64);
        let pos_cum = self // Finds the centroid whose *cumulative weight* exceeds or equals the quantile.
            .centroids
            .iter()
            .map(|c| c.weight)
            .scan(0, |acc, weight| {
                *acc += weight;
                Some(*acc)
            })
            .enumerate()
            .find(|&(_, cum)| target_cum < (cum as f64));

        match pos_cum {
            Some((pos, cum)) => {
                // TODO: We ignore edge-cases where Centroid's weights are 1.
                if (pos == 0) || (pos == self.centroids.len() - 1) {
                    self.centroids[pos].mean
                } else {
                    // Quantile is somewhere between in (prev+curr)/2 and (curr+next)/2 means.
                    let (prev, curr, next) = (
                        &self.centroids[pos - 1],
                        &self.centroids[pos],
                        &self.centroids[pos + 1],
                    );
                    let (min_q, max_q) =
                        ((prev.mean + curr.mean) / 2.0, (curr.mean + next.mean) / 2.0);
                    lerp(
                        min_q,
                        max_q,
                        ((cum as f64) - target_cum) / (curr.weight as f64),
                    )
                }
            }
            None => self.centroids.last().map(|c| c.mean).unwrap_or(0.0),
        }
    }

    /// Obtains the CDF corresponding to a given value.
    /// Returns 0.0 if the TDigest is empty.
    pub fn cdf(&self, v: f64) -> f64 {
        let mut cum_sum = 0;
        let pos_cum = self // Finds the centroid whose *mean* exceeds or equals the given value.
            .centroids
            .iter()
            .enumerate()
            .find(|(_, c)| {
                cum_sum += c.weight; // Get the cum_sum as a side effect.
                v < c.mean
            })
            .map(|(pos, _)| (pos, cum_sum));

        match pos_cum {
            Some((_pos, cum)) => {
                // TODO: Can do better with 2 lerps, left as future work.
                // TODO: We ignore edge-cases where Centroid's weights are 1.
                (cum as f64) / (self.total_weight as f64)
            }
            None => self.centroids.last().map(|_| 1.0).unwrap_or(0.0),
        }
    }

    // Obtains the k-distance for a given quantile.
    // Note: The scaling function implemented is k1 in Ted Dunning's paper.
    fn k_scale(&self, quantile: f64) -> f64 {
        (self.compression / (2.0 * PI)) * (2.0 * quantile - 1.0).asin()
    }

    // Obtains the quantile associated to a k-distance.
    // There are probably numerical optimizations to flatten the nested
    // k_scale(k_rev_scale()) calls. But let's keep it simple.
    fn k_rev_scale(&self, k_distance: f64) -> f64 {
        ((2.0 * PI * k_distance / self.compression).sin() + 1.0) / 2.0
    }
}

// Performs the linear interpolation between a and b, given a fraction f.
fn lerp(a: f64, b: f64, f: f64) -> f64 {
    (a * (1.0 - f)) + (b * f)
}

// Start of unit testing section.
#[cfg(test)]
mod tests {
    use super::TDigest;
    use crossbeam::thread;
    use rand::{
        distributions::{Distribution, Uniform, WeightedIndex},
        rngs::StdRng,
        SeedableRng,
    };
    use std::sync::{Arc, Mutex};

    // Whether obtained = expected +/- error
    fn is_close(obtained: f64, expected: f64, error: f64) -> bool {
        ((expected - error) < obtained) && (obtained < (expected + error))
    }

    // Checks whether the tdigest follows a uniform distribution.
    fn check_tdigest_uniform(tdigest: TDigest, buckets: i32, max: f64, min: f64, error: f64) {
        for k in 0..buckets {
            let expected_cdf = (k as f64) / (buckets as f64);
            let expected_quantile = (max - min) * expected_cdf + min;

            let obtained_cdf = tdigest.cdf(expected_quantile);
            let obtained_quantile = tdigest.quantile(expected_cdf);

            assert!(is_close(obtained_cdf, expected_cdf, error));
            assert!(is_close(
                obtained_quantile,
                expected_quantile,
                (max - min) * error,
            ));
        }
    }

    #[test]
    fn uniform_merge_sequential() {
        let buckets = 200;
        let error = 0.03; // 3% absolute error on each quantile; error gets worse near the median.
        let mut tdigest = TDigest::new(buckets as f64);

        let (min, max) = (-1000.0, 1000.0);
        let uniform_distr = Uniform::new(min, max);
        let mut rng = StdRng::seed_from_u64(0);

        let batch_size = 1024;
        let batch_numbers = 64;

        for _ in 0..batch_numbers {
            let mut random_numbers = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                let num: f64 = uniform_distr.sample(&mut rng);
                random_numbers.push(num);
            }
            tdigest = tdigest.merge_values(&mut random_numbers);
        }

        check_tdigest_uniform(tdigest, buckets, max, min, error);
    }

    #[test]
    fn uniform_merge_parallel() {
        let buckets = 200;
        let error = 0.03; // 3% absolute error on each quantile, note error is worse near the median.

        let (min, max) = (-1000.0, 1000.0);

        let batch_size = 65536;
        let batch_numbers = 64;

        let result_tdigest = Arc::new(Mutex::new(Option::Some(TDigest::new(buckets as f64))));
        thread::scope(|s| {
            for _ in 0..batch_numbers {
                s.spawn(|_| {
                    let mut local_tdigest = TDigest::new(buckets as f64);

                    let mut random_numbers = Vec::with_capacity(batch_size);
                    let uniform_distr = Uniform::new(min, max);
                    let mut rng = StdRng::seed_from_u64(0);

                    for _ in 0..batch_size {
                        let num: f64 = uniform_distr.sample(&mut rng);
                        random_numbers.push(num);
                    }
                    local_tdigest = local_tdigest.merge_values(&mut random_numbers);

                    let mut result = result_tdigest.lock().unwrap();
                    *result = Option::Some(result.take().unwrap().merge(local_tdigest));
                });
            }
        })
        .unwrap();

        let tdigest = result_tdigest.lock().unwrap().take().unwrap();
        check_tdigest_uniform(tdigest, buckets, max, min, error);
    }

    #[test]
    fn weighted_merge() {
        let buckets = 200;
        let error = 0.05; // 5% absolute error on each quantile, note error is worse near the median.

        let mut tdigest = TDigest::new(buckets as f64);

        let choices = [9.0, 900.0, 990.0, 9990.0, 190000.0, 990000.0];
        let weights = [1, 2, 1, 3, 4, 5]; // Total of 16.
        let total_weight: i32 = weights.iter().sum();

        let weighted_distr = WeightedIndex::new(weights).unwrap();
        let mut rng = StdRng::seed_from_u64(0);

        let batch_size = 128;
        let batch_numbers = 16;

        for _ in 0..batch_numbers {
            let mut random_numbers = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                let num: f64 = choices[weighted_distr.sample(&mut rng)];
                random_numbers.push(num);
            }
            tdigest = tdigest.merge_values(&mut random_numbers);
        }

        let mut curr_weight = 0;
        for (c, w) in choices.iter().zip(weights) {
            curr_weight += w;
            let estimate_cdf = tdigest.cdf(*c);
            let obtained_cdf = (curr_weight as f64) / (total_weight as f64);
            assert!(is_close(obtained_cdf, estimate_cdf, error));
        }
    }
}
