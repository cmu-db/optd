//----------------------------------------------------//
//   This free (MIT) Software is provided to you by   //
//       _____                         _              //
//      / ____|                       (_)             //
//      | |  __ _   _ _ __   __ _ _ __  _ _ __        //
//      | | |_ | | | | '_ \ / _` | '_ \| | '__|       //
//      | |__| | |_| | | | | (_| | | | | | |          //
//       \_____|\__,_|_| |_|\__, |_| |_|_|_|          //
//                           __/ |                    //
//                          |___/                     //
//                                                    //
// Author: Alexis Schlomer <aschlome@andrew.cmu.edu>  //
//----------------------------------------------------//

//! Implementation of the TDigest data structure as described in Ted Dunning's paper
//! "Computing Extremely Accurate Quantiles Using t-Digests" (2019).
//! For more details, refer to: https://arxiv.org/pdf/1902.04023.pdf

use itertools::Itertools;

// The TDigest structure for the statistical aggregator to query quantiles.
pub struct TDigest {
    centroids: Vec<Centroid>, // A sorted array of Centroids, according to their mean.
    compression: f64, // Compression factor: higher is more precise, but has higher memory requirements.
    total_weight: usize, // Number of values in the TDigest (sum of all centroids).
}

// A Centroid is a cluster of aggregated data points.
#[derive(PartialEq, PartialOrd, Clone)]
struct Centroid {
    mean: f64,     // Mean of all aggregated points in this cluster.
    weight: usize, // The number of points in this cluster.
}

// Utility functions defined on a Centroid.
impl Centroid {
    // Merges an existing Centroid into itself.
    fn merge(&mut self, other: &Centroid) {
        let weight = self.weight + other.weight; // TODO(Alexis): Investigate f64 precision loss.
        self.mean =
            ((self.mean * self.weight as f64) + (other.mean * other.weight as f64)) / weight as f64;
        self.weight = weight;
    }
}

// Self-contained implementation of the TDigest data structure.
impl TDigest {
    // Creates and initializes a new empty TDigest.
    pub fn new(compression: f64) -> Self {
        TDigest {
            centroids: Vec::new(),
            compression,
            total_weight: 0,
        }
    }

    // Ingests an array of values into the TDigest.
    // This is achieved by invoking the merge operation on unit Centroids.
    // 'Values' serves as a bounded buffer utilized by the execution engine, responsible
    // for determining when to merge and flush the accumulated values into the TDigest.
    pub fn merge_values(self, values: &mut Vec<f64>) -> Self {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap()); // TODO(Alexis) Assume no NaN.

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

    // Merges two TDigests together and returns a new one.
    // Particularly useful for parallel execution.
    // NOTE: Takes ownership of self and other.
    pub fn merge(self, other: TDigest) -> Self {
        let mut sorted_centroids = self.centroids.iter().merge(other.centroids.iter());

        let mut centroids: Vec<Centroid> = Vec::new();
        let compression = self.compression;
        let total_weight = self.total_weight + other.total_weight;

        // Initialize the greedy merging (copy first Centroid as a starting point).
        let mut q_curr = 0.0;
        let q_limit = 0.0; // TODO(Alexis): Write scale function.
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
                tmp_centroid.merge(&centroid)
            } else {
                q_curr += tmp_centroid.weight as f64 / total_weight as f64;
                // TODO(Alexis): Update q_limit here.
                centroids.push(tmp_centroid);
                tmp_centroid = centroid.clone();
            }
        }

        TDigest {
            centroids,
            compression,
            total_weight,
        }
    }

    // TODO(Alexis): See what function to use and how to optimize f(f-1()).
    fn k_scale(quantile: f64) -> f64 {
        0.0
    }
}
