// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Implementation of the HyperLogLog data structure as described in Flajolet et al. paper:
//! "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm" (2007).
//! For more details, refer to:
//! https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
//! We modified it by hashing objects into 64-bit values instead of 32-bit ones to reduce the
//! number of collisions and eliminate the need for a large range correction estimator.

use std::cmp::max;
use std::marker::PhantomData;

use optd_core::nodes::Value;

use crate::stats::murmur2::murmur_hash;

pub const DEFAULT_PRECISION: u8 = 16;

/// Trait to transform any object into a stream of bytes.
pub trait ByteSerializable {
    fn to_bytes(&self) -> Vec<u8>;
}

/// The HyperLogLog (HLL) structure to provide a statistical estimate of NDistinct.
/// For safety reasons, HLLs can only count elements of the same ByteSerializable type.
#[derive(Clone)]
pub struct HyperLogLog<T: ByteSerializable> {
    registers: Vec<u8>, // The buckets to estimate HLL on (i.e. upper p bits).
    precision: u8,      // The precision (p) of our HLL; 4 <= p <= 16.
    m: usize,           // The number of HLL buckets; 2^p.
    alpha: f64,         // The normal HLL multiplier factor.

    data_type: PhantomData<T>, // For type checker.
}

// Serialize optd's Value.
// TODO(Alexis): We should make stat serialization consistent.
// This solution works for now, but a cleaner approach would be to not
// create a new ByteSerializable interface. The initial design decision
// was to have a way to serialize objects into bytes so we could use a custom
// hash function and avoid hash recomputations.
impl ByteSerializable for Vec<Option<Value>> {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for opt in self {
            if let Some(val) = opt {
                bytes.append(&mut val.to_string().to_bytes());
            }
            bytes.push(0);
        }
        bytes
    }
}

// Serialize common data types for hashing (&str).
impl ByteSerializable for &str {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

// Serialize common data types for hashing (String).
impl ByteSerializable for String {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_str().to_bytes()
    }
}

// Serialize common data types for hashing (bool).
impl ByteSerializable for bool {
    fn to_bytes(&self) -> Vec<u8> {
        (*self as u8).to_bytes()
    }
}

// Serialize common data types for hashing (numeric).
macro_rules! impl_byte_serializable_for_numeric {
        ($($type:ty),*) => {
            $(
                impl ByteSerializable for $type {
                    fn to_bytes(&self) -> Vec<u8> {
                        self.to_le_bytes().to_vec()
                    }
                }
            )*
        };
    }

impl_byte_serializable_for_numeric!(u128, u64, u32, u16, u8);
impl_byte_serializable_for_numeric!(i128, i64, i32, i16, i8);
impl_byte_serializable_for_numeric!(usize, isize);
impl_byte_serializable_for_numeric!(f64, f32);

// Self-contained implementation of the HyperLogLog data structure.
impl<'a, T> HyperLogLog<T>
where
    T: ByteSerializable + 'a,
{
    /// Creates and initializes a new empty HyperLogLog.
    pub fn new(precision: u8) -> Self {
        assert!((4..=16).contains(&precision));

        let m = 1 << precision;
        let alpha = compute_alpha(m);

        HyperLogLog {
            registers: vec![0; m],
            precision,
            m,
            alpha,

            data_type: PhantomData,
        }
    }

    pub fn process(&mut self, element: &T)
    where
        T: ByteSerializable,
    {
        let hash = murmur_hash(&element.to_bytes(), 0); // TODO: We ignore DoS attacks (seed).
        let mask = (1 << (self.precision)) - 1;
        let idx = (hash & mask) as usize; // LSB is bucket discriminator; MSB is zero streak.
        self.registers[idx] = max(self.registers[idx], self.zeros(hash) + 1);
    }

    /// Digests an array of ByteSerializable data into the HLL.
    pub fn aggregate<I>(&mut self, data: I)
    where
        I: Iterator<Item = &'a T>,
        T: ByteSerializable,
    {
        data.for_each(|e| self.process(e));
    }

    /// Merges two HLLs together and returns a new one.
    /// Particularly useful for parallel execution.
    pub fn merge(&mut self, other: &HyperLogLog<T>) {
        assert!(self.precision == other.precision);

        self.registers = self
            .registers
            .iter()
            .zip(other.registers.iter())
            .map(|(&x, &y)| x.max(y))
            .collect();
    }

    /// Returns an estimation of the n_distinct seen so far by the HLL.
    pub fn n_distinct(&self) -> u64 {
        let m = self.m as f64;
        let raw_estimate = self.alpha * (m * m)
            / self
                .registers
                .iter()
                .fold(0.0, |acc, elem| (1.0 / 2.0f64.powi(*elem as i32)) + acc);

        if raw_estimate <= ((5.0 * m) / 2.0) {
            let empty_reg = self.registers.iter().filter(|&elem| *elem == 0).count();
            if empty_reg != 0 {
                (m * (m / (empty_reg as f64)).ln()).round() as u64
            } else {
                raw_estimate.round() as u64
            }
        } else {
            raw_estimate.round() as u64
        }
    }

    // Returns the number of consecutive zeros in hash, starting from LSB.
    fn zeros(&self, hash: u64) -> u8 {
        let max_bit = 64 - self.precision;
        (0..max_bit)
            .take_while(|i| (hash & (1 << (self.precision + i))) == 0)
            .count() as u8
    }
}

// Computes the alpha HLL parameter based on m.
fn compute_alpha(m: usize) -> f64 {
    match m {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / (m as f64)),
    }
}

// Start of unit testing section.
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use crossbeam::thread;
    use rand::distributions::Alphanumeric;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::HyperLogLog;

    #[test]
    fn hll_small_strings() {
        let mut hll = HyperLogLog::new(12);

        let data = ["a".to_string(), "b".to_string()];
        hll.aggregate(data.iter());
        assert_eq!(hll.n_distinct(), data.len() as u64);
    }

    #[test]
    fn hll_small_u64() {
        let mut hll = HyperLogLog::new(12);

        let data = [1, 2];
        hll.aggregate(data.iter());
        assert_eq!(hll.n_distinct(), data.len() as u64);
    }

    // Generates n random 32-char length strings that have an occurance sampled from
    // the uniform [1:max_occ] distribution.
    fn generate_random_strings(n: usize, max_occ: usize, job_id: usize) -> Vec<String> {
        let mut strings = Vec::with_capacity(n);
        for idx in 0..n {
            let mut rng = StdRng::seed_from_u64((job_id * n + idx) as u64);
            let rand_string: String = rng
                .clone()
                .sample_iter(&Alphanumeric)
                .take(32)
                .map(char::from)
                .collect();

            let occ: usize = rng.gen_range(1..=max_occ);
            strings.extend(std::iter::repeat(rand_string).take(occ));
        }

        strings
    }

    // Whether obtained = expected +/- relative_error
    fn is_close(obtained: f64, expected: f64, relative_error: f64) -> bool {
        let margin = expected * relative_error;
        ((expected - margin) < obtained) && (obtained < (expected + margin))
    }

    #[test]
    fn hll_big() {
        let precision = 12;
        let mut hll = HyperLogLog::new(precision);
        let n_distinct = 100000;
        let relative_error = 0.05; // We allow a 5% relatative error rate.

        let strings = generate_random_strings(n_distinct, 100, 0);
        hll.aggregate(strings.iter());

        assert!(is_close(
            hll.n_distinct() as f64,
            n_distinct as f64,
            relative_error
        ));
    }

    #[test]
    fn hll_massive_parallel() {
        let precision = 12;
        let n_distinct = 100000;
        let n_jobs = 16;
        let relative_error = 0.05; // We allow a 5% relatative error rate.

        let result_hll = Arc::new(Mutex::new(HyperLogLog::new(precision)));
        let job_id = AtomicUsize::new(0);
        thread::scope(|s| {
            for _ in 0..n_jobs {
                s.spawn(|_| {
                    let mut local_hll = HyperLogLog::new(precision);
                    let curr_job_id = job_id.fetch_add(1, Ordering::SeqCst);

                    let strings = generate_random_strings(n_distinct, 100, curr_job_id);
                    local_hll.aggregate(strings.iter());

                    assert!(is_close(
                        local_hll.n_distinct() as f64,
                        n_distinct as f64,
                        relative_error
                    ));

                    let mut result = result_hll.lock().unwrap();
                    result.merge(&local_hll);
                });
            }
        })
        .unwrap();

        let hll = result_hll.lock().unwrap();
        assert!(is_close(
            hll.n_distinct() as f64,
            (n_distinct * n_jobs) as f64,
            relative_error
        ));
    }
}
