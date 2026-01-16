//! HyperLogLog for number of distinct value approximation.
//!
//! Based on "New cardinality estimation algorithms for HyperLogLog sketches",
//! Otmar Ertl, https://arxiv.org/pdf/1702.01284.

use std::{
    borrow::Borrow,
    hash::{BuildHasher, BuildHasherDefault, Hash},
    marker::PhantomData,
};

use crate::utils::Murmur2Hash64a;

pub struct HyperLogLog<K, S = BuildHasherDefault<Murmur2Hash64a>, const P: usize = 12> {
    hash_builder: S,
    k: Box<[u8]>,
    _phantom: PhantomData<K>,
}

impl<K, S, const P: usize> HyperLogLog<K, S, P> {
    const Q: usize = 64 - P;
    const M: usize = 1 << P;
    const ALPHA: f64 = 0.721_347_520_444_481_7; // 1 / (2 log(2))

    /// Insert a hash value into the HyperLogLog (Algorithm 1)
    fn insert_hash(&mut self, mut h: u64) {
        // Extract register index from lower P bits
        let i = (h & ((1 << P) - 1)) as usize;

        // Shift hash right by P bits
        h >>= P;

        // Set bit Q to 1
        h |= 1u64 << Self::Q;

        // Count trailing zeros + 1
        let z = (h.trailing_zeros() + 1) as u8;

        // Update register
        self.update(i, z);
    }

    /// Update register i to max(k[i], z)
    #[inline]
    fn update(&mut self, i: usize, z: u8) {
        self.k[i] = self.k[i].max(z);
    }

    /// Extract counts for cardinality estimation (Algorithm 4)
    fn extract_counts(&self) -> Vec<u32> {
        let mut c = vec![0u32; Self::Q + 2];
        for &register in self.k.iter() {
            c[register as usize] += 1;
        }
        c
    }

    /// HLLSigma helper function (from Redis code)
    fn hll_sigma(mut x: f64) -> f64 {
        if x == 1.0 {
            return f64::INFINITY;
        }
        let mut z_prime;
        let mut y = 1.0;
        let mut z = x;
        loop {
            x *= x;
            z_prime = z;
            z += x * y;
            y += y;
            if z_prime == z {
                break;
            }
        }
        z
    }

    /// HLLTau helper function (from Redis code)
    fn hll_tau(mut x: f64) -> f64 {
        if x == 0.0 || x == 1.0 {
            return 0.0;
        }
        let mut z_prime;
        let mut y = 1.0;
        let mut z = 1.0 - x;
        loop {
            x = x.sqrt();
            z_prime = z;
            y *= 0.5;
            z -= (1.0 - x).powi(2) * y;
            if z_prime == z {
                break;
            }
        }
        z / 3.0
    }

    /// Estimate cardinality (Algorithm 6)
    fn estimate_cardinality(c: &[u32]) -> i64 {
        let m = Self::M as f64;
        let q = Self::Q;

        let mut z = m * Self::hll_tau((m - c[q] as f64) / m);

        for k in (1..=q).rev() {
            z += c[k] as f64;
            z *= 0.5;
        }

        z += m * Self::hll_sigma(c[0] as f64 / m);

        (Self::ALPHA * m * m / z).round() as i64
    }
}

impl<K> HyperLogLog<K> {
    pub fn new() -> Self {
        Self::with_hasher(BuildHasherDefault::new())
    }
}

impl<K> Default for HyperLogLog<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, S, const P: usize> HyperLogLog<K, S, P> {
    pub fn with_hasher(hash_builder: S) -> Self {
        let m = 1 << P;
        Self {
            hash_builder,
            k: vec![0u8; m].into_boxed_slice(),
            _phantom: PhantomData,
        }
    }
}

impl<K, S, const P: usize> HyperLogLog<K, S, P>
where
    K: Hash,
    S: BuildHasher,
{
    pub fn insert<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);
        self.insert_hash(hash);
    }

    /// Merge another HyperLogLog into this one (Algorithm 2)
    pub fn merge(&mut self, other: &Self) {
        for i in 0..Self::M {
            self.update(i, other.k[i]);
        }
    }

    /// Get the approximate distinct count
    pub fn approx_distinct(&self) -> usize {
        let c = self.extract_counts();
        let estimate = Self::estimate_cardinality(&c);
        estimate.max(0) as usize
    }
}

impl<K, S, const P: usize> Extend<K> for HyperLogLog<K, S, P>
where
    K: Hash,
    S: BuildHasher,
{
    fn extend<T: IntoIterator<Item = K>>(&mut self, iter: T) {
        for x in iter {
            let hash = self.hash_builder.hash_one(&x);
            self.insert_hash(hash);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use rand::distr::Alphanumeric;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[test]
    fn hll_small_strings() {
        let mut hll = HyperLogLog::new();
        let data = ["a".to_string(), "b".to_string()];
        hll.extend(data.iter().cloned());
        assert_eq!(hll.approx_distinct(), data.len());
    }

    #[test]
    fn hll_small_u64() {
        let mut hll = HyperLogLog::new();
        let data = [1, 2];
        hll.extend(data.iter().copied());
        assert_eq!(hll.approx_distinct(), data.len());
    }

    // Generates n random 32-char length strings that have an occurrence sampled from
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
            let occ: usize = rng.random_range(1..=max_occ);
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
        let mut hll = HyperLogLog::new();
        let n_distinct = 100000;
        let relative_error = 0.05; // We allow a 5% relative error rate.
        let strings = generate_random_strings(n_distinct, 100, 0);
        hll.extend(strings.into_iter());
        assert!(is_close(
            hll.approx_distinct() as f64,
            n_distinct as f64,
            relative_error
        ));
    }

    #[test]
    fn hll_massive_parallel() {
        let n_distinct = 100000;
        let n_jobs = 16;
        let relative_error = 0.05; // We allow a 5% relative error rate.
        let result_hll = Arc::new(Mutex::new(HyperLogLog::new()));
        let job_id = AtomicUsize::new(0);

        std::thread::scope(|s| {
            for _ in 0..n_jobs {
                let result_hll = Arc::clone(&result_hll);
                let job_id = &job_id;
                s.spawn(move || {
                    let mut local_hll = HyperLogLog::new();
                    let curr_job_id = job_id.fetch_add(1, Ordering::SeqCst);
                    let strings = generate_random_strings(n_distinct, 100, curr_job_id);
                    local_hll.extend(strings.into_iter());
                    assert!(
                        is_close(
                            local_hll.approx_distinct() as f64,
                            n_distinct as f64,
                            relative_error
                        ),
                        "approx = {}, expectd = {n_distinct}",
                        local_hll.approx_distinct(),
                    );
                    let mut result = result_hll.lock().unwrap();
                    result.merge(&local_hll);
                });
            }
        });

        let hll = result_hll.lock().unwrap();
        assert!(is_close(
            hll.approx_distinct() as f64,
            (n_distinct * n_jobs) as f64,
            relative_error
        ));
    }

    #[test]
    fn validate_approx_distinct() {
        let mut hll: HyperLogLog<i32> = HyperLogLog::new();
        (0..10000).for_each(|i| {
            if i % 2 == 0 {
                hll.insert(&i)
            }
        });
        println!("approx = {}, expected = 5000", hll.approx_distinct())
    }
}
