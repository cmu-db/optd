// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Implementation of the Misra-Gries Summary data structure as described in  
//! the Cormode et al. paper: "Misra-Gries Summaries" (2014).
//! We further refine the algorithm to ensure that K elements will always be
//! returned, as long as K <= N.
//! For more details, refer to:
//! https://people.csail.mit.edu/rrw/6.045-2017/encalgs-mg.pdf

use std::cmp::min;
use std::hash::Hash;

use hashbrown::HashMap;
use itertools::Itertools;

pub const DEFAULT_K_TO_TRACK: u16 = 200;

/// The Misra-Gries structure to approximate the k most frequent elements in
/// a stream of N elements. It will always identify elements with frequency
/// f >= (n/k), and include additional leftovers.
#[derive(Clone)]
pub struct MisraGries<T: PartialEq + Eq + Hash + Clone> {
    frequencies: HashMap<T, i32>, // The approximated frequencies of an element T.
    k: u16,                       // The max size of our frequencies hashmap.

    least_frequent: Option<(T, i32)>, // The least frequent element in frequencies.
}

// Self-contained implementation of the Misra-Gries data structure.
impl<'a, T> MisraGries<T>
where
    T: PartialEq + Eq + Hash + Clone + 'a,
{
    /// Creates and initializes a new empty Misra-Gries.
    pub fn new(k: u16) -> Self {
        assert!(k > 0);

        MisraGries::<T> {
            frequencies: HashMap::with_capacity(k as usize),
            k,

            least_frequent: None,
        }
    }

    // Returns the (key, val) pair of the least frequent element.
    // If more than one such element exists, returns an arbitrary one.
    // NOTE: Panics if no frequencies exist.
    fn find_least_frequent(&self) -> (T, i32) {
        let (key, occ) = self.frequencies.iter().min_by_key(|(_, occ)| *occ).unwrap();
        (key.clone(), *occ)
    }

    // Inserts an element occ times into the `self` Misra-Gries structure.
    pub fn insert_element(&mut self, elem: &T, occ: i32) {
        match self.frequencies.get_mut(elem) {
            Some(freq) => {
                *freq += occ; // Hit.
                if *elem == self.least_frequent.as_ref().unwrap().0 {
                    self.least_frequent = Some(self.find_least_frequent());
                }
            }
            None => {
                if self.frequencies.len() < self.k as usize {
                    self.frequencies.insert(elem.clone(), occ); // Discovery phase.

                    match &self.least_frequent {
                        Some(prev) => {
                            if prev.1 > occ {
                                self.least_frequent = Some((elem.clone(), occ));
                            }
                        }
                        None => self.least_frequent = Some((elem.clone(), occ)),
                    }
                } else {
                    let smallest_freq = self.least_frequent.as_ref().unwrap().1;

                    let decr = min(smallest_freq, occ);
                    if decr > 0 {
                        for freq in self.frequencies.values_mut() {
                            *freq -= decr;
                        }
                        self.least_frequent.as_mut().unwrap().1 -= decr;
                    }

                    let delta = smallest_freq - occ;
                    if delta < 0 {
                        self.frequencies
                            .remove(&self.least_frequent.as_ref().unwrap().0);
                        self.frequencies.insert(elem.clone(), -delta);

                        self.least_frequent = Some(self.find_least_frequent());
                    }
                }
            }
        }
    }

    /// Digests an array of data into the Misra-Gries structure.
    pub fn aggregate<I>(&mut self, data: I)
    where
        I: Iterator<Item = &'a T>,
    {
        data.for_each(|key| self.insert_element(key, 1));
    }

    /// Merges another MisraGries into the current one.
    /// Particularly useful for parallel execution.
    pub fn merge(&mut self, other: &MisraGries<T>) {
        assert!(self.k == other.k);

        other
            .frequencies
            .iter()
            .for_each(|(key, occ)| self.insert_element(key, *occ));
    }

    /// Returns all elements with frequency f >= (n/k),
    /// and may include additional leftovers.
    pub fn most_frequent_keys(&self) -> Vec<&T> {
        self.frequencies.keys().collect_vec()
    }
}

// Start of unit testing section.
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use crossbeam::thread;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;

    use super::MisraGries;

    #[test]
    fn aggregate_simple() {
        let data = [0, 1, 2, 3];
        let mut misra_gries = MisraGries::<i32>::new(data.len() as u16);

        misra_gries.aggregate(data.iter());

        for key in misra_gries.most_frequent_keys() {
            assert!(data.contains(key));
        }
    }

    #[test]
    fn aggregate_double() {
        let data = [0, 1, 2, 3];
        let data_dup = [data.as_slice(), data.as_slice()].concat();

        let mut misra_gries = MisraGries::<i32>::new(data.len() as u16);

        misra_gries.aggregate(data_dup.iter());

        for key in misra_gries.most_frequent_keys() {
            assert!(data.contains(key));
        }
    }

    // Generates a shuffled array of n distinct elements following a Zipfian
    // distribution based on the provided seed.
    fn create_zipfian(n_distinct: i32, seed: u64) -> Vec<i32> {
        let mut data = Vec::<i32>::new();
        for idx in 1..=n_distinct {
            let occurance = n_distinct / idx;
            for _ in 0..occurance {
                data.push(idx);
            }
        }
        let mut rng = StdRng::seed_from_u64(seed);
        data.shuffle(&mut rng);

        data
    }

    // Verifies the ability of Misra-Gries in identifying the most frequent elements
    // in a dataset following a Zipfian distribution.
    fn check_zipfian(misra_gries: &MisraGries<i32>, n_distinct: i32) {
        let mfk = misra_gries.most_frequent_keys();
        let k = misra_gries.k as i32;
        let total_length: i32 = (1..=n_distinct).map(|idx| n_distinct / idx).sum();

        assert!((1..=n_distinct)
            .filter(|idx| (n_distinct / idx) * k >= total_length)
            .all(|idx| mfk.contains(&&idx)));
        assert!(mfk.len() == (k as usize));
    }

    #[test]
    fn aggregate_zipfian() {
        let n_distinct = 10000;
        let k = 200;

        let data = create_zipfian(n_distinct, 0);
        let mut misra_gries = MisraGries::<i32>::new(k as u16);

        misra_gries.aggregate(data.iter());

        check_zipfian(&misra_gries, n_distinct);
    }

    #[test]
    fn merge_zipfians() {
        let n_distinct = 10000;
        let n_jobs = 16;
        let k = 200;

        let result_misra_gries = Arc::new(Mutex::new(MisraGries::<i32>::new(k as u16)));
        let job_id = AtomicUsize::new(0);
        thread::scope(|s| {
            for _ in 0..n_jobs {
                s.spawn(|_| {
                    let mut local_misra_gries = MisraGries::<i32>::new(k as u16);
                    let curr_job_id = job_id.fetch_add(1, Ordering::SeqCst);

                    let data = create_zipfian(n_distinct, curr_job_id as u64);
                    local_misra_gries.aggregate(data.iter());

                    check_zipfian(&local_misra_gries, n_distinct);

                    let mut result = result_misra_gries.lock().unwrap();
                    result.merge(&local_misra_gries);
                });
            }
        })
        .unwrap();

        check_zipfian(&result_misra_gries.lock().unwrap(), n_distinct);
    }
}
