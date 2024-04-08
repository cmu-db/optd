//! A hash-based Counter implementation that will track exact frequencies for
//! an array of prespecified elements.

use std::{collections::HashMap, hash::Hash};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// The Counter structure to track exact frequencies of fixed elements.
#[serde_with::serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct Counter<T: PartialEq + Eq + Hash + Clone + Serialize + DeserializeOwned> {
    #[serde_as(as = "HashMap<serde_with::json::JsonString, _>")]
    counts: HashMap<T, i32>, // The exact counts of an element T.
    total_count: i32, // The total number of elements.
}

// Self-contained implementation of the Counter data structure.
impl<T> Counter<T>
where
    T: PartialEq + Eq + Hash + Clone + Serialize + DeserializeOwned,
{
    /// Creates and initializes a new empty Counter with the frequency map sized
    /// based on the number of unique elements in `to_track`.
    pub fn new(to_track: &[T]) -> Self {
        let mut counts: HashMap<T, i32> = HashMap::with_capacity(to_track.len());
        for item in to_track {
            counts.insert(item.clone(), 0);
        }

        Counter::<T> {
            counts,
            total_count: 0,
        }
    }

    // Inserts an element in the Counter if it is being tracked.
    pub fn insert_element(&mut self, elem: T, occ: i32) {
        if let Some(frequency) = self.counts.get_mut(&elem) {
            *frequency += occ;
        }
    }

    /// Digests an array of data into the Counter structure.
    pub fn aggregate(&mut self, data: &[T]) {
        data.iter()
            .for_each(|key| self.insert_element(key.clone(), 1));
        self.total_count += data.len() as i32;
    }

    /// Merges another Counter into the current one.
    /// Particularly useful for parallel execution.
    pub fn merge(&mut self, other: &Counter<T>) {
        other
            .counts
            .iter()
            .for_each(|(key, occ)| self.insert_element(key.clone(), *occ));
        self.total_count += other.total_count;
    }

    /// Returns the frequencies of the most common values.
    pub fn frequencies(&self) -> HashMap<T, f64> {
        self.counts
            .iter()
            .map(|(key, &value)| (key.clone(), value as f64 / self.total_count as f64))
            .collect()
    }
}

// Start of unit testing section.
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crossbeam::thread;
    use rand::seq::SliceRandom;
    use rand::{rngs::StdRng, SeedableRng};

    use super::Counter;

    // Generates hardcoded frequencies and returns them,
    // along with a flattened randomized array containing those frequencies.
    fn generate_frequencies() -> (HashMap<i32, i32>, Vec<i32>) {
        let mut frequencies = std::collections::HashMap::new();

        frequencies.insert(0, 2);
        frequencies.insert(1, 4);
        frequencies.insert(2, 9);
        frequencies.insert(3, 8);
        frequencies.insert(4, 50);
        frequencies.insert(5, 6);

        let mut flattened = Vec::new();
        for (key, &value) in &frequencies {
            for _ in 0..value {
                flattened.push(*key);
            }
        }

        let mut rng = StdRng::seed_from_u64(0);
        flattened.shuffle(&mut rng);

        (frequencies, flattened)
    }

    #[test]
    fn aggregate() {
        let to_track = vec![0, 1, 2, 3];
        let mut mcv = Counter::<i32>::new(&to_track);

        let (frequencies, flattened) = generate_frequencies();

        mcv.aggregate(&flattened);

        let mcv_freq = mcv.frequencies();
        assert_eq!(mcv_freq.len(), to_track.len());

        to_track.iter().for_each(|item| {
            assert!(mcv_freq.contains_key(item));
            assert_eq!(
                mcv_freq.get(item),
                frequencies
                    .get(item)
                    .map(|e| (*e as f64 / flattened.len() as f64))
                    .as_ref()
            );
        });
    }

    #[test]
    fn merge() {
        let to_track = vec![0, 1, 2, 3];
        let n_jobs = 16;

        let total_frequencies = Arc::new(Mutex::new(HashMap::<i32, i32>::new()));
        let total_count = Arc::new(Mutex::new(0));
        let result_mcv = Arc::new(Mutex::new(Counter::<i32>::new(&to_track)));
        thread::scope(|s| {
            for _ in 0..n_jobs {
                s.spawn(|_| {
                    let mut local_mcv = Counter::<i32>::new(&to_track);

                    let (local_frequencies, flattened) = generate_frequencies();
                    let mut total_frequencies = total_frequencies.lock().unwrap();
                    let mut total_count = total_count.lock().unwrap();
                    for (&key, &value) in &local_frequencies {
                        *total_frequencies.entry(key).or_insert(0) += value;
                        *total_count += value;
                    }

                    local_mcv.aggregate(&flattened);

                    let mcv_local_freq = local_mcv.frequencies();
                    assert_eq!(mcv_local_freq.len(), to_track.len());

                    to_track.iter().for_each(|item| {
                        assert!(mcv_local_freq.contains_key(item));
                        assert_eq!(
                            mcv_local_freq.get(item),
                            local_frequencies
                                .get(item)
                                .map(|e| (*e as f64 / flattened.len() as f64))
                                .as_ref()
                        );
                    });

                    let mut result = result_mcv.lock().unwrap();
                    result.merge(&local_mcv);
                });
            }
        })
        .unwrap();

        let mcv = result_mcv.lock().unwrap();
        let total_count = total_count.lock().unwrap();
        let mcv_freq = mcv.frequencies();

        assert_eq!(*total_count, mcv.total_count);
        to_track.iter().for_each(|item| {
            assert!(mcv_freq.contains_key(item));
            assert_eq!(
                mcv_freq.get(item),
                total_frequencies
                    .lock()
                    .unwrap()
                    .get(item)
                    .map(|e| (*e as f64 / *total_count as f64))
                    .as_ref()
            );
        });
    }
}
