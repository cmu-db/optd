//! Implementation of the Misra-Gries Summary data structure as described in  
//! the Cormode et al. paper: "Misra-Gries Summaries" (2014).
//! For more details, refer to:
//! https://people.csail.mit.edu/rrw/6.045-2017/encalgs-mg.pdf

use std::{collections::HashMap, fmt::Debug, hash::Hash};

use itertools::Itertools;

/// The Misra-Gries structure to approximate the k most frequent elements in
/// a stream of N elements. It will always identify elements with frequency
/// f >= (n/k), and include additional leftovers.
#[derive(Debug)]
pub struct MisraGries<T: PartialEq + Eq + Hash + Clone + Debug> {
    frequencies: HashMap<T, i32>, // The approximated frequencies of an element T.
    k: u16,                       // The max size of our frequencies hashmap.
}

// Self-contained implementation of the Misra-Gries data structure.
impl<T> MisraGries<T>
where
    T: PartialEq + Eq + Hash + Clone + Debug,
{
    /// Creates and initializes a new empty Misra-Gries.
    pub fn new(k: u16) -> Self {
        assert!(k > 0);

        MisraGries::<T> {
            frequencies: HashMap::with_capacity(k as usize),
            k,
        }
    }

    // Inserts an element occ times into the `self` Misra-Gries structure.
    fn insert_element(&mut self, elem: T, occ: i32) {
        match self.frequencies.get_mut(&elem) {
            Some(freq) => *freq += occ,
            None => {
                if self.frequencies.len() < self.k as usize {
                    self.frequencies.insert(elem, occ);
                } else {
                    for freq in self.frequencies.values_mut() {
                        *freq -= occ;
                    }

                    let smallest_freq = self.frequencies.values().min().copied().unwrap();
                    let keys_to_remove: Vec<T> = self
                        .frequencies
                        .iter()
                        .filter(|&(_, &freq)| freq <= 0)
                        .map(|(key, _)| key.clone())
                        .collect();

                    for key in keys_to_remove {
                        self.frequencies.remove(&key);
                    }

                    if smallest_freq < 0 {
                        self.frequencies.insert(elem, -smallest_freq);
                    }
                }
            }
        }
    }

    /// Digests an array of data into the Misra-Gries structure.
    pub fn aggregate(&mut self, data: &[T]) {
        data.iter()
            .for_each(|key| self.insert_element(key.clone(), 1));
    }

    /// Merges another MisraGries into the current one.
    /// Particularly useful for parallel execution.
    pub fn merge(&mut self, other: &MisraGries<T>) {
        assert!(self.k == other.k);

        other
            .frequencies
            .iter()
            .for_each(|(key, occ)| self.insert_element(key.clone(), *occ));
    }

    /// Returns an estimate of the K most frequent keys.
    /// Returns less than K results if less than N distinct
    /// values have been encountered so far.
    pub fn most_frequent_keys(&self) -> Vec<&T> {
        self.frequencies.keys().collect_vec()
    }
}

// Start of unit testing section.
#[cfg(test)]
mod tests {
    use super::MisraGries;
    use rand::seq::SliceRandom;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn aggregate_full_size() {
        let data = vec![0, 1, 2, 3];
        let mut misra_gries = MisraGries::<i32>::new(data.len() as u16);

        misra_gries.aggregate(&data);

        for key in misra_gries.most_frequent_keys() {
            assert!(data.contains(key));
        }
    }

    #[test]
    fn aggregate_half_size() {
        let data = vec![0, 1, 2, 3];
        let data_dup = [data.as_slice(), data.as_slice()].concat();

        let mut misra_gries = MisraGries::<i32>::new(data.len() as u16);

        misra_gries.aggregate(&data_dup);

        for key in misra_gries.most_frequent_keys() {
            assert!(data.contains(key));
        }
    }

    #[test]
    fn aggregate_zipfian() {
        // TODO(Alexis): Cleanup below & handle only 4 but have room for 16.
        let n_distinct = 1000;
        let mut data = Vec::<i32>::new();

        for idx in 1..=n_distinct {
            let occurance = n_distinct / idx;
            for _ in 0..occurance {
                data.push(idx);
            }
        }

        let mut rng = StdRng::seed_from_u64(0);
        data.shuffle(&mut rng);

        let n = data.len();
        let mut misra_gries = MisraGries::<i32>::new(16);

        misra_gries.aggregate(&data);

        println!("{:#?}", misra_gries);
        println!("{}", n);
    }
}
