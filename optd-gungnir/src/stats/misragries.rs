//! Implementation of the Misra-Gries Summary data structure as described in  
//! the Cormode et al. paper: "Misra-Gries Summaries" (2014).
//! For more details, refer to:
//! https://people.csail.mit.edu/rrw/6.045-2017/encalgs-mg.pdf

use std::{collections::HashMap, hash::Hash};

/// The Misra-Gries structure to approximate the k most frequent elements in
/// a stream of N elements. It will always identify elements with frequency
/// f >= (n/k), and include additional leftovers.
pub struct MisraGries<T: PartialEq + Eq + Hash + Clone> {
    frequencies: HashMap<T, i32>, // The approximated frequencies of an element T.
    k: u16,                       // The max size of our frequencies hashmap.
}

// Self-contained implementation of the Misra-Gries data structure.
impl<T> MisraGries<T>
where
    T: PartialEq + Eq + Hash + Clone,
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
}

// Start of unit testing section.
#[cfg(test)]
mod tests {}
