//! A hash-based MCV implementation that will track exact frequencies for
//! an array of prespecified elements.

use std::{collections::HashMap, hash::Hash};

/// The MCV structure to track exact frequencies of fixed elements.
pub struct MCV<T: PartialEq + Eq + Hash + Clone> {
    frequencies: HashMap<T, i32>, // The exact frequencies of an element T.
}

// Self-contained implementation of the MCV data structure.
impl<T> MCV<T>
where
    T: PartialEq + Eq + Hash + Clone,
{
    /// Creates and initializes a new empty MCV with the frequency map sized
    /// based on the number of unique elements in `to_track`.
    pub fn new(to_track: &[T]) -> Self {
        let mut frequencies: HashMap<T, i32> = HashMap::with_capacity(to_track.len());
        for item in to_track {
            frequencies.insert(item.clone(), 0);
        }

        MCV::<T> { frequencies }
    }

    // Inserts an element in the MCV if it is being tracked.
    pub fn insert_element(&mut self, elem: T, occ: i32) {
        if let Some(frequency) = self.frequencies.get_mut(&elem) {
            *frequency += occ;
        }
    }

    /// Digests an array of data into the MCV structure.
    pub fn aggregate(&mut self, data: &[T]) {
        data.iter()
            .for_each(|key| self.insert_element(key.clone(), 1));
    }

    /// Merges another MCV into the current one.
    /// Particularly useful for parallel execution.
    pub fn merge(&mut self, other: &MCV<T>) {
        other
            .frequencies
            .iter()
            .for_each(|(key, occ)| self.insert_element(key.clone(), *occ));
    }

    /// Returns the frequencies of the most common values.
    pub fn frequencies(&self) -> &HashMap<T, i32> {
        &self.frequencies
    }
}

// Start of unit testing section.
#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use rand::{rngs::StdRng, SeedableRng};

    use super::MCV;

    #[test]
    fn aggregate() {
        let to_track = vec![0, 1, 2, 3];
        let mut mcv = MCV::<i32>::new(&to_track);

        let mut frequencies = std::collections::HashMap::new();
        frequencies.insert(0, 2);
        frequencies.insert(1, 4);
        frequencies.insert(2, 9);
        frequencies.insert(3, 8);
        frequencies.insert(4, 50);
        frequencies.insert(5, 6);

        let mut array = Vec::new();
        for (key, &value) in &frequencies {
            for _ in 0..value {
                array.push(*key);
            }
        }

        let mut rng = StdRng::seed_from_u64(0);
        array.shuffle(&mut rng);

        mcv.aggregate(&array);

        let mcv_freq = mcv.frequencies();
        assert_eq!(mcv_freq.len(), to_track.len());

        to_track.iter().for_each(|item| {
            assert!(mcv_freq.contains_key(item));
            assert_eq!(mcv_freq.get(item), frequencies.get(item));
        });
    }
}
