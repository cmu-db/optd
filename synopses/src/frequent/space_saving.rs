use std::{collections::HashMap, hash::Hash, ops::RangeInclusive};

use itertools::Itertools;

pub struct SpaceSaving<T> {
    /// Hash map for O(1) lookups of item weights
    items: HashMap<T, usize>,
    /// Maximum number of items to track.
    capacity: usize,
}

impl<T> SpaceSaving<T>
where
    T: Hash + Eq + Clone,
{
    /// Creates a new SpaceSaving summary with some `capacity`.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            items: HashMap::with_capacity(capacity),
        }
    }

    /// Returns the current number of items stored in the sketch.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the capacity of the sketch.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clears all items from the sketch.
    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn update(&mut self, item: T, weight: usize) {
        if let Some(w) = self.items.get_mut(&item) {
            // Item already exists, increment its weight
            *w += weight;
        } else if self.items.len() < self.capacity {
            // Space available, add the item
            self.items.insert(item, weight);
        } else {
            // No space, replace the item with minimum weight
            let (min_item, min_weight) = self.find_min_item();
            self.items.remove(&min_item.clone());
            self.items.insert(item, min_weight + weight);
        }
    }

    pub fn insert(&mut self, item: T) {
        self.update(item, 1);
    }

    pub fn get(&self, item: &T) -> RangeInclusive<usize> {
        let min_weight = self.min_weight();
        match self.items.get(item) {
            // Item is stored: upper bound is stored weight,
            // lower bound is stored weight minus min weight
            Some(weight) => RangeInclusive::new(weight.saturating_sub(min_weight), *weight),
            // Item not stored: upper bound is min weight (could be present but evicted),
            // lower bound is 0
            None => RangeInclusive::new(0, min_weight),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            self.capacity, other.capacity,
            "Cannot merge sketches with different capacities"
        );

        // Add all items from other sketch
        for (item, &weight) in &other.items {
            *self.items.entry(item.clone()).or_insert(0) += weight;
        }

        // Trim to capacity if needed
        if self.items.len() > self.capacity {
            self.trim_to_capacity();
        }
    }

    fn min_weight(&self) -> usize {
        if self.items.len() < self.capacity {
            return 0;
        }
        self.items.values().min().copied().unwrap_or(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&T, &usize)> {
        self.items.iter()
    }

    pub fn most_frequent(&self) -> Vec<(&T, RangeInclusive<usize>)> {
        let min_weight = self.min_weight();
        println!("min_weight is {:?}", min_weight);
        self.items
            .iter()
            .sorted_by_key(|(_, weight)| std::cmp::Reverse(*weight))
            .map(|(item, weight)| {
                (
                    item,
                    RangeInclusive::new(weight.saturating_sub(min_weight), *weight),
                )
            })
            .collect()
    }

    fn trim_to_capacity(&mut self) {
        if self.items.len() <= self.capacity {
            return;
        }

        // Collect and sort by weight (descending)
        let mut items: Vec<_> = self.items.iter().map(|(k, &v)| (k.clone(), v)).collect();
        items.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        // Keep only top k items
        self.items.clear();
        for (item, weight) in items.into_iter().take(self.capacity) {
            self.items.insert(item, weight);
        }
    }

    fn find_min_item(&self) -> (&T, usize) {
        self.items
            .iter()
            .min_by_key(|(_, weight)| *weight)
            .map(|(item, weight)| (item, *weight))
            .expect("items should not be empty when finding min")
    }
}

impl<T> Extend<T> for SpaceSaving<T>
where
    T: Hash + Eq + Clone,
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        iter.into_iter().for_each(|item| self.insert(item));
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_updates_within_capacity() {
        let mut sketch = SpaceSaving::with_capacity(3);

        sketch.update("a", 1);
        sketch.update("b", 1);
        sketch.update("c", 1);

        assert_eq!(sketch.len(), 3);

        // With min_weight = 1, the range for "a" is [1-1, 1] = [0, 1]
        assert_eq!(*sketch.get(&"a").start(), 0);
        assert_eq!(*sketch.get(&"a").end(), 1);
    }

    #[test]
    fn test_update_existing_item() {
        let mut sketch = SpaceSaving::with_capacity(3);

        sketch.update("a", 2);
        sketch.update("a", 3);
        sketch.update("b", 5);
        sketch.update("c", 7);

        assert_eq!(sketch.len(), 3);
        let range = sketch.get(&"a");
        // With capacity filled, min_weight = 5, range is [5-5, 5] = [0, 5]
        assert_eq!(*range.start(), 0);
        assert_eq!(*range.end(), 5);
    }

    #[test]
    fn test_eviction_replaces_minimum() {
        let mut sketch = SpaceSaving::with_capacity(3);

        sketch.update("a", 5);
        sketch.update("b", 3);
        sketch.update("c", 2);

        // Now capacity is full. Adding "d" should evict "c" (min weight = 2)
        sketch.update("d", 1);

        assert_eq!(sketch.len(), 3);
        assert!(sketch.items.contains_key(&"d"));
        assert!(!sketch.items.contains_key(&"c"));
        // "d" should have weight 2 + 1 = 3 (min_weight + new_weight)
        assert_eq!(sketch.items.get(&"d"), Some(&3));

        // Verify bounds for "d"
        let range_d = sketch.get(&"d");
        // min_weight is now 3, so range is [3-3, 3] = [0, 3]
        assert_eq!(*range_d.start(), 0);
        assert_eq!(*range_d.end(), 3);
    }

    #[test]
    fn test_document_example() {
        // Example from the PDF: a, b, a, c, d, e, a, d, f, a, d
        let mut sketch = SpaceSaving::with_capacity(4);

        let sequence = vec!["a", "b", "a", "c", "d", "e", "a", "d", "f", "a", "d"];
        for item in sequence {
            sketch.update(item, 1);
        }

        assert_eq!(sketch.len(), 4);

        // Trace: a=1, b=1, a=2, c=1, (evict b) d=2, (evict c) e=2, a=3, d=3, (evict e) f=3, a=4, d=4
        // Final: a=4, d=4, e=2, f=3 OR similar depending on HashMap iteration order

        // What we can assert with certainty:
        assert!(sketch.items.contains_key(&"a"));
        assert!(sketch.items.contains_key(&"d"));

        // "a" appears 4 times in the sequence
        assert_eq!(sketch.items.get(&"a"), Some(&4));
        // "d" appears 3 times in the sequence
        assert_eq!(sketch.items.get(&"d"), Some(&3));
    }

    #[test]
    fn test_query_stored_item() {
        let mut sketch = SpaceSaving::with_capacity(3);

        sketch.update("a", 5);
        sketch.update("b", 3);
        sketch.update("c", 4);

        let range = sketch.get(&"a");
        // min_weight = 3, so lower bound = 5 - 3 = 2
        assert_eq!(*range.start(), 2);
        assert_eq!(*range.end(), 5);
    }

    #[test]
    fn test_query_non_stored_item() {
        let mut sketch = SpaceSaving::with_capacity(2);

        sketch.update("a", 5);
        sketch.update("b", 3);

        let range = sketch.get(&"c");
        // Item not stored: lower bound = 0, upper bound = min_weight = 3
        assert_eq!(*range.start(), 0);
        assert_eq!(*range.end(), 3);
    }

    #[test]
    fn test_query_empty_sketch() {
        let sketch: SpaceSaving<&str> = SpaceSaving::with_capacity(3);

        let range = sketch.get(&"a");
        assert_eq!(*range.start(), 0);
        assert_eq!(*range.end(), 0);
    }

    #[test]
    fn test_merge_within_capacity() {
        let mut sketch1 = SpaceSaving::with_capacity(4);
        sketch1.update("a", 3);
        sketch1.update("b", 2);

        let mut sketch2 = SpaceSaving::with_capacity(4);
        sketch2.update("b", 1);
        sketch2.update("c", 2);

        sketch1.merge(&sketch2);

        // After merge: a=3, b=3, c=2
        assert_eq!(sketch1.len(), 3);
        assert_eq!(sketch1.items.get(&"a"), Some(&3));
        assert_eq!(sketch1.items.get(&"b"), Some(&3)); // 2 + 1
        assert_eq!(sketch1.items.get(&"c"), Some(&2));
    }

    #[test]
    fn test_merge_exceeds_capacity() {
        let mut sketch1 = SpaceSaving::with_capacity(3);
        sketch1.update("a", 5);
        sketch1.update("b", 3);
        sketch1.update("c", 2);

        let mut sketch2 = SpaceSaving::with_capacity(3);
        sketch2.update("d", 4);
        sketch2.update("e", 1);

        sketch1.merge(&sketch2);

        // After merge, should keep only top 3 by weight
        assert_eq!(sketch1.len(), 3);
        assert!(sketch1.items.contains_key(&"a")); // weight 5
        assert!(sketch1.items.contains_key(&"d")); // weight 4
        // Third should be "b" (weight 3)
        assert!(sketch1.items.contains_key(&"b"));
    }

    #[test]
    #[should_panic(expected = "Cannot merge sketches with different capacities")]
    fn test_merge_different_capacities_panics() {
        let mut sketch1: SpaceSaving<i32> = SpaceSaving::with_capacity(3);
        let sketch2 = SpaceSaving::with_capacity(5);

        sketch1.merge(&sketch2);
    }

    #[test]
    fn test_capacity_one() {
        let mut sketch = SpaceSaving::with_capacity(1);

        sketch.update("a", 3);
        assert_eq!(sketch.len(), 1);
        assert_eq!(sketch.items.get(&"a"), Some(&3));

        // Adding another item should replace "a"
        sketch.update("b", 2);
        assert_eq!(sketch.len(), 1);
        assert!(!sketch.items.contains_key(&"a"));
        assert_eq!(sketch.items.get(&"b"), Some(&5)); // 3 + 2

        // Verify bounds
        let range = sketch.get(&"b");
        assert_eq!(*range.start(), 0); // min_weight = 5, so 5-5 = 0
        assert_eq!(*range.end(), 5);
    }

    #[test]
    fn test_clear() {
        let mut sketch = SpaceSaving::with_capacity(3);
        sketch.update("a", 1);
        sketch.update("b", 2);

        sketch.clear();

        assert_eq!(sketch.len(), 0);
        assert!(sketch.is_empty());
    }

    #[test]
    fn test_multiple_updates_same_weight() {
        let mut sketch = SpaceSaving::with_capacity(3);

        sketch.update("a", 2);
        sketch.update("b", 2);
        sketch.update("c", 2);

        // All have same weight, adding "d" should evict one of them
        sketch.update("d", 1);

        assert_eq!(sketch.len(), 3);
        assert_eq!(sketch.items.get(&"d"), Some(&3)); // 2 + 1

        // Verify one item was evicted and the three remaining have correct weights
        let total_weight: usize = sketch.items.values().sum();
        assert_eq!(total_weight, 2 + 2 + 3); // Two items with weight 2, one with weight 3
    }

    #[test]
    fn test_weighted_updates() {
        let mut sketch = SpaceSaving::with_capacity(3);

        sketch.update("a", 10);
        sketch.update("b", 5);
        sketch.update("c", 7); // Add third item to reach capacity
        sketch.update("a", 15); // Total weight for "a" = 25

        assert_eq!(sketch.items.get(&"a"), Some(&25));

        // Verify bounds
        let range = sketch.get(&"a");
        // min_weight = 5, so range is [25-5, 25] = [20, 25]
        assert_eq!(*range.start(), 20);
        assert_eq!(*range.end(), 25);
    }

    #[test]
    fn test_iter() {
        let mut sketch = SpaceSaving::with_capacity(3);
        sketch.update("a", 3);
        sketch.update("b", 2);
        sketch.update("c", 1);

        let items: HashMap<_, _> = sketch.iter().map(|(k, v)| (*k, *v)).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items.get(&"a"), Some(&3));
        assert_eq!(items.get(&"b"), Some(&2));
        assert_eq!(items.get(&"c"), Some(&1));
    }

    #[test]
    fn test_error_bounds_property() {
        // Test that error is bounded by min_weight
        let mut sketch = SpaceSaving::with_capacity(2);

        sketch.update("a", 10);
        sketch.update("b", 5);
        sketch.update("c", 3); // Evicts "b", "c" gets weight 5+3=8

        let min = sketch.min_weight();

        // For stored items, error = stored_weight - (stored_weight - min) = min
        let range_a = sketch.get(&"a");
        assert_eq!(*range_a.end() - *range_a.start(), min);

        // For non-stored items, upper bound is min_weight
        let range_b = sketch.get(&"b");
        assert_eq!(*range_b.end(), min);
        assert_eq!(*range_b.start(), 0); // Added: verify lower bound
    }

    #[test]
    fn test_frequent_item_never_evicted() {
        let mut sketch = SpaceSaving::with_capacity(2);

        sketch.update("frequent".to_string(), 100);
        sketch.update("rare1".to_string(), 1);

        // Adding many rare items shouldn't evict the frequent one
        for i in 2..10 {
            sketch.update(format!("rare{}", i), 1);
        }

        assert!(sketch.items.contains_key("frequent"));
        assert_eq!(sketch.items.get("frequent"), Some(&100));
    }

    #[test]
    fn extend_simple() {
        let data = [0, 1, 2, 3];
        let mut ss = SpaceSaving::<i32>::with_capacity(data.len());

        ss.extend(data.into_iter());

        for (item, weight) in ss.most_frequent() {
            assert!(data.contains(item));
            assert_eq!(weight.start(), &0);
            assert_eq!(weight.end(), &1);
        }
    }

    #[test]
    fn extend_double() {
        let data = [0, 1, 2, 3];
        let data_dup = [data.as_slice(), data.as_slice()].concat();

        let mut ss = SpaceSaving::<i32>::with_capacity(data.len());

        ss.extend(data_dup.into_iter());

        for (item, count) in ss.most_frequent() {
            assert!(data.contains(item));
            assert_eq!(count.start(), &0);
            assert_eq!(count.end(), &2);
        }
    }

    #[test]
    fn test_bounds_consistency() {
        // Additional test to verify bounds are always consistent
        let mut sketch = SpaceSaving::with_capacity(3);

        sketch.update("x", 10);
        sketch.update("y", 7);
        sketch.update("z", 4);

        // For all stored items, start <= end
        for item in ["x", "y", "z"] {
            let range = sketch.get(&item);
            assert!(
                range.start() <= range.end(),
                "Invalid range for {}: [{}, {}]",
                item,
                range.start(),
                range.end()
            );
        }

        // For non-stored items, bounds should be [0, min_weight]
        let range_w = sketch.get(&"w");
        assert_eq!(*range_w.start(), 0);
        assert_eq!(*range_w.end(), 4); // min_weight
    }
}
