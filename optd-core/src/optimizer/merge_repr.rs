use std::collections::HashMap;
use std::hash::Hash;

/// A specialized Union-Find data structure for tracking group or goal representatives
/// caused by merges
///
/// Implements union-find with path compression for O(α(n)) amortized time complexity
pub struct Representative<T> {
    parents: HashMap<T, T>,
}

impl<T: Eq + Hash + Clone> Representative<T> {
    /// Creates a new empty Representative
    pub(super) fn new() -> Self {
        Self {
            parents: HashMap::new(),
        }
    }

    /// Finds the representative of the set containing `x`
    /// If `x` doesn't exist, returns `x` as its own representative
    ///
    /// This is a non-mutating find operation that does not perform path compression
    pub(super) fn find(&self, x: &T) -> T {
        match self.parents.get(x) {
            Some(parent) if parent == x => parent.clone(),
            Some(parent) => self.find(parent),
            None => x.clone(),
        }
    }

    /// Merges the sets containing `old` and `new`, forcing `new` to be the representative
    /// If either element doesn't exist, it's automatically added
    ///
    /// This operation performs path compression during the merge
    pub(super) fn merge(&mut self, old: &T, new: &T) -> T {
        // Ensure both elements exist
        self.parents.entry(old.clone()).or_insert(old.clone());
        self.parents.entry(new.clone()).or_insert(new.clone());

        // Find roots of both elements (without compression)
        let old_root = self.find(old);
        let new_root = self.find(new);

        if old_root != new_root {
            // Always make new_root the representative
            self.parents.insert(old_root, new_root.clone());

            // Apply path compression to both sides
            self.find_with_compression(old);
            self.find_with_compression(new);
        }

        new_root
    }

    /// Internal method that finds the representative with path compression
    fn find_with_compression(&mut self, x: &T) -> T {
        let parent = self.parents.get(x).cloned().unwrap_or(x.clone());

        if &parent == x {
            return parent;
        }

        // Path compression
        let root = self.find_with_compression(&parent);
        self.parents.insert(x.clone(), root.clone());
        root
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_nonexistent() {
        let repr = Representative::<u32>::new();
        assert_eq!(repr.find(&42), 42);
    }

    #[test]
    fn test_find_self() {
        let mut repr = Representative::<u32>::new();
        repr.parents.insert(42, 42);
        assert_eq!(repr.find(&42), 42);
    }

    #[test]
    fn test_find_without_compression() {
        let mut repr = Representative::<u32>::new();
        repr.parents.insert(1, 2);
        repr.parents.insert(2, 3);
        repr.parents.insert(3, 4);
        repr.parents.insert(4, 5);
        repr.parents.insert(5, 5);

        // Finding 1 should return 5 but not compress the path
        assert_eq!(repr.find(&1), 5);

        // The parent of 1 should still be 2
        assert_eq!(repr.parents.get(&1), Some(&2));
    }

    #[test]
    fn test_merge_with_compression() {
        let mut repr = Representative::<u32>::new();
        repr.parents.insert(1, 2);
        repr.parents.insert(2, 3);
        repr.parents.insert(3, 4);
        repr.parents.insert(4, 5);
        repr.parents.insert(5, 5);

        // Merge compresses paths
        repr.merge(&1, &6);

        // Now 1 should point directly to 6
        assert_eq!(repr.parents.get(&1), Some(&6));
    }

    #[test]
    fn test_merge_basic() {
        let mut repr = Representative::<u32>::new();
        let result = repr.merge(&1, &2);
        assert_eq!(result, 2);
        assert_eq!(repr.find(&1), 2);
    }

    #[test]
    fn test_merge_existing() {
        let mut repr = Representative::<u32>::new();
        repr.parents.insert(1, 1);
        repr.parents.insert(2, 2);

        let result = repr.merge(&1, &2);
        assert_eq!(result, 2);
        assert_eq!(repr.find(&1), 2);
    }

    #[test]
    fn test_merge_already_merged() {
        let mut repr = Representative::<u32>::new();
        repr.parents.insert(1, 2);
        repr.parents.insert(2, 2);

        let result = repr.merge(&1, &2);
        assert_eq!(result, 2);
        assert_eq!(repr.find(&1), 2);
    }

    #[test]
    fn test_merge_chains() {
        let mut repr = Representative::<u32>::new();

        // Create chain 1->2->3
        repr.merge(&1, &2);
        repr.merge(&2, &3);
        assert_eq!(repr.find(&1), 3);

        // Create chain 4->5->6
        repr.merge(&4, &5);
        repr.merge(&5, &6);
        assert_eq!(repr.find(&4), 6);

        // Merge the chains, making 6 the representative
        repr.merge(&3, &6);

        // All should point to 6 now
        assert_eq!(repr.find(&1), 6);
        assert_eq!(repr.find(&2), 6);
        assert_eq!(repr.find(&3), 6);
        assert_eq!(repr.find(&4), 6);
        assert_eq!(repr.find(&5), 6);
    }

    #[test]
    fn test_merge_with_string_keys() {
        let mut repr = Representative::<String>::new();

        let result = repr.merge(&"old".to_string(), &"new".to_string());
        assert_eq!(result, "new");
        assert_eq!(repr.find(&"old".to_string()), "new");
    }

    #[test]
    fn test_complex_merges() {
        let mut repr = Representative::<u32>::new();

        // First set of merges
        repr.merge(&1, &2);
        repr.merge(&3, &4);
        repr.merge(&5, &6);

        // Second set of merges
        repr.merge(&2, &4);
        repr.merge(&6, &4);

        // All should now point to 4
        for i in 1..=6 {
            assert_eq!(
                repr.find(&i),
                4,
                "Element {} should have representative 4",
                i
            );
        }

        // Another element joins
        repr.merge(&7, &8);
        assert_eq!(repr.find(&7), 8);

        // Connect the two trees
        repr.merge(&4, &8);

        // Now all should point to 8
        for i in 1..=8 {
            assert_eq!(
                repr.find(&i),
                8,
                "After final merge, element {} should have representative 8",
                i
            );
        }
    }
}
