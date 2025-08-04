//! A custom union-find implementation.

use std::{collections::HashMap, hash::Hash};

/// A specialized Union-Find data structure for tracking representatives.
///
/// Implements union-find with path compression for O(log n) amortized time complexity.
pub struct UnionFind<T> {
    parents: HashMap<T, T>,
}

impl<T: Eq + Hash + Clone> UnionFind<T> {
    /// Finds the representative of the set containing `x`
    /// If `x` doesn't exist, returns `x` as its own representative
    ///
    /// This is a non-mutating find operation that does not perform path compression
    pub fn find(&self, x: &T) -> T {
        match self.parents.get(x) {
            Some(parent) if parent == x => parent.clone(),
            Some(parent) => self.find(parent),
            None => x.clone(),
        }
    }

    /// Merges the sets containing `dest` and `src`, forcing `dest` to be the representative
    /// If either element doesn't exist, it's automatically added
    ///
    /// This operation performs path compression during the merge
    pub fn merge(&mut self, into: &T, from: &T) -> T {
        // Ensure both elements exist
        self.parents.entry(into.clone()).or_insert(into.clone());
        self.parents.entry(from.clone()).or_insert(from.clone());

        // Find roots of both elements (without compression)
        let old_root = self.find(into);
        let new_root = self.find(from);

        if old_root != new_root {
            // Always make old_root the representative
            self.parents.insert(new_root, old_root.clone());

            // Apply path compression to both sides
            self.find_with_compression(into);
            self.find_with_compression(from);
        }

        old_root
    }

    /// Internal method that finds the representative with path compression
    pub fn find_with_compression(&mut self, x: &T) -> T {
        let parent = self.parents.get(x).cloned().unwrap_or(x.clone());

        if &parent == x {
            return parent;
        }

        // Path compression
        let representative = self.find_with_compression(&parent);
        self.parents.insert(x.clone(), representative.clone());
        representative
    }
}

impl<T> Default for UnionFind<T> {
    fn default() -> Self {
        UnionFind {
            parents: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_nonexistent() {
        let repr = UnionFind::<u32>::default();
        assert_eq!(repr.find(&42), 42);
    }

    #[test]
    fn find_self() {
        let mut repr = UnionFind::<u32>::default();
        repr.parents.insert(42, 42);
        assert_eq!(repr.find(&42), 42);
    }

    #[test]
    fn find_without_compression() {
        let mut repr = UnionFind::<u32>::default();
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
    fn merge_with_compression() {
        let mut repr = UnionFind::<u32>::default();
        repr.parents.insert(1, 1);
        repr.parents.insert(2, 1);
        repr.parents.insert(3, 2);
        repr.parents.insert(4, 3);
        repr.parents.insert(5, 4);

        // Merge compresses paths
        repr.merge(&1, &6);

        // Now 6 should point directly to 1
        assert_eq!(repr.parents.get(&6), Some(&1));
    }

    #[test]
    fn merge_basic() {
        let mut repr = UnionFind::<u32>::default();
        let result = repr.merge(&1, &2);
        assert_eq!(result, 1);
        assert_eq!(repr.find(&2), 1);
    }

    #[test]
    fn merge_existing() {
        let mut repr = UnionFind::<u32>::default();
        repr.parents.insert(1, 1);
        repr.parents.insert(2, 2);

        let result = repr.merge(&1, &2);
        assert_eq!(result, 1);
        assert_eq!(repr.find(&2), 1);
    }

    #[test]
    fn merge_already_merged() {
        let mut repr = UnionFind::<u32>::default();
        repr.parents.insert(1, 2);
        repr.parents.insert(2, 2);

        let result = repr.merge(&1, &2);
        assert_eq!(result, 2);
        assert_eq!(repr.find(&1), 2);
    }

    #[test]
    fn merge_chains() {
        let mut repr = UnionFind::<u32>::default();

        // Create chain 1->2->3
        repr.merge(&2, &3);
        repr.merge(&1, &2);
        assert_eq!(repr.find(&3), 1);

        // Create chain 4->5->6
        repr.merge(&5, &6);
        repr.merge(&4, &5);
        assert_eq!(repr.find(&6), 4);

        // Merge the chains, making 1 the representative
        repr.merge(&1, &4);

        // All should point to 1 now
        assert_eq!(repr.find(&1), 1);
        assert_eq!(repr.find(&2), 1);
        assert_eq!(repr.find(&3), 1);
        assert_eq!(repr.find(&4), 1);
        assert_eq!(repr.find(&5), 1);
    }

    #[test]
    fn merge_with_string_keys() {
        let mut repr = UnionFind::<String>::default();

        let result = repr.merge(&"old".to_string(), &"new".to_string());
        assert_eq!(result, "old");
        assert_eq!(repr.find(&"new".to_string()), "old".to_string());
    }

    #[test]
    fn complex_merges() {
        let mut repr = UnionFind::<u32>::default();

        // First set of merges
        repr.merge(&1, &2);
        repr.merge(&3, &4);
        repr.merge(&5, &6);

        // Second set of merges
        repr.merge(&1, &3);
        repr.merge(&1, &5);

        // All should now point to 1
        for i in 1..=6 {
            assert_eq!(
                repr.find(&i),
                1,
                "Element {} should have representative 4",
                i
            );
        }

        // Another element joins
        repr.merge(&7, &8);
        assert_eq!(repr.find(&8), 7);

        // Connect the two trees
        repr.merge(&4, &8);

        // Now all should point to 1
        for i in 1..=8 {
            assert_eq!(
                repr.find(&i),
                1,
                "After final merge, element {} should have representative 8",
                i
            );
        }
    }
}
