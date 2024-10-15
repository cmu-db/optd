use std::{
    collections::{hash_map, HashMap},
    fmt::Debug,
    hash::Hash,
};

/// A data structure for efficiently maintaining disjoint sets of `T`.
pub struct DisjointSet<T> {
    /// Mapping from node to its parent.
    ///
    /// # Design
    /// We use a `mutex` instead of reader-writer lock so that
    /// we always need write permission to perform `path compression`
    /// during "finds".
    ///
    /// Alternatively, we could do no path compression at `find`,
    /// and only do path compression when we were doing union.
    node_parents: HashMap<T, T>,
    /// Number of disjoint sets.
    num_sets: usize,
}

pub trait UnionFind<T>
where
    T: Ord,
{
    /// Unions the set containing `a` and the set containing `b`.
    /// Returns the new representative followed by the other node,
    /// or `None` if one the node is not present.
    ///
    /// The smaller representative is selected as the new representative.
    fn union(&mut self, a: &T, b: &T) -> Option<[T; 2]>;

    /// Gets the representative node of the set that `node` is in.
    /// Path compression is performed while finding the representative.
    fn find_path_compress(&mut self, node: &T) -> Option<T>;

    /// Gets the representative node of the set that `node` is in.
    fn find(&self, node: &T) -> Option<T>;
}

impl<T> DisjointSet<T>
where
    T: Ord + Hash + Copy + Debug,
{
    pub fn new() -> Self {
        DisjointSet {
            node_parents: HashMap::new(),
            num_sets: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.node_parents.len()
    }

    pub fn num_sets(&self) -> usize {
        self.num_sets
    }

    pub fn add(&mut self, node: T) {
        use std::collections::hash_map::Entry;

        if let Entry::Vacant(entry) = self.node_parents.entry(node) {
            entry.insert(node);
            self.num_sets += 1;
        }
    }

    fn get_parent(&self, node: &T) -> Option<T> {
        self.node_parents.get(node).copied()
    }

    fn set_parent(&mut self, node: T, parent: T) {
        self.node_parents.insert(node, parent);
    }

    /// Recursively find the parent of the `node` until reaching the representative of the set.
    /// A node is the representative if the its parent is the node itself.
    ///
    /// We utilize "path compression" to shorten the height of parent forest.
    fn find_path_compress_inner(&mut self, node: &T) -> Option<T> {
        let mut parent = self.get_parent(node)?;

        if *node != parent {
            parent = self.find_path_compress_inner(&parent)?;

            // Path compression.
            self.set_parent(*node, parent);
        }

        Some(parent)
    }

    /// Recursively find the parent of the `node` until reaching the representative of the set.
    /// A node is the representative if the its parent is the node itself.
    fn find_inner(&self, node: &T) -> Option<T> {
        let mut parent = self.get_parent(node)?;

        if *node != parent {
            parent = self.find_inner(&parent)?;
        }

        Some(parent)
    }

    /// Gets the given node corresponding entry in `node_parents` map for in-place manipulation.
    pub(super) fn entry(&mut self, node: T) -> hash_map::Entry<'_, T, T> {
        self.node_parents.entry(node)
    }
}

impl<T> UnionFind<T> for DisjointSet<T>
where
    T: Ord + Hash + Copy + Debug,
{
    fn union(&mut self, a: &T, b: &T) -> Option<[T; 2]> {
        use std::cmp::Ordering;

        // Gets the represenatives for set containing `a`.
        let a_rep = self.find_path_compress(&a)?;

        // Gets the represenatives for set containing `b`.
        let b_rep = self.find_path_compress(&b)?;

        // Node with smaller value becomes the representative.
        let res = match a_rep.cmp(&b_rep) {
            Ordering::Less => {
                self.set_parent(b_rep, a_rep);
                self.num_sets -= 1;
                [a_rep, b_rep]
            }
            Ordering::Greater => {
                self.set_parent(a_rep, b_rep);
                self.num_sets -= 1;
                [b_rep, a_rep]
            }
            Ordering::Equal => [a_rep, b_rep],
        };
        Some(res)
    }

    /// See [`Self::find_inner`] for implementation detail.
    fn find_path_compress(&mut self, node: &T) -> Option<T> {
        self.find_path_compress_inner(node)
    }

    /// See [`Self::find_inner`] for implementation detail.
    fn find(&self, node: &T) -> Option<T> {
        self.find_inner(node)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn minmax<T>(v1: T, v2: T) -> [T; 2]
    where
        T: Ord,
    {
        if v1 <= v2 {
            [v1, v2]
        } else {
            [v2, v1]
        }
    }

    fn test_union_find<T>(inputs: Vec<T>)
    where
        T: Ord + Hash + Copy + Debug,
    {
        let mut set = DisjointSet::new();

        for input in inputs.iter() {
            set.add(*input);
        }

        for input in inputs.iter() {
            let rep = set.find(input);
            assert_eq!(
                rep,
                Some(*input),
                "representive should be node itself for singleton"
            );
        }
        assert_eq!(set.size(), 10);
        assert_eq!(set.num_sets(), 10);

        for input in inputs.iter() {
            set.union(input, input).unwrap();
            let rep = set.find(input);
            assert_eq!(
                rep,
                Some(*input),
                "representive should be node itself for singleton"
            );
        }
        assert_eq!(set.size(), 10);
        assert_eq!(set.num_sets(), 10);

        for (x, y) in inputs.iter().zip(inputs.iter().rev()) {
            let y_rep = set.find(&y).unwrap();
            let [rep, other] = set.union(x, y).expect(&format!(
                "union should be successful between {:?} and {:?}",
                x, y,
            ));
            if rep != other {
                assert_eq!([rep, other], minmax(*x, y_rep));
            }
        }

        for (x, y) in inputs.iter().zip(inputs.iter().rev()) {
            let rep = set.find(x);

            let expected = x.min(y);
            assert_eq!(rep, Some(*expected));
        }
        assert_eq!(set.size(), 10);
        assert_eq!(set.num_sets(), 5);
    }

    #[test]
    fn test_union_find_i32() {
        test_union_find(Vec::from_iter(0..10));
    }

    #[test]
    fn test_union_find_group() {
        test_union_find(Vec::from_iter((0..10).map(|i| crate::cascades::GroupId(i))));
    }
}
