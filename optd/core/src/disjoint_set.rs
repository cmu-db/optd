//! Internal disjoint-set forests shared by optimizer algorithms.
//!
//! The forest owns only partition mechanics. Algorithms that associate meaning
//! with a representative—such as a GOO tree node—keep that state in a
//! domain-specific wrapper.

/// A disjoint-set forest over the dense indices `0..len`.
///
/// [`Self::union`] uses union by size and path compression.
#[derive(Debug)]
pub(crate) struct DisjointSet {
    parents: Vec<usize>,
    sizes: Vec<usize>,
}

impl DisjointSet {
    pub(crate) fn new(len: usize) -> Self {
        Self {
            parents: (0..len).collect(),
            sizes: vec![1; len],
        }
    }

    pub(crate) fn find(&mut self, mut element: usize) -> usize {
        let mut root = element;
        while self.parents[root] != root {
            root = self.parents[root];
        }

        while self.parents[element] != element {
            let parent = self.parents[element];
            self.parents[element] = root;
            element = parent;
        }
        root
    }

    /// Unites two sets and reports whether they were previously disjoint.
    ///
    /// The larger set supplies the representative; ties retain `left`'s
    /// representative. Call [`Self::find`] after union when the representative
    /// itself matters.
    pub(crate) fn union(&mut self, left: usize, right: usize) -> bool {
        let mut left_root = self.find(left);
        let mut right_root = self.find(right);
        if left_root == right_root {
            return false;
        }
        if self.sizes[left_root] < self.sizes[right_root] {
            std::mem::swap(&mut left_root, &mut right_root);
        }
        self.parents[right_root] = left_root;
        self.sizes[left_root] += self.sizes[right_root];
        true
    }
}

#[cfg(test)]
mod tests {
    use super::DisjointSet;

    #[test]
    fn dense_disjoint_set_matches_all_graph_partitions_through_six_nodes() {
        for len in 0..=6 {
            let edges = (0..len)
                .flat_map(|left| ((left + 1)..len).map(move |right| (left, right)))
                .collect::<Vec<_>>();

            for mask in 0_usize..(1_usize << edges.len()) {
                let selected = edges
                    .iter()
                    .enumerate()
                    .filter_map(|(bit, edge)| ((mask >> bit) & 1 == 1).then_some(*edge))
                    .collect::<Vec<_>>();
                check_partition(len, &selected);

                let mut reversed = selected;
                reversed.reverse();
                check_partition(len, &reversed);
            }
        }
    }

    fn check_partition(len: usize, edges: &[(usize, usize)]) {
        let mut sets = DisjointSet::new(len);
        let mut labels = (0..len).collect::<Vec<_>>();

        for &(left, right) in edges {
            let left_label = labels[left];
            let right_label = labels[right];
            let expected_change = left_label != right_label;
            assert_eq!(sets.union(left, right), expected_change);

            if expected_change {
                for label in &mut labels {
                    if *label == right_label {
                        *label = left_label;
                    }
                }
            }

            assert!(!sets.union(left, right));
            assert!(!sets.union(right, left));
        }

        for left in 0..len {
            assert!(!sets.union(left, left));
            for right in 0..len {
                assert_eq!(
                    sets.find(left) == sets.find(right),
                    labels[left] == labels[right]
                );
            }
        }
    }

    #[test]
    fn equal_size_union_retains_the_left_representative() {
        let mut sets = DisjointSet::new(4);
        assert!(sets.union(1, 0));
        assert_eq!(sets.find(0), 1);

        assert!(sets.union(3, 2));
        assert!(sets.union(1, 3));
        assert_eq!(sets.find(2), 1);
        assert_eq!(sets.parents[3], 1);
    }
}
