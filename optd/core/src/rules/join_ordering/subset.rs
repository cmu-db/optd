use bitvec::prelude::*;
use itertools::Itertools;

/// Returns an iterator over all non-empty subsets of `set`, yielding smaller
/// subsets (by cardinality) first.
///
/// For a set with bits {a, b, c}, the iteration order is:
/// {a}, {b}, {c}, {a,b}, {a,c}, {b,c}, {a,b,c}
pub fn subsets(set: &BitVec) -> impl Iterator<Item = BitVec> {
    let set_bits: Vec<usize> = set.iter_ones().collect();
    let len = set.len();
    let n = set_bits.len();

    (1..=n).flat_map(move |k| {
        let set_bits = set_bits.clone();
        set_bits.into_iter().combinations(k).map(move |combo| {
            let mut bv = bitvec![0; len];
            for pos in combo {
                bv.set(pos, true);
            }
            bv
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_set() {
        let set = bitvec![0; 3];
        let result: Vec<BitVec> = subsets(&set).collect();
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_bit() {
        let set = bitvec![0, 1, 0];
        let result: Vec<BitVec> = subsets(&set).collect();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], bitvec![0, 1, 0]);
    }

    #[test]
    fn test_three_bits() {
        let set = bitvec![1, 1, 1];
        let result: Vec<BitVec> = subsets(&set).collect();

        // 2^3 - 1 = 7 non-empty subsets
        assert_eq!(result.len(), 7);

        // Size 1: {0}, {1}, {2}
        assert_eq!(result[0], bitvec![1, 0, 0]);
        assert_eq!(result[1], bitvec![0, 1, 0]);
        assert_eq!(result[2], bitvec![0, 0, 1]);

        // Size 2: {0,1}, {0,2}, {1,2}
        assert_eq!(result[3], bitvec![1, 1, 0]);
        assert_eq!(result[4], bitvec![1, 0, 1]);
        assert_eq!(result[5], bitvec![0, 1, 1]);

        // Size 3: {0,1,2}
        assert_eq!(result[6], bitvec![1, 1, 1]);
    }

    #[test]
    fn test_sparse_bits() {
        let set = bitvec![1, 0, 1, 0, 1];
        let result: Vec<BitVec> = subsets(&set).collect();

        // 3 set bits -> 2^3 - 1 = 7 subsets
        assert_eq!(result.len(), 7);

        // Size 1: {0}, {2}, {4}
        assert_eq!(result[0], bitvec![1, 0, 0, 0, 0]);
        assert_eq!(result[1], bitvec![0, 0, 1, 0, 0]);
        assert_eq!(result[2], bitvec![0, 0, 0, 0, 1]);

        // Size 2: {0,2}, {0,4}, {2,4}
        assert_eq!(result[3], bitvec![1, 0, 1, 0, 0]);
        assert_eq!(result[4], bitvec![1, 0, 0, 0, 1]);
        assert_eq!(result[5], bitvec![0, 0, 1, 0, 1]);

        // Size 3: {0,2,4}
        assert_eq!(result[6], bitvec![1, 0, 1, 0, 1]);
    }

    #[test]
    fn test_subset_count() {
        // 4 set bits -> 2^4 - 1 = 15 subsets
        let set = bitvec![1, 1, 1, 1];
        assert_eq!(subsets(&set).count(), 15);
    }

    #[test]
    fn test_ordering_by_cardinality() {
        let set = bitvec![1, 1, 1, 1];
        let result: Vec<BitVec> = subsets(&set).collect();

        // Verify that subsets are ordered by increasing cardinality.
        let mut prev_size = 0;
        for subset in &result {
            let size = subset.count_ones();
            assert!(
                size >= prev_size,
                "subsets should be non-decreasing in cardinality"
            );
            prev_size = size;
        }
    }
}
