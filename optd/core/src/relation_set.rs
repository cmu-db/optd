//! Small-optimized immutable sets of relation identifiers.
//!
//! Join enumeration is dominated by set union, subset, and disjointness checks. The common
//! case therefore stays in one machine word, while large join groups transparently spill into
//! a canonical boxed word slice. Canonicalization is important because relation sets are DP-table
//! keys: two equal sets must always compare and hash equally, regardless of how they were built.

use std::fmt;
use std::ops::{BitOr, BitOrAssign};

const WORD_BITS: usize = u64::BITS as usize;

/// An immutable set of zero-based relation identifiers.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RelationSet {
    repr: Repr,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum Repr {
    Inline(u64),
    Heap(Box<[u64]>),
}

impl RelationSet {
    /// The empty relation set.
    pub const EMPTY: Self = Self {
        repr: Repr::Inline(0),
    };

    /// Returns a set containing one relation.
    #[inline]
    pub fn singleton(relation: usize) -> Self {
        if relation < WORD_BITS {
            Self::inline(1 << relation)
        } else {
            let mut words = vec![0; relation / WORD_BITS + 1];
            words[relation / WORD_BITS] = 1 << (relation % WORD_BITS);
            Self::from_words(words)
        }
    }

    /// Returns a set containing relation identifiers in `0..relation_count`.
    pub fn all(relation_count: usize) -> Self {
        match relation_count {
            0 => Self::EMPTY,
            1..=WORD_BITS => Self::inline(u64::MAX >> (WORD_BITS - relation_count)),
            _ => {
                let mut words = vec![u64::MAX; relation_count.div_ceil(WORD_BITS)];
                let remainder = relation_count % WORD_BITS;
                if remainder != 0 {
                    *words.last_mut().expect("a non-empty set has a last word") =
                        u64::MAX >> (WORD_BITS - remainder);
                }
                Self::from_words(words)
            }
        }
    }

    /// Returns the inclusive prefix `0..=relation`.
    #[inline]
    pub fn prefix_inclusive(relation: usize) -> Self {
        Self::all(relation + 1)
    }

    /// Returns `true` when the set has no members.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_words().iter().all(|word| *word == 0)
    }

    /// Returns the number of relations in the set.
    #[inline]
    pub fn len(&self) -> usize {
        self.as_words()
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    /// Compatibility name for bit-mask callers; equivalent to [`Self::len`].
    #[inline]
    pub fn count_ones(&self) -> usize {
        self.len()
    }

    /// Returns `true` if `relation` is a member.
    #[inline]
    pub fn contains(&self, relation: usize) -> bool {
        self.as_words()
            .get(relation / WORD_BITS)
            .is_some_and(|word| word & (1 << (relation % WORD_BITS)) != 0)
    }

    /// Returns the smallest relation identifier, or `None` for an empty set.
    pub fn min(&self) -> Option<usize> {
        self.as_words()
            .iter()
            .enumerate()
            .find(|(_, word)| **word != 0)
            .map(|(index, word)| index * WORD_BITS + word.trailing_zeros() as usize)
    }

    /// Iterates relation identifiers in ascending order.
    pub fn iter(&self) -> RelationSetIter<'_> {
        RelationSetIter {
            words: self.as_words(),
            word_index: 0,
            current: self.as_words().first().copied().unwrap_or(0),
        }
    }

    /// Returns `true` when every member of `self` is in `other`.
    #[inline]
    pub fn is_subset(&self, other: &Self) -> bool {
        self.as_words()
            .iter()
            .enumerate()
            .all(|(index, word)| word & !other.as_words().get(index).copied().unwrap_or(0) == 0)
    }

    /// Returns `true` when the sets have no members in common.
    #[inline]
    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.as_words()
            .iter()
            .zip(other.as_words())
            .all(|(left, right)| left & right == 0)
    }

    /// Returns the union of two sets.
    #[inline]
    pub fn union(&self, other: &Self) -> Self {
        if let (Repr::Inline(left), Repr::Inline(right)) = (&self.repr, &other.repr) {
            return Self::inline(left | right);
        }
        let word_count = self.as_words().len().max(other.as_words().len());
        Self::from_words(
            (0..word_count)
                .map(|index| {
                    self.as_words().get(index).copied().unwrap_or(0)
                        | other.as_words().get(index).copied().unwrap_or(0)
                })
                .collect(),
        )
    }

    /// Returns the intersection of two sets.
    pub fn intersection(&self, other: &Self) -> Self {
        Self::from_words(
            self.as_words()
                .iter()
                .zip(other.as_words())
                .map(|(left, right)| left & right)
                .collect(),
        )
    }

    /// Returns the set difference `self − other`.
    pub fn difference(&self, other: &Self) -> Self {
        Self::from_words(
            self.as_words()
                .iter()
                .enumerate()
                .map(|(index, word)| word & !other.as_words().get(index).copied().unwrap_or(0))
                .collect(),
        )
    }

    /// Returns a copy with `relation` inserted.
    #[inline]
    pub fn with(&self, relation: usize) -> Self {
        self.union(&Self::singleton(relation))
    }

    /// Iterates all non-empty subsets. Callers must impose a budget before using this on wide
    /// sets; subset enumeration is inherently exponential.
    pub fn non_empty_subsets(&self) -> NonEmptySubsets {
        match self.repr {
            Repr::Inline(mask) => NonEmptySubsets::Inline {
                mask,
                current: 0,
                done: mask == 0,
            },
            Repr::Heap(_) => NonEmptySubsets::Dynamic {
                members: self.iter().collect(),
                selection: vec![false; self.len()],
                started: false,
                done: self.is_empty(),
            },
        }
    }

    #[inline]
    const fn inline(bits: u64) -> Self {
        Self {
            repr: Repr::Inline(bits),
        }
    }

    fn from_words(mut words: Vec<u64>) -> Self {
        while words.last() == Some(&0) {
            words.pop();
        }
        match words.len() {
            0 => Self::EMPTY,
            1 => Self::inline(words[0]),
            _ => Self {
                repr: Repr::Heap(words.into_boxed_slice()),
            },
        }
    }

    #[inline]
    fn as_words(&self) -> &[u64] {
        match &self.repr {
            Repr::Inline(word) => std::slice::from_ref(word),
            Repr::Heap(words) => words,
        }
    }
}

impl Default for RelationSet {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl FromIterator<usize> for RelationSet {
    fn from_iter<T: IntoIterator<Item = usize>>(relations: T) -> Self {
        let mut words = Vec::new();
        for relation in relations {
            let word_index = relation / WORD_BITS;
            if word_index >= words.len() {
                words.resize(word_index + 1, 0);
            }
            words[word_index] |= 1 << (relation % WORD_BITS);
        }
        Self::from_words(words)
    }
}

impl fmt::Debug for RelationSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_set().entries(self.iter()).finish()
    }
}

impl BitOr for &RelationSet {
    type Output = RelationSet;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl BitOrAssign<&RelationSet> for RelationSet {
    fn bitor_assign(&mut self, rhs: &RelationSet) {
        let replacement = match (&mut self.repr, &rhs.repr) {
            (Repr::Inline(left), Repr::Inline(right)) => {
                *left |= right;
                None
            }
            (Repr::Heap(left), Repr::Inline(right)) => {
                left[0] |= right;
                None
            }
            (Repr::Inline(left), Repr::Heap(right)) => {
                let mut words = right.to_vec();
                words[0] |= *left;
                Some(Repr::Heap(words.into_boxed_slice()))
            }
            (Repr::Heap(left), Repr::Heap(right)) if left.len() >= right.len() => {
                for (left, right) in left.iter_mut().zip(right.iter()) {
                    *left |= right;
                }
                None
            }
            (Repr::Heap(left), Repr::Heap(right)) => {
                let mut words = right.to_vec();
                for (word, left) in words.iter_mut().zip(left.iter()) {
                    *word |= left;
                }
                *left = words.into_boxed_slice();
                None
            }
        };

        if let Some(repr) = replacement {
            self.repr = repr;
        }
    }
}

/// Iterator over a [`RelationSet`].
pub struct RelationSetIter<'a> {
    words: &'a [u64],
    word_index: usize,
    current: u64,
}

impl Iterator for RelationSetIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current == 0 {
            self.word_index += 1;
            self.current = *self.words.get(self.word_index)?;
        }
        let bit = self.current.trailing_zeros() as usize;
        self.current &= self.current - 1;
        Some(self.word_index * WORD_BITS + bit)
    }
}

/// Iterator over all non-empty subsets of a relation set.
pub enum NonEmptySubsets {
    Inline {
        mask: u64,
        current: u64,
        done: bool,
    },
    Dynamic {
        members: Vec<usize>,
        selection: Vec<bool>,
        started: bool,
        done: bool,
    },
}

impl Iterator for NonEmptySubsets {
    type Item = RelationSet;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline {
                mask,
                current,
                done,
            } => {
                if *done {
                    return None;
                }
                *current = current.wrapping_sub(*mask) & *mask;
                *done = *current == *mask;
                Some(RelationSet::inline(*current))
            }
            Self::Dynamic {
                members,
                selection,
                started,
                done,
            } => {
                if *done {
                    return None;
                }
                if !*started {
                    selection[0] = true;
                    *started = true;
                } else {
                    let mut index = 0;
                    while index < selection.len() && selection[index] {
                        selection[index] = false;
                        index += 1;
                    }
                    if index == selection.len() {
                        *done = true;
                        return None;
                    }
                    selection[index] = true;
                }
                let result = members
                    .iter()
                    .zip(selection.iter())
                    .filter_map(|(member, selected)| selected.then_some(*member))
                    .collect();
                *done = selection.iter().all(|selected| *selected);
                Some(result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::{RelationSet, Repr};

    fn fingerprint(set: &RelationSet) -> u64 {
        let mut hasher = DefaultHasher::new();
        set.hash(&mut hasher);
        hasher.finish()
    }

    fn heap_storage(set: &RelationSet) -> (*const u64, usize) {
        match &set.repr {
            Repr::Heap(words) => (words.as_ptr(), words.len()),
            Repr::Inline(_) => panic!("expected dynamic relation-set storage"),
        }
    }

    #[test]
    fn inline_and_dynamic_sets_share_functional_operations() {
        let set = [0, 63, 64, 129].into_iter().collect::<RelationSet>();
        assert_eq!(set.iter().collect::<Vec<_>>(), [0, 63, 64, 129]);
        assert!(
            [63, 64]
                .into_iter()
                .collect::<RelationSet>()
                .is_subset(&set)
        );
        assert!(set.is_disjoint(&[1, 65].into_iter().collect()));
        assert_eq!(
            set.difference(&[63, 129].into_iter().collect()),
            [0, 64].into_iter().collect::<RelationSet>()
        );
    }

    #[test]
    fn all_and_prefix_cross_word_boundaries() {
        assert_eq!(RelationSet::all(0), RelationSet::EMPTY);
        assert_eq!(RelationSet::all(64).len(), 64);
        assert_eq!(RelationSet::all(65).iter().last(), Some(64));
        assert_eq!(RelationSet::prefix_inclusive(128).len(), 129);
    }

    #[test]
    fn owned_union_reuses_dynamic_storage_when_it_already_fits() {
        let mut set = [0, 64, 129].into_iter().collect::<RelationSet>();
        let before = heap_storage(&set);

        set |= &[1, 63, 65, 128].into_iter().collect();

        assert_eq!(heap_storage(&set), before);
        assert_eq!(set.iter().collect::<Vec<_>>(), [0, 1, 63, 64, 65, 128, 129]);
    }

    #[test]
    fn owned_union_expands_at_word_boundaries_and_remains_canonical() {
        let mut set = RelationSet::singleton(63);
        set |= &RelationSet::singleton(64);
        assert!(matches!(&set.repr, Repr::Heap(words) if words.len() == 2));

        set |= &RelationSet::singleton(128);
        assert!(matches!(&set.repr, Repr::Heap(words) if words.len() == 3));
        assert_eq!(set.iter().collect::<Vec<_>>(), [63, 64, 128]);

        let reduced = set.difference(&[64, 128].into_iter().collect());
        assert!(matches!(reduced.repr, Repr::Inline(_)));
        assert_eq!(reduced, RelationSet::singleton(63));
    }

    #[test]
    fn from_iterator_builds_canonical_sets_across_boundaries() {
        let empty = std::iter::empty().collect::<RelationSet>();
        let inline = [63, 0, 63].into_iter().collect::<RelationSet>();
        let dynamic = [10_000, 64, 0, 128, 10_000]
            .into_iter()
            .collect::<RelationSet>();

        assert!(matches!(empty.repr, Repr::Inline(0)));
        assert!(matches!(inline.repr, Repr::Inline(_)));
        assert!(
            matches!(&dynamic.repr, Repr::Heap(words) if words.last().is_some_and(|word| *word != 0))
        );
        assert_eq!(dynamic.iter().collect::<Vec<_>>(), [0, 64, 128, 10_000]);

        let same = [128, 0, 10_000, 64].into_iter().collect::<RelationSet>();
        assert_eq!(dynamic, same);
        assert_eq!(fingerprint(&dynamic), fingerprint(&same));
    }

    #[test]
    fn subset_iteration_supports_high_relation_ids() {
        let subsets = [1, 65]
            .into_iter()
            .collect::<RelationSet>()
            .non_empty_subsets()
            .collect::<Vec<_>>();
        assert_eq!(subsets.len(), 3);
        assert!(subsets.contains(&RelationSet::singleton(1)));
        assert!(subsets.contains(&RelationSet::singleton(65)));
        assert!(subsets.contains(&[1, 65].into_iter().collect()));
    }
}
