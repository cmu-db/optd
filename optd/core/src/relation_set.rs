//! Small-optimized immutable sets of relation identifiers.
//!
//! Join enumeration is dominated by set union, subset, and disjointness checks. The representation
//! therefore follows the tiers suggested by Neumann and Radke for very large join queries:
//!
//! - one inline word for relation identifiers below 64;
//! - two inline words for relation identifiers below 128;
//! - dense boxed words for larger, sufficiently dense sets; and
//! - sorted sparse members for very large sparse sets.
//!
//! The paper chooses the last tier from the query's total relation count. `RelationSet` intentionally
//! carries no universe metadata, so this implementation uses a content-canonical adaptation:
//! sparse storage is considered once the largest member reaches 1024 and is selected only when its
//! payload is smaller than dense words. The choice consequently depends only on the members. This
//! is essential because relation sets are hash-table keys: equal sets must have the same
//! representation, equality, and hash regardless of how they were constructed.

use std::fmt;
use std::mem::size_of;
use std::ops::{BitOr, BitOrAssign};

const WORD_BITS: usize = u64::BITS as usize;
const INLINE128_BITS: usize = 2 * WORD_BITS;
const SPARSE_MIN_RELATION: usize = 1024;

/// An immutable set of zero-based relation identifiers.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RelationSet {
    repr: Repr,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum Repr {
    Inline64(u64),
    Inline128([u64; 2]),
    Dense(Box<[u64]>),
    Sparse(Box<[usize]>),
}

impl RelationSet {
    /// The empty relation set.
    pub const EMPTY: Self = Self {
        repr: Repr::Inline64(0),
    };

    /// Returns a set containing one relation.
    #[inline]
    pub fn singleton(relation: usize) -> Self {
        match relation {
            0..WORD_BITS => Self::inline64(1 << relation),
            WORD_BITS..INLINE128_BITS => Self::inline128([0, 1 << (relation - WORD_BITS)]),
            _ => Self::from_sorted_members(vec![relation]),
        }
    }

    /// Returns a set containing relation identifiers in `0..relation_count`.
    pub fn all(relation_count: usize) -> Self {
        match relation_count {
            0 => Self::EMPTY,
            count if count <= WORD_BITS => Self::inline64(u64::MAX >> (WORD_BITS - relation_count)),
            count if count <= INLINE128_BITS => {
                Self::inline128([u64::MAX, u64::MAX >> (INLINE128_BITS - relation_count)])
            }
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
    ///
    /// # Panics
    ///
    /// Panics when `relation` is [`usize::MAX`], because that prefix contains one more member
    /// than a `usize` can represent.
    #[inline]
    pub fn prefix_inclusive(relation: usize) -> Self {
        Self::all(
            relation
                .checked_add(1)
                .expect("an inclusive relation prefix must fit in usize"),
        )
    }

    /// Returns `true` when the set has no members.
    #[inline]
    pub fn is_empty(&self) -> bool {
        match &self.repr {
            Repr::Inline64(word) => *word == 0,
            Repr::Inline128(words) => words.iter().all(|word| *word == 0),
            Repr::Dense(words) => words.iter().all(|word| *word == 0),
            Repr::Sparse(members) => members.is_empty(),
        }
    }

    /// Returns the number of relations in the set.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Inline64(word) => word.count_ones() as usize,
            Repr::Inline128(words) => words.iter().map(|word| word.count_ones() as usize).sum(),
            Repr::Dense(words) => words.iter().map(|word| word.count_ones() as usize).sum(),
            Repr::Sparse(members) => members.len(),
        }
    }

    /// Compatibility name for bit-mask callers; equivalent to [`Self::len`].
    #[inline]
    pub fn count_ones(&self) -> usize {
        self.len()
    }

    /// Returns `true` if `relation` is a member.
    #[inline]
    pub fn contains(&self, relation: usize) -> bool {
        match &self.repr {
            Repr::Inline64(word) => relation < WORD_BITS && word & (1 << relation) != 0,
            Repr::Inline128(words) => words
                .get(relation / WORD_BITS)
                .is_some_and(|word| word & (1 << (relation % WORD_BITS)) != 0),
            Repr::Dense(words) => words
                .get(relation / WORD_BITS)
                .is_some_and(|word| word & (1 << (relation % WORD_BITS)) != 0),
            Repr::Sparse(members) => members.binary_search(&relation).is_ok(),
        }
    }

    /// Returns the smallest relation identifier, or `None` for an empty set.
    pub fn min(&self) -> Option<usize> {
        match &self.repr {
            Repr::Sparse(members) => members.first().copied(),
            _ => self.words().and_then(|words| {
                words
                    .iter()
                    .enumerate()
                    .find(|(_, word)| **word != 0)
                    .map(|(index, word)| index * WORD_BITS + word.trailing_zeros() as usize)
            }),
        }
    }

    /// Iterates relation identifiers in ascending order.
    pub fn iter(&self) -> RelationSetIter<'_> {
        match &self.repr {
            Repr::Sparse(members) => RelationSetIter {
                inner: RelationSetIterInner::Sparse(members.iter()),
            },
            _ => {
                let words = self
                    .words()
                    .expect("non-sparse relation sets expose dense words");
                RelationSetIter {
                    inner: RelationSetIterInner::Words {
                        words,
                        word_index: 0,
                        current: words.first().copied().unwrap_or(0),
                    },
                }
            }
        }
    }

    /// Returns `true` when every member of `self` is in `other`.
    #[inline]
    pub fn is_subset(&self, other: &Self) -> bool {
        match (self.words(), other.words()) {
            (Some(left), Some(right)) => left
                .iter()
                .enumerate()
                .all(|(index, word)| word & !right.get(index).copied().unwrap_or(0) == 0),
            _ if self.len() > other.len() => false,
            _ => self.iter().all(|relation| other.contains(relation)),
        }
    }

    /// Returns `true` when the sets have no members in common.
    #[inline]
    pub fn is_disjoint(&self, other: &Self) -> bool {
        match (self.words(), other.words()) {
            (Some(left), Some(right)) => left
                .iter()
                .zip(right)
                .all(|(left, right)| left & right == 0),
            _ => {
                let (smaller, larger) = if self.len() <= other.len() {
                    (self, other)
                } else {
                    (other, self)
                };
                smaller.iter().all(|relation| !larger.contains(relation))
            }
        }
    }

    /// Returns the union of two sets.
    #[inline]
    pub fn union(&self, other: &Self) -> Self {
        if let (Some(left), Some(right)) = (self.words(), other.words()) {
            return Self::from_word_fn(left.len().max(right.len()), |index| {
                left.get(index).copied().unwrap_or(0) | right.get(index).copied().unwrap_or(0)
            });
        }

        Self::from_sorted_members(merge_sorted_members(self.iter(), other.iter()))
    }

    /// Returns the intersection of two sets.
    pub fn intersection(&self, other: &Self) -> Self {
        if let (Some(left), Some(right)) = (self.words(), other.words()) {
            return Self::from_word_fn(left.len().min(right.len()), |index| {
                left[index] & right[index]
            });
        }

        let (candidates, lookup) = if self.len() <= other.len() {
            (self, other)
        } else {
            (other, self)
        };
        Self::from_sorted_members(
            candidates
                .iter()
                .filter(|relation| lookup.contains(*relation))
                .collect(),
        )
    }

    /// Returns the set difference `self − other`.
    pub fn difference(&self, other: &Self) -> Self {
        if let (Some(left), Some(right)) = (self.words(), other.words()) {
            return Self::from_word_fn(left.len(), |index| {
                left[index] & !right.get(index).copied().unwrap_or(0)
            });
        }

        Self::from_sorted_members(
            self.iter()
                .filter(|relation| !other.contains(*relation))
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
        match &self.repr {
            Repr::Inline64(mask) => NonEmptySubsets::Inline {
                mask: *mask,
                current: 0,
                done: *mask == 0,
            },
            Repr::Inline128([low, high]) => {
                let mask = u128::from(*low) | (u128::from(*high) << WORD_BITS);
                NonEmptySubsets::Inline128 {
                    mask,
                    current: 0,
                    done: mask == 0,
                }
            }
            Repr::Dense(_) | Repr::Sparse(_) => NonEmptySubsets::Dynamic {
                members: self.iter().collect(),
                selection: vec![false; self.len()],
                started: false,
                done: self.is_empty(),
            },
        }
    }

    #[inline]
    const fn inline64(bits: u64) -> Self {
        Self {
            repr: Repr::Inline64(bits),
        }
    }

    #[inline]
    fn inline128(words: [u64; 2]) -> Self {
        if words[1] == 0 {
            Self::inline64(words[0])
        } else {
            Self {
                repr: Repr::Inline128(words),
            }
        }
    }

    #[inline]
    fn from_u128(bits: u128) -> Self {
        Self::inline128([bits as u64, (bits >> WORD_BITS) as u64])
    }

    fn from_word_fn(mut word_count: usize, mut word_at: impl FnMut(usize) -> u64) -> Self {
        while word_count > 0 && word_at(word_count - 1) == 0 {
            word_count -= 1;
        }
        match word_count {
            0 => Self::EMPTY,
            1 => Self::inline64(word_at(0)),
            2 => Self::inline128([word_at(0), word_at(1)]),
            _ => Self::from_words((0..word_count).map(word_at).collect()),
        }
    }

    fn from_words(mut words: Vec<u64>) -> Self {
        while words.last() == Some(&0) {
            words.pop();
        }
        match words.len() {
            0 => Self::EMPTY,
            1 => Self::inline64(words[0]),
            2 => Self::inline128([words[0], words[1]]),
            word_count => {
                let member_count = words.iter().map(|word| word.count_ones() as usize).sum();
                let last_word = words[word_count - 1];
                let highest_relation = (word_count - 1) * WORD_BITS
                    + (WORD_BITS - 1 - last_word.leading_zeros() as usize);
                if should_use_sparse(highest_relation, member_count, word_count) {
                    let mut members = Vec::with_capacity(member_count);
                    for (word_index, mut word) in words.iter().copied().enumerate() {
                        while word != 0 {
                            let bit = word.trailing_zeros() as usize;
                            members.push(word_index * WORD_BITS + bit);
                            word &= word - 1;
                        }
                    }
                    Self {
                        repr: Repr::Sparse(members.into_boxed_slice()),
                    }
                } else {
                    Self {
                        repr: Repr::Dense(words.into_boxed_slice()),
                    }
                }
            }
        }
    }

    fn from_sorted_members(members: Vec<usize>) -> Self {
        debug_assert!(members.windows(2).all(|pair| pair[0] < pair[1]));
        let Some(&highest_relation) = members.last() else {
            return Self::EMPTY;
        };

        if highest_relation < WORD_BITS {
            return Self::inline64(
                members
                    .into_iter()
                    .fold(0, |word, relation| word | (1 << relation)),
            );
        }
        if highest_relation < INLINE128_BITS {
            let words = members.into_iter().fold([0, 0], |mut words, relation| {
                words[relation / WORD_BITS] |= 1 << (relation % WORD_BITS);
                words
            });
            return Self::inline128(words);
        }

        let word_count = highest_relation / WORD_BITS + 1;
        if should_use_sparse(highest_relation, members.len(), word_count) {
            return Self {
                repr: Repr::Sparse(members.into_boxed_slice()),
            };
        }

        let mut words = vec![0; word_count];
        for relation in members {
            words[relation / WORD_BITS] |= 1 << (relation % WORD_BITS);
        }
        Self {
            repr: Repr::Dense(words.into_boxed_slice()),
        }
    }

    #[inline]
    fn words(&self) -> Option<&[u64]> {
        match &self.repr {
            Repr::Inline64(word) => Some(std::slice::from_ref(word)),
            Repr::Inline128(words) => Some(words),
            Repr::Dense(words) => Some(words),
            Repr::Sparse(_) => None,
        }
    }
}

#[inline]
fn should_use_sparse(highest_relation: usize, member_count: usize, word_count: usize) -> bool {
    highest_relation >= SPARSE_MIN_RELATION
        && member_count.saturating_mul(size_of::<usize>())
            < word_count.saturating_mul(size_of::<u64>())
}

fn merge_sorted_members(
    left: impl Iterator<Item = usize>,
    right: impl Iterator<Item = usize>,
) -> Vec<usize> {
    let mut left = left.peekable();
    let mut right = right.peekable();
    let mut merged = Vec::new();

    loop {
        match (left.peek().copied(), right.peek().copied()) {
            (Some(left_member), Some(right_member)) if left_member < right_member => {
                merged.push(left_member);
                left.next();
            }
            (Some(left_member), Some(right_member)) if left_member > right_member => {
                merged.push(right_member);
                right.next();
            }
            (Some(member), Some(_)) => {
                merged.push(member);
                left.next();
                right.next();
            }
            (Some(_), None) => {
                merged.extend(left);
                break;
            }
            (None, Some(_)) => {
                merged.extend(right);
                break;
            }
            (None, None) => break,
        }
    }

    merged
}

impl Default for RelationSet {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl FromIterator<usize> for RelationSet {
    fn from_iter<T: IntoIterator<Item = usize>>(relations: T) -> Self {
        let mut members = relations.into_iter().collect::<Vec<_>>();
        let Some(highest_relation) = members.iter().copied().max() else {
            return Self::EMPTY;
        };
        let word_count = highest_relation / WORD_BITS + 1;

        // Avoid sorting the ordinary join-enumeration case. Once the input itself is at least as
        // large as the dense word vector, allocating that vector is also bounded by memory we
        // already hold. `from_words` still makes the final canonical dense/sparse decision after
        // duplicates have collapsed.
        if highest_relation < SPARSE_MIN_RELATION || word_count <= members.len() {
            let mut words = vec![0; word_count];
            for relation in members {
                words[relation / WORD_BITS] |= 1 << (relation % WORD_BITS);
            }
            return Self::from_words(words);
        }

        members.sort_unstable();
        members.dedup();
        Self::from_sorted_members(members)
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
        let mut use_functional_union = false;
        let replacement = match (&mut self.repr, &rhs.repr) {
            (Repr::Inline64(left), Repr::Inline64(right)) => {
                *left |= right;
                None
            }
            (Repr::Inline128(left), Repr::Inline64(right)) => {
                left[0] |= right;
                None
            }
            (Repr::Inline128(left), Repr::Inline128(right)) => {
                left[0] |= right[0];
                left[1] |= right[1];
                None
            }
            (Repr::Inline64(left), Repr::Inline128(right)) => {
                Some(Repr::Inline128([*left | right[0], right[1]]))
            }
            (Repr::Dense(left), Repr::Inline64(right)) => {
                left[0] |= right;
                None
            }
            (Repr::Dense(left), Repr::Inline128(right)) => {
                left[0] |= right[0];
                left[1] |= right[1];
                None
            }
            (Repr::Dense(left), Repr::Dense(right)) if left.len() >= right.len() => {
                for (left, right) in left.iter_mut().zip(right.iter()) {
                    *left |= right;
                }
                None
            }
            _ => {
                use_functional_union = true;
                None
            }
        };

        if let Some(repr) = replacement {
            self.repr = repr;
        } else if use_functional_union {
            *self = self.union(rhs);
        }
    }
}

/// Iterator over a [`RelationSet`].
pub struct RelationSetIter<'a> {
    inner: RelationSetIterInner<'a>,
}

enum RelationSetIterInner<'a> {
    Words {
        words: &'a [u64],
        word_index: usize,
        current: u64,
    },
    Sparse(std::slice::Iter<'a, usize>),
}

impl Iterator for RelationSetIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            RelationSetIterInner::Words {
                words,
                word_index,
                current,
            } => {
                while *current == 0 {
                    *word_index += 1;
                    *current = *words.get(*word_index)?;
                }
                let bit = current.trailing_zeros() as usize;
                *current &= *current - 1;
                Some(*word_index * WORD_BITS + bit)
            }
            RelationSetIterInner::Sparse(members) => members.next().copied(),
        }
    }
}

/// Iterator over all non-empty subsets of a relation set.
pub enum NonEmptySubsets {
    Inline {
        mask: u64,
        current: u64,
        done: bool,
    },
    Inline128 {
        mask: u128,
        current: u128,
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
                Some(RelationSet::inline64(*current))
            }
            Self::Inline128 {
                mask,
                current,
                done,
            } => {
                if *done {
                    return None;
                }
                *current = current.wrapping_sub(*mask) & *mask;
                *done = *current == *mask;
                Some(RelationSet::from_u128(*current))
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
    use std::collections::{BTreeSet, HashMap, hash_map::DefaultHasher};
    use std::hash::{Hash, Hasher};

    use super::{RelationSet, Repr, should_use_sparse};

    fn fingerprint(set: &RelationSet) -> u64 {
        let mut hasher = DefaultHasher::new();
        set.hash(&mut hasher);
        hasher.finish()
    }

    fn dense_storage(set: &RelationSet) -> (*const u64, usize) {
        match &set.repr {
            Repr::Dense(words) => (words.as_ptr(), words.len()),
            _ => panic!("expected dense relation-set storage"),
        }
    }

    #[test]
    fn representations_change_at_canonical_boundaries() {
        assert!(matches!(RelationSet::EMPTY.repr, Repr::Inline64(0)));
        assert!(matches!(RelationSet::singleton(63).repr, Repr::Inline64(_)));
        assert!(matches!(
            RelationSet::singleton(64).repr,
            Repr::Inline128(_)
        ));
        assert!(matches!(
            RelationSet::singleton(127).repr,
            Repr::Inline128(_)
        ));
        assert!(matches!(
            RelationSet::singleton(128).repr,
            Repr::Dense(words) if words.len() == 3
        ));
        assert!(matches!(
            RelationSet::singleton(1023).repr,
            Repr::Dense(words) if words.len() == 16
        ));
        assert!(matches!(
            RelationSet::singleton(1024).repr,
            Repr::Sparse(members) if members.as_ref() == [1024]
        ));
        assert!(matches!(
            RelationSet::all(1025).repr,
            Repr::Dense(words) if words.len() == 17
        ));
    }

    #[test]
    fn every_representation_supports_the_same_set_operations() {
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
        assert_eq!(RelationSet::all(128).iter().last(), Some(127));
        assert_eq!(RelationSet::prefix_inclusive(128).len(), 129);
        assert_eq!(RelationSet::prefix_inclusive(1024).len(), 1025);
    }

    #[test]
    #[should_panic(expected = "an inclusive relation prefix must fit in usize")]
    fn inclusive_prefix_rejects_unrepresentable_cardinality() {
        RelationSet::prefix_inclusive(usize::MAX);
    }

    #[test]
    fn owned_union_reuses_dense_storage_when_it_already_fits() {
        let mut set = [0, 64, 129].into_iter().collect::<RelationSet>();
        let before = dense_storage(&set);

        set |= &[1, 63, 65, 128].into_iter().collect();

        assert_eq!(dense_storage(&set), before);
        assert_eq!(set.iter().collect::<Vec<_>>(), [0, 1, 63, 64, 65, 128, 129]);
    }

    #[test]
    fn owned_union_expands_at_word_boundaries_and_remains_canonical() {
        let mut set = RelationSet::singleton(63);
        set |= &RelationSet::singleton(64);
        assert!(matches!(&set.repr, Repr::Inline128(_)));

        set |= &RelationSet::singleton(128);
        assert!(matches!(&set.repr, Repr::Dense(words) if words.len() == 3));
        assert_eq!(set.iter().collect::<Vec<_>>(), [63, 64, 128]);

        let reduced = set.difference(&[64, 128].into_iter().collect());
        assert!(matches!(reduced.repr, Repr::Inline64(_)));
        assert_eq!(reduced, RelationSet::singleton(63));
    }

    #[test]
    fn from_iterator_builds_canonical_sets_across_boundaries() {
        let empty = std::iter::empty().collect::<RelationSet>();
        let inline = [63, 0, 63].into_iter().collect::<RelationSet>();
        let sparse = [10_000, 64, 0, 128, 10_000]
            .into_iter()
            .collect::<RelationSet>();

        assert!(matches!(empty.repr, Repr::Inline64(0)));
        assert!(matches!(inline.repr, Repr::Inline64(_)));
        assert!(matches!(&sparse.repr, Repr::Sparse(_)));
        assert_eq!(sparse.iter().collect::<Vec<_>>(), [0, 64, 128, 10_000]);

        let same = [128, 0, 10_000, 64].into_iter().collect::<RelationSet>();
        assert_eq!(sparse, same);
        assert_eq!(fingerprint(&sparse), fingerprint(&same));
    }

    #[test]
    fn sparse_sets_do_not_allocate_up_to_the_largest_identifier() {
        let set = [usize::MAX, 0, usize::MAX]
            .into_iter()
            .collect::<RelationSet>();

        assert!(matches!(&set.repr, Repr::Sparse(members) if members.len() == 2));
        assert_eq!(set.min(), Some(0));
        assert!(set.contains(usize::MAX));
        assert_eq!(set.iter().collect::<Vec<_>>(), [0, usize::MAX]);
    }

    #[test]
    fn operations_reselect_the_canonical_sparse_or_dense_tier() {
        let left = (0..17).chain([2048]).collect::<RelationSet>();
        let right = (17..33).chain([2047]).collect::<RelationSet>();
        assert!(matches!(&left.repr, Repr::Sparse(_)));
        assert!(matches!(&right.repr, Repr::Sparse(_)));

        let union = left.union(&right);
        let union_should_be_sparse = should_use_sparse(2048, 35, 33);
        assert_eq!(
            matches!(&union.repr, Repr::Sparse(_)),
            union_should_be_sparse
        );
        assert_eq!(union.len(), 35);

        let only_last = RelationSet::all(2049).difference(&(0..2048).collect::<RelationSet>());
        assert!(matches!(
            &only_last.repr,
            Repr::Sparse(members) if members.as_ref() == [2048]
        ));
    }

    #[test]
    fn subset_iteration_supports_inline128_and_sparse_sets() {
        for members in [[1, 65], [1024, 10_000]] {
            let subsets = members
                .into_iter()
                .collect::<RelationSet>()
                .non_empty_subsets()
                .collect::<Vec<_>>();
            assert_eq!(subsets.len(), 3);
            assert!(subsets.contains(&RelationSet::singleton(members[0])));
            assert!(subsets.contains(&RelationSet::singleton(members[1])));
            assert!(subsets.contains(&members.into_iter().collect()));
        }
    }

    #[test]
    fn subset_iteration_is_complete_and_unique_in_every_dynamic_tier() {
        for members in [[0, 63, 64, 127], [0, 64, 128, 191], [0, 64, 1024, 10_000]] {
            let mut actual = members
                .into_iter()
                .collect::<RelationSet>()
                .non_empty_subsets()
                .map(|subset| subset.iter().collect::<Vec<_>>())
                .collect::<Vec<_>>();
            actual.sort_unstable();
            actual.dedup();

            let mut expected = (1_u8..1 << members.len())
                .map(|selection| {
                    members
                        .into_iter()
                        .enumerate()
                        .filter_map(|(index, member)| {
                            (selection & (1 << index) != 0).then_some(member)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            expected.sort_unstable();

            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn canonical_hash_is_independent_of_construction_path_at_sparse_crossover() {
        let members = (0..16).chain([1024]).collect::<Vec<_>>();
        let collected = members.iter().copied().rev().chain([0, 1024]).collect();
        let inserted = members
            .iter()
            .copied()
            .fold(RelationSet::EMPTY, |set, relation| set.with(relation));
        let split_union =
            &(0..8).collect::<RelationSet>() | &(8..16).chain([1024]).collect::<RelationSet>();

        assert_eq!(collected, inserted);
        assert_eq!(inserted, split_union);
        assert_eq!(fingerprint(&collected), fingerprint(&inserted));
        assert_eq!(fingerprint(&inserted), fingerprint(&split_union));

        let mut lookup = HashMap::from([(collected, "canonical")]);
        assert_eq!(lookup.remove(&inserted), Some("canonical"));
    }

    #[test]
    fn extreme_identifier_set_algebra_never_allocates_by_identifier_value() {
        let extreme = [0, 127, 1024, usize::MAX]
            .into_iter()
            .collect::<RelationSet>();
        let overlap = [127, usize::MAX].into_iter().collect::<RelationSet>();
        let disjoint = [1, 1023].into_iter().collect::<RelationSet>();

        assert_eq!(
            extreme.intersection(&overlap).iter().collect::<Vec<_>>(),
            [127, usize::MAX]
        );
        assert_eq!(
            extreme.difference(&overlap).iter().collect::<Vec<_>>(),
            [0, 1024]
        );
        assert_eq!(
            overlap.union(&disjoint).iter().collect::<Vec<_>>(),
            [1, 127, 1023, usize::MAX]
        );
        assert!(overlap.is_subset(&extreme));
        assert!(extreme.is_disjoint(&disjoint));
    }

    #[test]
    fn randomized_set_algebra_and_canonical_hash_match_btree_set() {
        const DOMAIN: [usize; 18] = [
            0,
            1,
            62,
            63,
            64,
            65,
            126,
            127,
            128,
            129,
            1022,
            1023,
            1024,
            1025,
            2048,
            10_000,
            usize::MAX - 1,
            usize::MAX,
        ];

        let mut random = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next_random = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };

        for _ in 0..256 {
            let left_model = DOMAIN
                .into_iter()
                .filter(|_| next_random() & 1 != 0)
                .collect::<BTreeSet<_>>();
            let right_model = DOMAIN
                .into_iter()
                .filter(|_| next_random() & 1 != 0)
                .collect::<BTreeSet<_>>();
            let left = left_model.iter().copied().rev().collect::<RelationSet>();
            let right = right_model.iter().copied().rev().collect::<RelationSet>();

            let expected_union = left_model.union(&right_model).copied().collect::<Vec<_>>();
            let expected_intersection = left_model
                .intersection(&right_model)
                .copied()
                .collect::<Vec<_>>();
            let expected_difference = left_model
                .difference(&right_model)
                .copied()
                .collect::<Vec<_>>();

            assert_eq!(
                left.union(&right).iter().collect::<Vec<_>>(),
                expected_union
            );
            assert_eq!(
                left.intersection(&right).iter().collect::<Vec<_>>(),
                expected_intersection
            );
            assert_eq!(
                left.difference(&right).iter().collect::<Vec<_>>(),
                expected_difference
            );
            assert_eq!(left.is_subset(&right), left_model.is_subset(&right_model));
            assert_eq!(
                left.is_disjoint(&right),
                left_model.is_disjoint(&right_model)
            );

            let rebuilt = left
                .iter()
                .fold(RelationSet::EMPTY, |set, relation| set.with(relation));
            assert_eq!(left, rebuilt);
            assert_eq!(fingerprint(&left), fingerprint(&rebuilt));
        }
    }

    #[test]
    fn set_algebra_matches_btree_set_across_all_tiers() {
        let cases = [
            RelationSet::EMPTY,
            [0, 63].into_iter().collect(),
            [64, 127].into_iter().collect(),
            [0, 64, 128, 1023].into_iter().collect(),
            [1, 1024, 10_000].into_iter().collect(),
            RelationSet::all(130),
        ];

        for left in &cases {
            for right in &cases {
                let left_model = left.iter().collect::<BTreeSet<_>>();
                let right_model = right.iter().collect::<BTreeSet<_>>();
                let expected_union = left_model
                    .union(&right_model)
                    .copied()
                    .collect::<RelationSet>();
                let expected_intersection = left_model
                    .intersection(&right_model)
                    .copied()
                    .collect::<RelationSet>();
                let expected_difference = left_model
                    .difference(&right_model)
                    .copied()
                    .collect::<RelationSet>();

                let union = left.union(right);
                assert_eq!(union, expected_union);
                assert_eq!(fingerprint(&union), fingerprint(&expected_union));
                assert_eq!(left | right, expected_union);
                let mut assigned_union = left.clone();
                assigned_union |= right;
                assert_eq!(assigned_union, expected_union);
                assert_eq!(left.intersection(right), expected_intersection);
                assert_eq!(left.difference(right), expected_difference);
                assert_eq!(left.is_subset(right), left_model.is_subset(&right_model));
                assert_eq!(
                    left.is_disjoint(right),
                    left_model.is_disjoint(&right_model)
                );
            }
        }
    }
}
