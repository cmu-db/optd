//! This module defines a set that can be used to store an unordered set of
//! columns, as well as implements associated set operations on it

use std::collections::HashSet;
use itertools::Itertools;
use crate::ir::Column;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ColumnSet(HashSet<Column>);

impl std::fmt::Display for ColumnSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.iter().sorted()).finish()
    }
}

impl FromIterator<Column> for ColumnSet {
    fn from_iter<T: IntoIterator<Item = Column>>(iter: T) -> Self {
        ColumnSet(HashSet::from_iter(iter))
    }
}

impl ColumnSet {
    pub fn with_capacity(n: usize) -> Self {
        Self(HashSet::with_capacity(n))
    }
    pub fn contains(&self, column: &Column) -> bool {
        self.0.contains(column)
    }

    pub fn is_subset(&self, other: &ColumnSet) -> bool {
        self.0.is_subset(&other.0)
    }
}

impl std::ops::BitOr<&ColumnSet> for ColumnSet {
    type Output = ColumnSet;

    fn bitor(mut self, rhs: &ColumnSet) -> Self::Output {
        self.0.extend(rhs.0.iter());
        self
    }
}

impl std::ops::BitOr<&ColumnSet> for &ColumnSet {
    type Output = ColumnSet;

    fn bitor(self, rhs: &ColumnSet) -> Self::Output {
        self.0.union(&rhs.0).cloned().collect()
    }
}

impl std::ops::BitOrAssign<&ColumnSet> for ColumnSet {
    fn bitor_assign(&mut self, rhs: &ColumnSet) {
        self.0.extend(rhs.0.iter());
    }
}

impl std::ops::BitAnd<&ColumnSet> for ColumnSet {
    type Output = ColumnSet;

    fn bitand(mut self, rhs: &ColumnSet) -> Self::Output {
        self.0.retain(|column| rhs.contains(column));
        self
    }
}

impl std::ops::BitAnd<&ColumnSet> for &ColumnSet {
    type Output = ColumnSet;

    fn bitand(self, rhs: &ColumnSet) -> Self::Output {
        self.0.intersection(&rhs.0).cloned().collect()
    }
}

impl std::ops::BitAndAssign<&ColumnSet> for ColumnSet {
    fn bitand_assign(&mut self, rhs: &ColumnSet) {
        self.0.retain(|column| rhs.contains(column));
    }
}
