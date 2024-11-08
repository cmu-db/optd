// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! optd's implementation of disjoint sets (union finds). It's send + sync + serializable.

use std::{collections::HashMap, hash::Hash};
#[derive(Clone, Default)]
pub struct DisjointSets<T: Clone> {
    data_idx: HashMap<T, usize>,
    parents: Vec<usize>,
}

impl<T: Clone> std::fmt::Debug for DisjointSets<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DisjointSets")
    }
}

impl<T: Clone + Eq + PartialEq + Hash> DisjointSets<T> {
    pub fn new() -> Self {
        Self {
            data_idx: HashMap::new(),
            parents: Vec::new(),
        }
    }

    pub fn contains(&self, data: &T) -> bool {
        self.data_idx.contains_key(data)
    }

    #[must_use]
    pub fn make_set(&mut self, data: T) -> Option<()> {
        if self.data_idx.contains_key(&data) {
            return None;
        }
        let idx = self.parents.len();
        self.data_idx.insert(data.clone(), idx);
        self.parents.push(idx);
        Some(())
    }

    fn find(&mut self, mut idx: usize) -> usize {
        while self.parents[idx] != idx {
            self.parents[idx] = self.parents[self.parents[idx]];
            idx = self.parents[idx];
        }
        idx
    }

    fn find_const(&self, mut idx: usize) -> usize {
        while self.parents[idx] != idx {
            idx = self.parents[idx];
        }
        idx
    }

    #[must_use]
    pub fn union(&mut self, data1: &T, data2: &T) -> Option<()> {
        let idx1 = *self.data_idx.get(data1)?;
        let idx2 = *self.data_idx.get(data2)?;
        let parent1 = self.find(idx1);
        let parent2 = self.find(idx2);
        if parent1 != parent2 {
            self.parents[parent1] = parent2;
        }
        Some(())
    }

    pub fn same_set(&self, data1: &T, data2: &T) -> Option<bool> {
        let idx1 = *self.data_idx.get(data1)?;
        let idx2 = *self.data_idx.get(data2)?;
        Some(self.find_const(idx1) == self.find_const(idx2))
    }

    pub fn set_size(&self, data: &T) -> Option<usize> {
        let idx = *self.data_idx.get(data)?;
        let parent = self.find_const(idx);
        Some(
            self.parents
                .iter()
                .filter(|&&x| self.find_const(x) == parent)
                .count(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_union_find() {
        let mut set = DisjointSets::new();
        set.make_set("a").unwrap();
        set.make_set("b").unwrap();
        set.make_set("c").unwrap();
        set.make_set("d").unwrap();
        set.make_set("e").unwrap();
        assert!(set.same_set(&"a", &"a").unwrap());
        assert!(!set.same_set(&"a", &"b").unwrap());
        assert_eq!(set.set_size(&"a").unwrap(), 1);
        assert_eq!(set.set_size(&"c").unwrap(), 1);
        set.union(&"a", &"b").unwrap();
        assert_eq!(set.set_size(&"a").unwrap(), 2);
        assert_eq!(set.set_size(&"c").unwrap(), 1);
        assert!(set.same_set(&"a", &"b").unwrap());
        assert!(!set.same_set(&"a", &"c").unwrap());
        set.union(&"b", &"c").unwrap();
        assert!(set.same_set(&"a", &"c").unwrap());
        assert!(!set.same_set(&"a", &"d").unwrap());
        assert_eq!(set.set_size(&"a").unwrap(), 3);
        assert_eq!(set.set_size(&"d").unwrap(), 1);
        set.union(&"d", &"e").unwrap();
        assert!(set.same_set(&"d", &"e").unwrap());
        assert!(!set.same_set(&"a", &"d").unwrap());
        assert_eq!(set.set_size(&"a").unwrap(), 3);
        assert_eq!(set.set_size(&"d").unwrap(), 2);
        set.union(&"c", &"e").unwrap();
        assert!(set.same_set(&"a", &"e").unwrap());
        assert_eq!(set.set_size(&"d").unwrap(), 5);
    }
}
