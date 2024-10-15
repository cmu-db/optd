use std::{
    collections::{hash_map, HashMap},
    ops::Index,
};

use itertools::Itertools;
use set::{DisjointSet, UnionFind};

use crate::{
    cascades::{optimizer::ExprId, GroupId},
    rel_node::RelNodeTyp,
};

use super::{Group, RelMemoNodeRef};

pub mod set;

const MISMATCH_ERROR: &str = "`groups` and `id_map` report unmatched group membership";

pub(crate) struct DisjointGroupMap {
    id_map: DisjointSet<GroupId>,
    groups: HashMap<GroupId, Group>,
}

impl DisjointGroupMap {
    /// Creates a new disjoint group instance.
    pub fn new() -> Self {
        DisjointGroupMap {
            id_map: DisjointSet::new(),
            groups: HashMap::new(),
        }
    }

    pub fn get(&self, id: &GroupId) -> Option<&Group> {
        self.id_map
            .find(id)
            .map(|rep| self.groups.get(&rep).expect(MISMATCH_ERROR))
    }

    pub fn get_mut(&mut self, id: &GroupId) -> Option<&mut Group> {
        self.id_map
            .find(id)
            .map(|rep| self.groups.get_mut(&rep).expect(MISMATCH_ERROR))
    }

    unsafe fn insert_new(&mut self, id: GroupId, group: Group) {
        self.id_map.add(id);
        self.groups.insert(id, group);
    }

    /// Merge the group `a` and group `b`. Returns the merged representative group id.
    pub fn merge(&mut self, a: GroupId, b: GroupId) -> GroupId {
        if a == b {
            return a;
        }

        let [rep, other] = self.id_map.union(&a, &b).unwrap();

        // Drain all expressions from group other, copy to its representative
        let other_exprs = self.drain_all_exprs_in(&other);
        let rep_group = self.get_mut(&rep).expect("group not found");
        for expr_id in other_exprs {
            rep_group.group_exprs.insert(expr_id);
        }

        rep
    }

    /// Drain all expressions from the group, returns an iterator of the expressions.
    fn drain_all_exprs_in(&mut self, id: &GroupId) -> impl Iterator<Item = ExprId> {
        let group = self.groups.remove(&id).expect("group not found");
        group.group_exprs.into_iter().sorted()
    }

    pub fn entry(&mut self, id: GroupId) -> GroupEntry<'_> {
        use hash_map::Entry::*;
        let rep = self.id_map.find(&id).unwrap_or(id);
        let id_entry = self.id_map.entry(rep);
        let group_entry = self.groups.entry(rep);
        match (id_entry, group_entry) {
            (Occupied(_), Occupied(inner)) => GroupEntry::Occupied(OccupiedGroupEntry { inner }),
            (Vacant(id), Vacant(inner)) => GroupEntry::Vacant(VacantGroupEntry { id, inner }),
            _ => unreachable!("{MISMATCH_ERROR}"),
        }
    }
}

pub enum GroupEntry<'a> {
    Occupied(OccupiedGroupEntry<'a>),
    Vacant(VacantGroupEntry<'a>),
}

pub struct OccupiedGroupEntry<'a> {
    inner: hash_map::OccupiedEntry<'a, GroupId, Group>,
}

pub struct VacantGroupEntry<'a> {
    id: hash_map::VacantEntry<'a, GroupId, GroupId>,
    inner: hash_map::VacantEntry<'a, GroupId, Group>,
}

impl<'a> OccupiedGroupEntry<'a> {
    pub fn id(&self) -> &GroupId {
        self.inner.key()
    }

    pub fn get(&self) -> &Group {
        self.inner.get()
    }

    pub fn get_mut(&mut self) -> &mut Group {
        self.inner.get_mut()
    }
}

impl<'a> VacantGroupEntry<'a> {
    pub fn id(&self) -> &GroupId {
        self.inner.key()
    }

    pub fn insert(self, group: Group) -> &'a mut Group {
        let id = *self.id();
        self.id.insert(id);
        self.inner.insert(group)
    }
}

impl Index<&GroupId> for DisjointGroupMap {
    type Output = Group;

    fn index(&self, index: &GroupId) -> &Self::Output {
        let rep = self
            .id_map
            .find(index)
            .expect("no group found for group id");

        self.groups.get(&rep).expect(MISMATCH_ERROR)
    }
}
