use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::{Arc, atomic::AtomicI64},
};

use itertools::Itertools;
use tokio::sync::watch;
use tracing::trace;

use crate::{
    ir::{
        GroupId, Operator, OperatorKind, Scalar,
        cost::Cost,
        properties::{OperatorProperties, required::Required},
    },
    utility::union_find::UnionFind,
};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct MemoGroupExpr {
    meta: OperatorKind,
    inputs: Box<[GroupId]>,
    split: usize,
}

impl MemoGroupExpr {
    pub fn new(meta: OperatorKind, inputs: Box<[GroupId]>, split: usize) -> Self {
        Self {
            meta,
            inputs,
            split,
        }
    }

    pub fn input_operators(&self) -> &[GroupId] {
        &self.inputs[..self.split]
    }

    pub fn input_scalars(&self) -> &[GroupId] {
        &self.inputs[self.split..]
    }

    pub fn clone_with_inputs(&self, inputs: Box<[GroupId]>) -> Self {
        Self {
            meta: self.meta.clone(),
            inputs,
            split: self.split,
        }
    }
}

impl std::fmt::Debug for MemoGroupExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoGroupExpr")
            .field("meta", &self.meta)
            .field("ops", &self.input_operators())
            .field("scalars", &self.input_scalars())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(i64);

impl Id {
    pub const UNKNOWN: Self = Id(0);
}

impl From<GroupId> for Id {
    fn from(value: GroupId) -> Self {
        Id(value.0)
    }
}

impl From<Id> for GroupId {
    fn from(value: Id) -> Self {
        GroupId(value.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WithId<K> {
    id: Id,
    key: K,
}

impl<K> WithId<K> {
    pub const fn unknown(key: K) -> Self {
        Self {
            id: Id::UNKNOWN,
            key,
        }
    }

    pub const fn new(id: Id, key: K) -> Self {
        Self { id, key }
    }
    pub const fn id(&self) -> Id {
        self.id
    }
}

impl<K> From<K> for WithId<K> {
    fn from(value: K) -> Self {
        WithId::unknown(value)
    }
}

#[derive(Default)]
pub struct MemoTable {
    /// Scalar deduplication.
    scalar_dedup: HashMap<Arc<Scalar>, GroupId>,
    scalar_id_to_key: HashMap<GroupId, Arc<Scalar>>,
    /// Operator deduplication.
    operator_dedup: HashMap<Arc<MemoGroupExpr>, Id>,
    /// Operator Id to
    id_to_group_ids: UnionFind<GroupId>,
    groups: BTreeMap<GroupId, MemoGroup>,
    id_allocator: IdAllocator,
}

impl MemoTable {
    /// Adds an operator to the memo table.
    ///
    /// Returns the group id where the operator belongs:
    /// - If it's a new operator: creates a new memo group and returns its id.
    /// - If it already exists: returns the existing group id.
    ///
    /// **Note:** This would not trigger group merges.
    pub fn insert_new_operator(&mut self, operator: Arc<Operator>) -> Result<GroupId, GroupId> {
        self.insert_operator(operator.clone()).map(|first_expr| {
            trace!("obtain new expr: {:?}", first_expr);
            let id = first_expr.id();
            let memo_group = MemoGroup::new(first_expr, operator.properties().clone());
            let res = self.groups.insert(GroupId::from(id), memo_group);
            assert!(res.is_none());
            GroupId::from(id)
        })
    }

    /// Inserts an operator into a specific memo group.
    ///
    /// If the operator is new:
    /// - Adds it as a new expression to the target group
    /// - Returns the new expression
    ///
    /// If the operator already exists in another group:
    /// - Merges that group with the target group
    /// - Returns an error with the target group id.
    ///
    /// **Note:** This may trigger cascading group merges.
    pub fn insert_operator_into_group(
        &mut self,
        operator: Arc<Operator>,
        into_group_id: GroupId,
    ) -> Result<WithId<Arc<MemoGroupExpr>>, GroupId> {
        let res = self.insert_operator(operator.clone());
        let into_group_id = self.id_to_group_ids.find(&into_group_id);
        res.inspect(|expr| {
            trace!("obtain new expr: {:?}", expr);
            let group = self.groups.get(&into_group_id).unwrap();
            self.id_to_group_ids
                .merge(&into_group_id, &GroupId::from(expr.id()));
            group.exploration.send_modify(|exploration| {
                exploration.exprs.push(expr.clone());
            });
        })
        .map_err(|from_group_id| {
            trace!(
                "got existing group {}, group merges triggered:",
                from_group_id
            );
            self.dump();
            self.merge_group(into_group_id, from_group_id);
            trace!("group merging finished:");
            self.dump();
            into_group_id
        })
    }
    /// Inserts an operator into the memo table and returns its memo expression.
    ///
    /// This is the core method for adding operators to the memo table. It recursively processes
    /// all input operators and scalars:
    /// - If the operator is new: creates a new memo expression and returns it
    /// - If the operator already exists: returns an error with the existing group id
    ///
    /// **Note:** This method handles recursive insertion of child operators and scalars.
    pub fn insert_operator(
        &mut self,
        operator: Arc<Operator>,
    ) -> Result<WithId<Arc<MemoGroupExpr>>, GroupId> {
        if let OperatorKind::Group(group) = &operator.kind {
            let repr_id = self.id_to_group_ids.find(&group.group_id);
            trace!("inserted group {}", repr_id);
            return Err(GroupId::from(repr_id));
        }

        // Split point = len(input_operators)
        let split = operator.input_operators().len();
        let mut inputs = operator
            .input_operators()
            .iter()
            .map(|op| {
                self.insert_operator(op.clone())
                    .map(|first_expr| {
                        trace!("obtain new expr: {:?}", first_expr);
                        let group_id = GroupId::from(first_expr.id());
                        let memo_group = MemoGroup::new(first_expr, op.properties().clone());
                        let res = self.groups.insert(group_id, memo_group);
                        assert!(res.is_none());
                        group_id
                    })
                    .unwrap_or_else(|group_id| {
                        trace!("got existing group: {}", group_id);
                        group_id
                    })
            })
            .collect_vec();

        inputs.extend(
            operator
                .input_scalars()
                .iter()
                .map(|s| self.insert_scalar(s.clone()).unwrap_or_else(|id| id)),
        );

        let group_expr = Arc::new(MemoGroupExpr::new(
            operator.kind.clone(),
            inputs.into_boxed_slice(),
            split,
        ));

        use std::collections::hash_map::Entry;
        match self.operator_dedup.entry(group_expr.clone()) {
            Entry::Occupied(occupied) => {
                let id = occupied.get();
                Err(GroupId::from(
                    self.id_to_group_ids.find(&GroupId::from(*id)),
                ))
            }
            Entry::Vacant(vacant) => {
                let id = self.id_allocator.next_id();
                vacant.insert(id);
                let key_with_id = WithId::new(id, group_expr);
                Ok(key_with_id)
            }
        }
    }

    /// Inserts a scalar into the memo table's scalar deduplication map.
    ///
    /// Handles scalar deduplication and group id assignment:
    /// - If the scalar is new: creates a new group id and returns it
    /// - If the scalar already exists: returns an error with the existing group id
    fn insert_scalar(&mut self, scalar: Arc<Scalar>) -> Result<GroupId, GroupId> {
        use std::collections::hash_map::Entry;
        match self.scalar_dedup.entry(scalar.clone()) {
            Entry::Occupied(occupied) => {
                let id = occupied.get();
                assert!(self.scalar_id_to_key.contains_key(id));
                trace!("got existing scalar with {:?}", id);
                Err(*id)
            }
            Entry::Vacant(vacant) => {
                let group_id = GroupId::from(self.id_allocator.next_id());
                vacant.insert(group_id);
                self.scalar_id_to_key.insert(group_id, scalar);
                trace!("got new scalar with {:?}", group_id);
                Ok(group_id)
            }
        }
    }

    /// Retrieves a scalar value by its group id.
    ///
    /// Returns the scalar associated with the given group id:
    /// - If the group id corresponds to a scalar: returns `Some(scalar)`
    /// - If the group id is not found or is not a scalar: returns `None`
    pub fn get_scalar(&self, group_id: &GroupId) -> Option<Arc<Scalar>> {
        self.scalar_id_to_key.get(group_id).cloned()
    }

    /// Gets the memo group corresponding to a group id.
    ///
    /// Uses the union-find structure to resolve the representative group id and returns
    /// the associated memo group:
    /// - Finds the representative group id using union-find
    /// - Returns a reference to the corresponding memo group
    pub fn get_memo_group(&self, group_id: &GroupId) -> &MemoGroup {
        let repr_group_id = self.id_to_group_ids.find(group_id);
        self.groups.get(&repr_group_id).unwrap()
    }

    /// Merges two memo groups into one, combining their expressions.
    ///
    /// Transfers all expressions from the source group to the target group and updates
    /// all references throughout the memo table:
    /// - Moves expressions from `from_group_id` to `into_group_id`
    /// - Updates operator deduplication map with merged expressions
    /// - Handles cascading group merges when expressions become duplicated
    /// - Uses union-find to track group equivalences
    ///
    /// **Note:** This operation may trigger additional group merges recursively.
    fn merge_group(&mut self, into_group_id: GroupId, from_group_id: GroupId) {
        trace!("merging {} <- {}", into_group_id, from_group_id);
        if into_group_id == from_group_id {
            return;
        }

        let from_group = self.groups.remove(&from_group_id).unwrap();
        let into_group = self.groups.get(&into_group_id).unwrap();

        let mut from_group_exprs = Vec::new();
        from_group.exploration.send_modify(|state| {
            std::mem::swap(&mut state.exprs, &mut from_group_exprs);
            state.status = Status::Obsolete;
        });

        // After this point all receiver will notice the sender got dropped.
        drop(from_group);

        // TODO(yuchen): What about Optimization?
        // As of writing, we do not merge optimization entries, meaning that
        // existing optimization progress for `from_group` is lost.
        // Might be a simple hash map merge and resolve.
        into_group.exploration.send_modify(|state| {
            state.exprs.extend(from_group_exprs);
        });

        self.id_to_group_ids.merge(&into_group_id, &from_group_id);

        let mut pending_group_merges = Vec::new();
        for (group_id, group) in self.groups.iter_mut() {
            // design: group exprs are cloned so we don't hold the lock on `exploration` for too long.
            let mut group_exprs = group.exploration.borrow().exprs.clone();
            group_exprs.iter_mut().for_each(|expr| {
                let input_groups = expr.key.input_operators();

                if input_groups.contains(&from_group_id) {
                    let inputs_after_merge = input_groups
                        .iter()
                        .map(|group_id| {
                            if group_id.eq(&from_group_id) {
                                &into_group_id
                            } else {
                                group_id
                            }
                        })
                        .chain(expr.key.input_scalars())
                        .cloned()
                        .collect::<Box<[GroupId]>>();
                    let new_key = Arc::new(expr.key.clone_with_inputs(inputs_after_merge));
                    self.operator_dedup.remove(&expr.key);

                    use std::collections::hash_map::Entry;
                    match self.operator_dedup.entry(new_key.clone()) {
                        Entry::Occupied(occupied) => {
                            let dup_expr_id = *occupied.get();
                            let dup_group_id =
                                self.id_to_group_ids.find(&GroupId::from(dup_expr_id));
                            if dup_group_id != *group_id {
                                pending_group_merges.push((dup_group_id, *group_id));
                            }
                            *expr = WithId::new(dup_expr_id, occupied.key().clone());
                        }
                        Entry::Vacant(vacant) => {
                            vacant.insert(expr.id());
                            *expr = WithId::new(expr.id(), new_key);
                        }
                    }
                }
            });

            // Deduplication is needed so we don't have duplicated expressions in a memo group.
            group_exprs.dedup_by_key(|key| key.id());
            group.exploration.send_modify(|state| {
                std::mem::swap(&mut state.exprs, &mut group_exprs);
            });
            group_exprs.clear();
        }
        trace!(?pending_group_merges);
        for (into_group_id, from_group_id) in pending_group_merges {
            let into_group_id = self.id_to_group_ids.find(&into_group_id);
            let from_group_id = self.id_to_group_ids.find(&from_group_id);
            self.merge_group(into_group_id, from_group_id);
        }
    }

    /// Prints a human-readable representation of the memo table contents.
    ///
    /// Outputs all memo groups and their expressions to stdout for debugging purposes:
    /// - Shows group ids and number of expressions per group
    /// - Lists all expressions within each group with their ids and details
    ///
    /// This method is primarily intended for debugging and testing.
    pub fn dump(&self) {
        trace!("======== MEMO DUMP BEGIN ========");
        for (group_id, group) in &self.groups {
            let exploration = group.exploration.borrow();
            assert_eq!(group_id, &group.group_id);
            trace!(
                "MemoGroup ({}, num_exprs={}):",
                group_id,
                exploration.exprs.len()
            );

            exploration.exprs.iter().for_each(|expr| {
                trace!("{:?} -> {:?}", expr.id(), &expr.key);
            });
        }
        trace!("======== MEMO DUMP END ==========");
    }
}

pub struct IdAllocator {
    next_id: AtomicI64,
}

impl Default for IdAllocator {
    fn default() -> Self {
        Self {
            next_id: AtomicI64::new(1),
        }
    }
}

impl IdAllocator {
    pub fn next_id(&self) -> Id {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Id(id)
    }
}

pub enum Status {
    NotStarted,
    InProgress,
    Complete,
    Obsolete,
}

pub struct Exploration {
    exprs: Vec<WithId<Arc<MemoGroupExpr>>>,
    properties: Arc<OperatorProperties>,
    status: Status,
}

impl Exploration {
    pub fn new(
        first_expr: WithId<Arc<MemoGroupExpr>>,
        properties: Arc<OperatorProperties>,
    ) -> Self {
        Self {
            exprs: vec![first_expr],
            status: Status::NotStarted,
            properties,
        }
    }
}

pub struct CostedExpr {
    pub group_expr: WithId<Arc<MemoGroupExpr>>,
    pub operator_cost: Cost,
    pub total_cost: Cost,
    /// The input requirements and the index of the costed expressions for the inputs.
    pub input_requirements: Arc<[(Required, usize)]>,
}

pub struct Optimization {
    pub costed_exprs: Vec<CostedExpr>,
    pub enforcers: Vec<MemoGroupExpr>,
    pub status: Status,
}

pub struct MemoGroup {
    group_id: GroupId,
    exploration: watch::Sender<Exploration>,
    optimizations: HashMap<Required, watch::Sender<Optimization>>,
}

impl MemoGroup {
    /// Creates a new memo group with its first expression and properties.
    pub fn new(
        first_expr: WithId<Arc<MemoGroupExpr>>,
        properties: Arc<OperatorProperties>,
    ) -> Self {
        let group_id = GroupId::from(first_expr.id());
        let exploration = watch::Sender::new(Exploration::new(first_expr, properties));
        Self {
            group_id,
            exploration,
            optimizations: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::ir::{
        Column, IRContext, builder::*, explain::quick_explain, operator::join::JoinType,
    };

    #[test]
    fn insert_scalar() {
        let mut memo = MemoTable::default();
        let scalar = column_ref(Column(1)).equal(int32(799));
        let scalar_from_clone = scalar.clone();
        let scalar_dup = column_ref(Column(1)).equal(int32(799));
        let id = memo.insert_scalar(scalar).unwrap();
        let res = memo.insert_scalar(scalar_from_clone);
        assert_eq!(Err(id), res);
        let res = memo.insert_scalar(scalar_dup);
        assert_eq!(Err(id), res);
    }

    #[test]
    fn insert_new_operator() {
        let mut memo = MemoTable::default();
        let join = mock_scan(1, vec![1], 0.).logical_join(
            mock_scan(2, vec![2], 0.),
            boolean(true),
            JoinType::Inner,
        );

        let join_dup = mock_scan(1, vec![1], 0.).logical_join(
            mock_scan(2, vec![2], 0.),
            boolean(true),
            JoinType::Inner,
        );

        let group_id = memo.insert_new_operator(join.clone()).unwrap();
        let res = memo.insert_new_operator(join);
        assert_eq!(Err(group_id), res);
        let res = memo.insert_new_operator(join_dup);
        assert_eq!(Err(group_id), res);
    }

    #[test]
    fn insert_operator_into_group() {
        let mut memo = MemoTable::default();
        let join = mock_scan(1, vec![1], 0.).logical_join(
            mock_scan(2, vec![2], 0.),
            boolean(true),
            JoinType::Inner,
        );
        let group_id = memo.insert_new_operator(join).unwrap();

        let join_commuted = mock_scan(2, vec![2], 0.).logical_join(
            mock_scan(1, vec![1], 0.),
            boolean(true),
            JoinType::Inner,
        );
        let res = memo.insert_operator_into_group(join_commuted.clone(), group_id);
        assert!(res.is_ok());

        let res = memo.insert_operator_into_group(join_commuted, group_id);
        assert!(res.is_err());

        let group = memo.get_memo_group(&group_id);
        assert_eq!(2, group.exploration.borrow().exprs.len());
    }

    #[test]
    fn parent_group_merge() {
        let mut memo = MemoTable::default();

        let m1 = mock_scan(1, vec![1], 0.);
        let m1_alias = mock_scan(2, vec![1], 0.);

        let g1 = memo
            .insert_new_operator(m1.clone().logical_select(boolean(true)))
            .unwrap();

        let g2 = memo
            .insert_new_operator(m1_alias.clone().logical_select(boolean(true)))
            .unwrap();

        let m1_group_id = memo.insert_operator(m1.clone()).unwrap_err();
        let res = memo.insert_operator_into_group(m1_alias, m1_group_id);
        assert_eq!(Err(m1_group_id), res);

        assert_eq!(
            memo.id_to_group_ids.find(&g1),
            memo.id_to_group_ids.find(&g2)
        );

        let g1_group = memo.get_memo_group(&g1);
        let g2_group = memo.get_memo_group(&g2);
        assert_eq!(g1_group.group_id, g2_group.group_id);
    }

    #[test]
    #[tracing_test::traced_test]
    fn cascading_group_merges() {
        let mut memo = MemoTable::default();

        let m1 = mock_scan(1, vec![1], 0.);
        let ctx = IRContext::with_empty_magic();
        trace!("\n{}", quick_explain(&m1, &ctx));
        let m1_alias = mock_scan(2, vec![1], 0.);

        let g1 = memo
            .insert_new_operator(
                m1.clone()
                    .logical_select(boolean(true))
                    .logical_select(boolean(true)),
            )
            .unwrap();

        let g2 = memo
            .insert_new_operator(
                m1_alias
                    .clone()
                    .logical_select(boolean(true))
                    .logical_select(boolean(true)),
            )
            .unwrap();

        let m1_group_id = memo.insert_operator(m1.clone()).unwrap_err();
        let res = memo.insert_operator_into_group(m1_alias, m1_group_id);
        assert_eq!(Err(m1_group_id), res);

        assert_eq!(
            memo.id_to_group_ids.find(&g1),
            memo.id_to_group_ids.find(&g2)
        );

        let g1_group = memo.get_memo_group(&g1);
        let g2_group = memo.get_memo_group(&g2);
        assert_eq!(g1_group.group_id, g2_group.group_id);
        assert_eq!(1, g1_group.exploration.borrow().exprs.len());
    }

    #[test]
    fn insert_partial_binding() {
        let mut memo = MemoTable::default();

        let m1 = mock_scan(1, vec![1], 0.);
        let m1_alias = mock_scan(2, vec![1], 0.);
        memo.insert_new_operator(
            m1.clone()
                .logical_select(boolean(true))
                .logical_select(boolean(true)),
        )
        .unwrap();

        memo.insert_new_operator(
            m1_alias
                .clone()
                .logical_select(boolean(true))
                .logical_select(boolean(true)),
        )
        .unwrap();

        let m1_group_id = memo.insert_operator(m1.clone()).unwrap_err();

        let properties = memo
            .get_memo_group(&m1_group_id)
            .exploration
            .borrow()
            .properties
            .clone();

        let m1_select_binding = group(m1_group_id, properties).logical_select(boolean(true));

        let into_group_id = memo
            .insert_operator(m1_alias.clone().logical_select(boolean(true)))
            .unwrap_err();

        let res = memo.insert_operator_into_group(m1_select_binding, into_group_id);
        assert_eq!(Err(into_group_id), res);
    }
}
