use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use anyhow::{bail, Result};
use itertools::Itertools;
use std::any::Any;
use tracing::trace;

use crate::{
    cost::Cost,
    property::PropertyBuilderAny,
    rel_node::{RelNode, RelNodeMeta, RelNodeMetaMap, RelNodeRef, RelNodeTyp, Value},
};

use super::optimizer::{ExprId, GroupId};

pub type RelMemoNodeRef<T> = Arc<RelMemoNode<T>>;

/// Equivalent to MExpr in Columbia/Cascades.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelMemoNode<T: RelNodeTyp> {
    pub typ: T,
    pub children: Vec<GroupId>,
    pub data: Option<Value>,
}

impl<T: RelNodeTyp> RelMemoNode<T> {
    pub fn into_rel_node(self) -> RelNode<T> {
        RelNode {
            typ: self.typ,
            children: self
                .children
                .into_iter()
                .map(|x| Arc::new(RelNode::new_group(x)))
                .collect(),
            data: self.data,
        }
    }
}

impl<T: RelNodeTyp> std::fmt::Display for RelMemoNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        if let Some(ref data) = self.data {
            write!(f, " {}", data)?;
        }
        for child in &self.children {
            write!(f, " {}", child)?;
        }
        write!(f, ")")
    }
}

#[derive(Default, Debug, Clone)]
pub struct Winner {
    pub impossible: bool,
    pub expr_id: ExprId,
    pub cost: Cost,
}

#[derive(Default, Debug, Clone)]
pub struct GroupInfo {
    pub winner: Option<Winner>,
}

pub(crate) struct Group {
    pub(crate) group_exprs: HashSet<ExprId>,
    pub(crate) info: GroupInfo,
    pub(crate) properties: Arc<[Box<dyn Any + Send + Sync + 'static>]>,
}

pub struct Memo<T: RelNodeTyp> {
    // Source of truth.
    groups: HashMap<GroupId, Group>,
    expr_id_to_expr_node: HashMap<ExprId, RelMemoNodeRef<T>>,

    // Internal states.
    group_expr_counter: usize,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,

    // Indexes.
    expr_node_to_expr_id: HashMap<RelMemoNode<T>, ExprId>,
    expr_id_to_group_id: HashMap<ExprId, GroupId>,

    // We update all group IDs in the memo table upon group merging, but
    // there might be edge cases that some tasks still hold the old group ID.
    // In this case, we need this mapping to redirect to the merged group ID.
    merged_group_mapping: HashMap<GroupId, GroupId>,
    dup_expr_mapping: HashMap<ExprId, ExprId>,
}

impl<T: RelNodeTyp> Memo<T> {
    pub fn new(property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>) -> Self {
        Self {
            expr_id_to_group_id: HashMap::new(),
            expr_id_to_expr_node: HashMap::new(),
            expr_node_to_expr_id: HashMap::new(),
            groups: HashMap::new(),
            group_expr_counter: 0,
            merged_group_mapping: HashMap::new(),
            property_builders,
            dup_expr_mapping: HashMap::new(),
        }
    }

    /// Get the next group id. Group id and expr id shares the same counter, so as to make it easier to debug...
    fn next_group_id(&mut self) -> GroupId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        GroupId(id)
    }

    /// Get the next expr id. Group id and expr id shares the same counter, so as to make it easier to debug...
    fn next_expr_id(&mut self) -> ExprId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        ExprId(id)
    }

    fn verify_integrity(&self) {
        if cfg!(debug_assertions) {
            let num_of_exprs = self.expr_id_to_expr_node.len();
            assert_eq!(num_of_exprs, self.expr_node_to_expr_id.len());
            assert_eq!(num_of_exprs, self.expr_id_to_group_id.len());

            let mut valid_groups = HashSet::new();
            for to in self.merged_group_mapping.values() {
                assert_eq!(self.merged_group_mapping[to], *to);
                valid_groups.insert(*to);
            }
            assert_eq!(valid_groups.len(), self.groups.len());

            for (id, node) in self.expr_id_to_expr_node.iter() {
                assert_eq!(self.expr_node_to_expr_id[node], *id);
                for child in &node.children {
                    assert!(
                        valid_groups.contains(child),
                        "invalid group used in expression {}, where {} does not exist any more",
                        node,
                        child
                    );
                }
            }

            let mut cnt = 0;
            for (group_id, group) in &self.groups {
                assert!(valid_groups.contains(group_id));
                cnt += group.group_exprs.len();
                assert!(!group.group_exprs.is_empty());
                for expr in &group.group_exprs {
                    assert_eq!(self.expr_id_to_group_id[expr], *group_id);
                }
            }
            assert_eq!(cnt, num_of_exprs);
        }
    }

    #[allow(dead_code)]
    fn merge_group(&mut self, group_a: GroupId, group_b: GroupId) -> GroupId {
        use std::cmp::Ordering;
        let group_a = self.reduce_group(group_a);
        let group_b = self.reduce_group(group_b);
        let (merge_into, merge_from) = match group_a.0.cmp(&group_b.0) {
            Ordering::Less => (group_a, group_b),
            Ordering::Equal => return group_a,
            Ordering::Greater => (group_b, group_a),
        };
        self.merge_group_inner(merge_into, merge_from);
        self.verify_integrity();
        merge_into
    }

    /// Add an expression into the memo, returns the group id and the expr id.
    pub fn add_new_expr(&mut self, rel_node: RelNodeRef<T>) -> (GroupId, ExprId) {
        let (group_id, expr_id) = self
            .add_new_group_expr_inner(rel_node, None)
            .expect("should not trigger merge group");
        self.verify_integrity();
        (group_id, expr_id)
    }

    /// Add an expression into the memo, returns the expr id if rel_node is NOT a group.
    pub fn add_expr_to_group(
        &mut self,
        rel_node: RelNodeRef<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        if let Some(input_group) = rel_node.typ.extract_group() {
            let input_group = self.reduce_group(input_group);
            let group_id = self.reduce_group(group_id);
            self.merge_group_inner(input_group, group_id);
            return None;
        }
        let reduced_group_id = self.reduce_group(group_id);
        let (returned_group_id, expr_id) = self
            .add_new_group_expr_inner(rel_node, Some(reduced_group_id))
            .unwrap();
        assert_eq!(returned_group_id, reduced_group_id);
        self.verify_integrity();
        Some(expr_id)
    }

    fn reduce_group(&self, group_id: GroupId) -> GroupId {
        self.merged_group_mapping[&group_id]
    }

    fn merge_group_inner(&mut self, merge_into: GroupId, merge_from: GroupId) {
        if merge_into == merge_from {
            return;
        }
        trace!(event = "merge_group", merge_into = %merge_into, merge_from = %merge_from);
        let group_merge_from = self.groups.remove(&merge_from).unwrap();
        let group_merge_into = self.groups.get_mut(&merge_into).unwrap();
        // TODO: update winner, cost and properties
        for from_expr in group_merge_from.group_exprs {
            let ret = self.expr_id_to_group_id.insert(from_expr, merge_into);
            assert!(ret.is_some());
            group_merge_into.group_exprs.insert(from_expr);
        }
        self.merged_group_mapping.insert(merge_from, merge_into);

        // Update all indexes and other data structures
        // 1. update merged group mapping -- could be optimized with union find
        for (_, mapped_to) in self.merged_group_mapping.iter_mut() {
            if *mapped_to == merge_from {
                *mapped_to = merge_into;
            }
        }

        let mut pending_recursive_merge = Vec::new();
        // 2. update all group expressions and indexes
        for (group_id, group) in self.groups.iter_mut() {
            let mut new_expr_list = HashSet::new();
            for expr_id in group.group_exprs.iter() {
                let expr = self.expr_id_to_expr_node[expr_id].clone();
                if expr.children.contains(&merge_from) {
                    // Create the new expr node
                    let old_expr = expr.as_ref().clone();
                    let mut new_expr = expr.as_ref().clone();
                    new_expr.children.iter_mut().for_each(|x| {
                        if *x == merge_from {
                            *x = merge_into;
                        }
                    });
                    // Update all existing entries and indexes
                    self.expr_id_to_expr_node
                        .insert(*expr_id, Arc::new(new_expr.clone()));
                    self.expr_node_to_expr_id.remove(&old_expr);
                    if let Some(dup_expr) = self.expr_node_to_expr_id.get(&new_expr) {
                        // If new_expr == some_other_old_expr in the memo table, unless they belong to the same group,
                        // we should merge the two groups. This should not happen. We should simply drop this expression.
                        let dup_group_id = self.expr_id_to_group_id[dup_expr];
                        if dup_group_id != *group_id {
                            pending_recursive_merge.push((dup_group_id, *group_id));
                        }
                        self.expr_id_to_expr_node.remove(expr_id);
                        self.expr_id_to_group_id.remove(expr_id);
                        self.dup_expr_mapping.insert(*expr_id, *dup_expr);
                        new_expr_list.insert(*dup_expr); // adding this temporarily -- should be removed once recursive merge finishes
                    } else {
                        self.expr_node_to_expr_id.insert(new_expr, *expr_id);
                        new_expr_list.insert(*expr_id);
                    }
                } else {
                    new_expr_list.insert(*expr_id);
                }
            }
            assert!(!new_expr_list.is_empty());
            group.group_exprs = new_expr_list;
        }
        for (merge_from, merge_into) in pending_recursive_merge {
            // We need to reduce because each merge would probably invalidate some groups in the last loop iteration.
            let merge_from = self.reduce_group(merge_from);
            let merge_into = self.reduce_group(merge_into);
            self.merge_group_inner(merge_into, merge_from);
        }
    }

    fn add_new_group_expr_inner(
        &mut self,
        rel_node: RelNodeRef<T>,
        add_to_group_id: Option<GroupId>,
    ) -> anyhow::Result<(GroupId, ExprId)> {
        assert!(rel_node.typ.extract_group().is_none());
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| {
                if let Some(group) = child.typ.extract_group() {
                    self.reduce_group(group) // TODO: can I remove?
                } else {
                    // No merge / modification to the memo should occur for the following operation
                    let (group, _) = self
                        .add_new_group_expr_inner(child.clone(), None)
                        .expect("should not trigger merge group");
                    self.reduce_group(group) // TODO: can I remove?
                }
            })
            .collect::<Vec<_>>();
        let memo_node = RelMemoNode {
            typ: rel_node.typ.clone(),
            children: children_group_ids,
            data: rel_node.data.clone(),
        };
        if let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) {
            let group_id = self.expr_id_to_group_id[&expr_id];
            if let Some(add_to_group_id) = add_to_group_id {
                let add_to_group_id = self.reduce_group(add_to_group_id);
                self.merge_group_inner(add_to_group_id, group_id);
                return Ok((add_to_group_id, expr_id));
            }
            return Ok((group_id, expr_id));
        }
        let expr_id = self.next_expr_id();
        let group_id = if let Some(group_id) = add_to_group_id {
            group_id
        } else {
            self.next_group_id()
        };
        self.expr_id_to_expr_node
            .insert(expr_id, memo_node.clone().into());
        self.expr_id_to_group_id.insert(expr_id, group_id);
        self.expr_node_to_expr_id.insert(memo_node.clone(), expr_id);
        self.append_expr_to_group(expr_id, group_id, memo_node);
        Ok((group_id, expr_id))
    }

    /// This is inefficient: usually the optimizer should have a MemoRef instead of passing the full rel node.
    pub fn get_expr_info(&self, rel_node: RelNodeRef<T>) -> (GroupId, ExprId) {
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| {
                if let Some(group) = child.typ.extract_group() {
                    group
                } else {
                    self.get_expr_info(child.clone()).0
                }
            })
            .collect::<Vec<_>>();
        let memo_node = RelMemoNode {
            typ: rel_node.typ.clone(),
            children: children_group_ids,
            data: rel_node.data.clone(),
        };
        let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) else {
            unreachable!("not found {}", memo_node)
        };
        let group_id = self.expr_id_to_group_id[&expr_id];
        (group_id, expr_id)
    }

    fn infer_properties(
        &self,
        memo_node: RelMemoNode<T>,
    ) -> Vec<Box<dyn Any + 'static + Send + Sync>> {
        let child_properties = memo_node
            .children
            .iter()
            .map(|child| self.groups[child].properties.clone())
            .collect_vec();
        let mut props = Vec::with_capacity(self.property_builders.len());
        for (id, builder) in self.property_builders.iter().enumerate() {
            let child_properties = child_properties
                .iter()
                .map(|x| x[id].as_ref() as &dyn std::any::Any)
                .collect::<Vec<_>>();
            let prop = builder.derive_any(
                memo_node.typ.clone(),
                memo_node.data.clone(),
                child_properties.as_slice(),
            );
            props.push(prop);
        }
        props
    }

    /// If group_id exists, it adds expr_id to the existing group
    /// Otherwise, it creates a new group of that group_id and insert expr_id into the new group
    fn append_expr_to_group(
        &mut self,
        expr_id: ExprId,
        group_id: GroupId,
        memo_node: RelMemoNode<T>,
    ) {
        trace!(event = "add_expr_to_group", group_id = %group_id, expr_id = %expr_id, memo_node = %memo_node);
        if let Entry::Occupied(mut entry) = self.groups.entry(group_id) {
            let group = entry.get_mut();
            group.group_exprs.insert(expr_id);
            return;
        }
        // Create group and infer properties (only upon initializing a group).
        let mut group = Group {
            group_exprs: HashSet::new(),
            info: GroupInfo::default(),
            properties: self.infer_properties(memo_node).into(),
        };
        group.group_exprs.insert(expr_id);
        self.groups.insert(group_id, group);
        self.merged_group_mapping.insert(group_id, group_id);
    }

    /// Get the group id of an expression.
    /// The group id is volatile, depending on whether the groups are merged.
    pub fn get_group_id(&self, mut expr_id: ExprId) -> GroupId {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        *self
            .expr_id_to_group_id
            .get(&expr_id)
            .expect("expr not found in group mapping")
    }

    /// Get the memoized representation of a node, only for debugging purpose
    pub fn get_expr_memoed(&self, mut expr_id: ExprId) -> RelMemoNodeRef<T> {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }
        self.expr_id_to_expr_node
            .get(&expr_id)
            .expect("expr not found in expr mapping")
            .clone()
    }

    pub fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        let group_id = self.reduce_group(group_id);
        let group = self.groups.get(&group_id).expect("group not found");
        let mut exprs = group.group_exprs.iter().copied().collect_vec();
        exprs.sort();
        exprs
    }

    pub(crate) fn get_all_group_ids(&self) -> Vec<GroupId> {
        let mut ids = self.groups.keys().copied().collect_vec();
        ids.sort();
        ids
    }

    pub(crate) fn get_group_info(&self, group_id: GroupId) -> GroupInfo {
        let group_id = self.reduce_group(group_id);
        self.groups.get(&group_id).as_ref().unwrap().info.clone()
    }

    pub(crate) fn get_group(&self, group_id: GroupId) -> &Group {
        let group_id = self.reduce_group(group_id);
        self.groups.get(&group_id).as_ref().unwrap()
    }

    pub fn update_group_info(&mut self, group_id: GroupId, group_info: GroupInfo) {
        if let Some(ref winner) = group_info.winner {
            if !winner.impossible {
                assert!(
                    winner.cost.0[0] != 0.0,
                    "{}",
                    self.expr_id_to_expr_node[&winner.expr_id]
                );
            }
        }
        let grp = self.groups.get_mut(&group_id);
        grp.unwrap().info = group_info;
    }

    /// Get all bindings of a predicate group. Will panic if the group contains more than one bindings.
    pub fn get_predicate_binding(&self, group_id: GroupId) -> Option<RelNodeRef<T>> {
        self.get_predicate_binding_group_inner(group_id)
    }

    fn get_predicate_binding_expr_inner(&self, expr_id: ExprId) -> Option<RelNodeRef<T>> {
        let expr = self.expr_id_to_expr_node[&expr_id].clone();
        let mut children = Vec::with_capacity(expr.children.len());
        for child in expr.children.iter() {
            if let Some(child) = self.get_predicate_binding_group_inner(*child) {
                children.push(child);
            } else {
                return None;
            }
        }
        Some(Arc::new(RelNode {
            typ: expr.typ.clone(),
            data: expr.data.clone(),
            children,
        }))
    }

    fn get_predicate_binding_group_inner(&self, group_id: GroupId) -> Option<RelNodeRef<T>> {
        let exprs = &self.groups[&group_id].group_exprs;
        match exprs.len() {
            0 => None,
            1 => self.get_predicate_binding_expr_inner(*exprs.iter().next().unwrap()),
            len => panic!("group {group_id} has {len} expressions"),
        }
    }

    pub fn get_best_group_binding(
        &self,
        group_id: GroupId,
        meta: &mut Option<RelNodeMetaMap>,
    ) -> Result<RelNodeRef<T>> {
        let info = self.get_group_info(group_id);
        if let Some(winner) = info.winner {
            if !winner.impossible {
                let expr_id = winner.expr_id;
                let expr = self.expr_id_to_expr_node[&expr_id].clone();
                let mut children = Vec::with_capacity(expr.children.len());
                for child in &expr.children {
                    children.push(self.get_best_group_binding(*child, meta)?);
                }
                let node = Arc::new(RelNode {
                    typ: expr.typ.clone(),
                    children,
                    data: expr.data.clone(),
                });

                if let Some(meta) = meta {
                    meta.insert(
                        node.as_ref() as *const _ as usize,
                        RelNodeMeta::new(group_id, winner.cost),
                    );
                }
                return Ok(node);
            }
        }
        bail!("no best group binding for group {}", group_id)
    }

    pub fn clear_winner(&mut self) {
        for group in self.groups.values_mut() {
            group.info.winner = None;
        }
    }

    /// Return number of expressions in the memo table.
    pub fn compute_plan_space(&self) -> usize {
        self.expr_id_to_expr_node.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    enum MemoTestRelTyp {
        Group(GroupId),
        List,
        Join,
        Project,
        Scan,
        Expr,
    }

    impl std::fmt::Display for MemoTestRelTyp {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Group(x) => write!(f, "{}", x),
                other => write!(f, "{:?}", other),
            }
        }
    }

    impl RelNodeTyp for MemoTestRelTyp {
        fn is_logical(&self) -> bool {
            matches!(self, Self::Project | Self::Scan | Self::Join)
        }

        fn group_typ(group_id: GroupId) -> Self {
            Self::Group(group_id)
        }

        fn list_typ() -> Self {
            Self::List
        }

        fn extract_group(&self) -> Option<GroupId> {
            if let Self::Group(group_id) = self {
                Some(*group_id)
            } else {
                None
            }
        }
    }

    type MemoTestRelNode = RelNode<MemoTestRelTyp>;
    type MemoTestRelNodeRef = RelNodeRef<MemoTestRelTyp>;

    fn join(
        left: impl Into<MemoTestRelNodeRef>,
        right: impl Into<MemoTestRelNodeRef>,
        cond: impl Into<MemoTestRelNodeRef>,
    ) -> MemoTestRelNode {
        RelNode {
            typ: MemoTestRelTyp::Join,
            children: vec![left.into(), right.into(), cond.into()],
            data: None,
        }
    }

    fn scan(table: &str) -> MemoTestRelNode {
        RelNode {
            typ: MemoTestRelTyp::Scan,
            children: vec![],
            data: Some(Value::String(table.to_string().into())),
        }
    }

    fn project(
        input: impl Into<MemoTestRelNodeRef>,
        expr_list: impl Into<MemoTestRelNodeRef>,
    ) -> MemoTestRelNode {
        RelNode {
            typ: MemoTestRelTyp::Project,
            children: vec![input.into(), expr_list.into()],
            data: None,
        }
    }

    fn list(items: Vec<impl Into<MemoTestRelNodeRef>>) -> MemoTestRelNode {
        RelNode {
            typ: MemoTestRelTyp::List,
            children: items.into_iter().map(|x| x.into()).collect(),
            data: None,
        }
    }

    fn expr(data: Value) -> MemoTestRelNode {
        RelNode {
            typ: MemoTestRelTyp::Expr,
            children: vec![],
            data: Some(data),
        }
    }

    fn group(group_id: GroupId) -> MemoTestRelNode {
        RelNode {
            typ: MemoTestRelTyp::Group(group_id),
            children: vec![],
            data: None,
        }
    }

    #[test]
    fn group_merge_1() {
        let mut memo = Memo::new(Arc::new([]));
        let (group_id, _) =
            memo.add_new_expr(join(scan("t1"), scan("t2"), expr(Value::Bool(true))).into());
        memo.add_expr_to_group(
            join(scan("t2"), scan("t1"), expr(Value::Bool(true))).into(),
            group_id,
        );
        assert_eq!(memo.get_group(group_id).group_exprs.len(), 2);
    }

    #[test]
    fn group_merge_2() {
        let mut memo = Memo::new(Arc::new([]));
        let (group_id_1, _) = memo.add_new_expr(
            project(
                join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
                list(vec![expr(Value::Int64(1))]),
            )
            .into(),
        );
        let (group_id_2, _) = memo.add_new_expr(
            project(
                join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
                list(vec![expr(Value::Int64(1))]),
            )
            .into(),
        );
        assert_eq!(group_id_1, group_id_2);
    }

    #[test]
    fn group_merge_3() {
        let mut memo = Memo::new(Arc::new([]));
        let expr1 = Arc::new(project(scan("t1"), list(vec![expr(Value::Int64(1))])));
        let expr2 = Arc::new(project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])));
        memo.add_new_expr(expr1.clone());
        memo.add_new_expr(expr2.clone());
        // merging two child groups causes parent to merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1").into());
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr);
        let (group_1, _) = memo.get_expr_info(expr1);
        let (group_2, _) = memo.get_expr_info(expr2);
        assert_eq!(group_1, group_2);
    }

    #[test]
    fn group_merge_4() {
        let mut memo = Memo::new(Arc::new([]));
        let expr1 = Arc::new(project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        ));
        let expr2 = Arc::new(project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        ));
        memo.add_new_expr(expr1.clone());
        memo.add_new_expr(expr2.clone());
        // merge two child groups, cascading merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1").into());
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr);
        let (group_1, _) = memo.get_expr_info(expr1.clone());
        let (group_2, _) = memo.get_expr_info(expr2.clone());
        assert_eq!(group_1, group_2);
        let (group_1, _) = memo.get_expr_info(expr1.child(0));
        let (group_2, _) = memo.get_expr_info(expr2.child(0));
        assert_eq!(group_1, group_2);
    }

    #[test]
    fn group_merge_5() {
        let mut memo = Memo::new(Arc::new([]));
        let expr1 = Arc::new(project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        ));
        let expr2 = Arc::new(project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        ));
        let (_, expr1_id) = memo.add_new_expr(expr1.clone());
        let (_, expr2_id) = memo.add_new_expr(expr2.clone());

        // experimenting with group id in expr (i.e., when apply rules)
        let (scan_t1, _) = memo.get_expr_info(scan("t1").into());
        let (expr_middle_proj, _) = memo.get_expr_info(list(vec![expr(Value::Int64(1))]).into());
        let proj_binding = project(group(scan_t1), group(expr_middle_proj));
        let middle_proj_2 = memo.get_expr_memoed(expr2_id).children[0];

        memo.add_expr_to_group(proj_binding.into(), middle_proj_2);

        assert_eq!(
            memo.get_expr_memoed(expr1_id),
            memo.get_expr_memoed(expr2_id)
        ); // these two expressions are merged
        assert_eq!(memo.get_expr_info(expr1), memo.get_expr_info(expr2));
    }
}
