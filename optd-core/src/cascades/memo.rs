use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use anyhow::{bail, Result};
use itertools::Itertools;
use std::any::Any;

use crate::{
    cost::Cost,
    property::PropertyBuilderAny,
    rel_node::{RelNode, RelNodeRef, RelNodeTyp, Value},
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

impl<T: RelNodeTyp> std::fmt::Display for RelMemoNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        if let Some(ref data) = self.data {
            write!(f, " {}", data)?;
        }
        for child in &self.children {
            write!(f, " !{}", child)?;
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
struct ReducedGroupId(usize);

impl ReducedGroupId {
    pub fn as_group_id(self) -> GroupId {
        GroupId(self.0)
    }
}

impl Display for ReducedGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct Memo<T: RelNodeTyp> {
    expr_id_to_group_id: HashMap<ExprId, GroupId>,
    expr_id_to_expr_node: HashMap<ExprId, RelMemoNodeRef<T>>,
    expr_node_to_expr_id: HashMap<RelMemoNode<T>, ExprId>,
    groups: HashMap<ReducedGroupId, Group>,
    group_expr_counter: usize,
    merged_groups: HashMap<GroupId, GroupId>,
    property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>,
}

impl<T: RelNodeTyp> Memo<T> {
    pub fn new(property_builders: Arc<[Box<dyn PropertyBuilderAny<T>>]>) -> Self {
        Self {
            expr_id_to_group_id: HashMap::new(),
            expr_id_to_expr_node: HashMap::new(),
            expr_node_to_expr_id: HashMap::new(),
            groups: HashMap::new(),
            group_expr_counter: 0,
            merged_groups: HashMap::new(),
            property_builders,
        }
    }

    /// Get the next group id. Group id and expr id shares the same counter, so as to make it easier to debug...
    fn next_group_id(&mut self) -> ReducedGroupId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        ReducedGroupId(id)
    }

    /// Get the next expr id. Group id and expr id shares the same counter, so as to make it easier to debug...
    fn next_expr_id(&mut self) -> ExprId {
        let id = self.group_expr_counter;
        self.group_expr_counter += 1;
        ExprId(id)
    }

    fn merge_group(&mut self, group_a: ReducedGroupId, group_b: ReducedGroupId) -> ReducedGroupId {
        if group_a == group_b {
            return group_a;
        }
        self.merged_groups
            .insert(group_a.as_group_id(), group_b.as_group_id());
        group_b
    }

    fn get_group_id_of_expr_id(&self, expr_id: ExprId) -> GroupId {
        self.expr_id_to_group_id[&expr_id]
    }

    fn get_reduced_group_id(&self, mut group_id: GroupId) -> ReducedGroupId {
        while let Some(next_group_id) = self.merged_groups.get(&group_id) {
            group_id = *next_group_id;
        }
        ReducedGroupId(group_id.0)
    }

    /// Add or get an expression into the memo, returns the group id and the expr id. If `GroupId` is `None`,
    /// create a new group. Otherwise, add the expression to the group.
    pub fn add_new_group_expr(
        &mut self,
        rel_node: RelNodeRef<T>,
        add_to_group_id: Option<GroupId>,
    ) -> (GroupId, ExprId) {
        if rel_node.typ.extract_group().is_some() {
            unreachable!();
        }
        let (group_id, expr_id) = self.add_new_group_expr_inner(
            rel_node,
            add_to_group_id.map(|x| self.get_reduced_group_id(x)),
        );
        (group_id.as_group_id(), expr_id)
    }

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
        let group_id = self.get_group_id_of_expr_id(expr_id);
        return (group_id, expr_id);
    }

    fn infer_properties(
        &self,
        memo_node: RelMemoNode<T>,
    ) -> Vec<Box<dyn Any + 'static + Send + Sync>> {
        let child_properties = memo_node
            .children
            .iter()
            .map(|child| {
                let group_id = self.get_reduced_group_id(*child);
                self.groups[&group_id].properties.clone()
            })
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

    fn add_expr_to_group(
        &mut self,
        expr_id: ExprId,
        group_id: ReducedGroupId,
        memo_node: RelMemoNode<T>,
    ) {
        match self.groups.entry(group_id) {
            Entry::Occupied(mut entry) => {
                let group = entry.get_mut();
                group.group_exprs.insert(expr_id);
                return;
            }
            _ => {}
        }
        let mut group = Group {
            group_exprs: HashSet::new(),
            info: GroupInfo::default(),
            properties: self.infer_properties(memo_node).into(),
        };
        group.group_exprs.insert(expr_id);
        self.groups.insert(group_id, group);
    }

    fn add_new_group_expr_inner(
        &mut self,
        rel_node: RelNodeRef<T>,
        add_to_group_id: Option<ReducedGroupId>,
    ) -> (ReducedGroupId, ExprId) {
        let children_group_ids = rel_node
            .children
            .iter()
            .map(|child| {
                if let Some(group) = child.typ.extract_group() {
                    group
                } else {
                    self.add_new_group_expr(child.clone(), None).0
                }
            })
            .collect::<Vec<_>>();
        let memo_node = RelMemoNode {
            typ: rel_node.typ.clone(),
            children: children_group_ids,
            data: rel_node.data.clone(),
        };
        if let Some(&expr_id) = self.expr_node_to_expr_id.get(&memo_node) {
            let group_id = self.get_group_id_of_expr_id(expr_id);
            let group_id = self.get_reduced_group_id(group_id);
            if let Some(add_to_group_id) = add_to_group_id {
                self.merge_group(add_to_group_id, group_id);
            }
            return (group_id, expr_id);
        }
        let expr_id = self.next_expr_id();
        let group_id = if let Some(group_id) = add_to_group_id {
            group_id
        } else {
            self.next_group_id()
        };
        self.expr_id_to_expr_node
            .insert(expr_id, memo_node.clone().into());
        self.expr_id_to_group_id
            .insert(expr_id, group_id.as_group_id());
        self.expr_node_to_expr_id.insert(memo_node.clone(), expr_id);
        self.add_expr_to_group(expr_id, group_id, memo_node);
        (group_id, expr_id)
    }

    /// Get the group id of an expression.
    /// The group id is volatile, depending on whether the groups are merged.
    pub fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        let group_id = self
            .expr_id_to_group_id
            .get(&expr_id)
            .expect("expr not found in group mapping");
        self.get_reduced_group_id(*group_id).as_group_id()
    }

    /// Get the memoized representation of a node.
    pub fn get_expr_memoed(&self, expr_id: ExprId) -> RelMemoNodeRef<T> {
        self.expr_id_to_expr_node
            .get(&expr_id)
            .expect("expr not found in expr mapping")
            .clone()
    }

    /// Get all bindings of a group.
    /// TODO: this is not efficient. Should decide whether to expand the rule based on the matcher.
    pub fn get_all_group_bindings(
        &self,
        group_id: GroupId,
        physical_only: bool,
        exclude_placeholder: bool,
        level: Option<usize>,
    ) -> Vec<RelNodeRef<T>> {
        let group_id = self.get_reduced_group_id(group_id);
        let group = self.groups.get(&group_id).expect("group not found");
        group
            .group_exprs
            .iter()
            .filter(|x| !physical_only || !self.get_expr_memoed(**x).typ.is_logical())
            .map(|&expr_id| {
                self.get_all_expr_bindings(expr_id, physical_only, exclude_placeholder, level)
            })
            .concat()
    }

    /// Get all bindings of an expression.
    /// TODO: this is not efficient. Should decide whether to expand the rule based on the matcher.
    pub fn get_all_expr_bindings(
        &self,
        expr_id: ExprId,
        physical_only: bool,
        exclude_placeholder: bool,
        level: Option<usize>,
    ) -> Vec<RelNodeRef<T>> {
        let expr = self.get_expr_memoed(expr_id);
        if let Some(level) = level {
            if level == 0 {
                if exclude_placeholder {
                    return vec![];
                } else {
                    let node = Arc::new(RelNode {
                        typ: expr.typ.clone(),
                        children: expr
                            .children
                            .iter()
                            .map(|x| Arc::new(RelNode::new_group(*x)))
                            .collect_vec(),
                        data: expr.data.clone(),
                    });
                    return vec![node];
                }
            }
        }
        let mut children = vec![];
        let mut cumulative = 1;
        for child in &expr.children {
            let group_exprs = self.get_all_group_bindings(
                *child,
                physical_only,
                exclude_placeholder,
                level.map(|x| x - 1),
            );
            cumulative *= group_exprs.len();
            children.push(group_exprs);
        }
        let mut result = vec![];
        for i in 0..cumulative {
            let mut selected_nodes = vec![];
            let mut ii = i;
            for child in children.iter().rev() {
                let idx = ii % child.len();
                ii /= child.len();
                selected_nodes.push(child[idx].clone());
            }
            selected_nodes.reverse();
            let node = Arc::new(RelNode {
                typ: expr.typ.clone(),
                children: selected_nodes,
                data: expr.data.clone(),
            });
            result.push(node);
        }
        result
    }

    pub fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        let group_id = self.get_reduced_group_id(group_id);
        let group = self.groups.get(&group_id).expect("group not found");
        let mut exprs = group.group_exprs.iter().copied().collect_vec();
        exprs.sort();
        exprs
    }

    pub fn get_all_group_ids(&self) -> Vec<GroupId> {
        let mut ids = self
            .groups
            .keys()
            .copied()
            .map(|x| x.as_group_id())
            .collect_vec();
        ids.sort();
        ids
    }

    pub fn get_group_info(&self, group_id: GroupId) -> GroupInfo {
        self.groups
            .get(&self.get_reduced_group_id(group_id))
            .as_ref()
            .unwrap()
            .info
            .clone()
    }

    pub(crate) fn get_group(&self, group_id: GroupId) -> &Group {
        self.groups
            .get(&self.get_reduced_group_id(group_id))
            .as_ref()
            .unwrap()
    }

    pub fn update_group_info(&mut self, group_id: GroupId, group_info: GroupInfo) {
        let grp = self.groups.get_mut(&self.get_reduced_group_id(group_id));
        grp.unwrap().info = group_info;
    }

    pub fn get_best_group_binding(
        &self,
        group_id: GroupId,
        on_produce: &mut impl FnMut(RelNodeRef<T>, GroupId) -> RelNodeRef<T>,
    ) -> Result<RelNodeRef<T>> {
        let info = self.get_group_info(group_id);
        if let Some(winner) = info.winner {
            if !winner.impossible {
                let expr_id = winner.expr_id;
                let expr = self.get_expr_memoed(expr_id);
                let mut children = Vec::new();
                children.reserve(expr.children.len());
                for child in &expr.children {
                    children.push(self.get_best_group_binding(*child, on_produce)?);
                }
                let node = Arc::new(RelNode {
                    typ: expr.typ.clone(),
                    children,
                    data: expr.data.clone(),
                });
                return Ok(on_produce(node, group_id));
            }
        }
        bail!("no best group binding for group {}", group_id)
    }

    pub fn clear_winner(&mut self) {
        for group in self.groups.values_mut() {
            group.info.winner = None;
        }
    }
}
