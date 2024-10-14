use itertools::Itertools;
use optd_core::rel_node::{RelNode, RelNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct ExprList(OptRelNodeRef);

impl ExprList {
    pub fn new(exprs: Vec<Expr>) -> Self {
        ExprList(
            RelNode::new_list(exprs.into_iter().map(|x| x.into_rel_node()).collect_vec()).into(),
        )
    }

    /// Gets number of expressions in the list
    pub fn len(&self) -> usize {
        self.0.children.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.children.is_empty()
    }

    pub fn child(&self, idx: usize) -> Expr {
        Expr::from_rel_node(self.0.child(idx)).unwrap()
    }

    pub fn to_vec(&self) -> Vec<Expr> {
        self.0
            .children
            .iter()
            .map(|x| Expr::from_rel_node(x.clone()).unwrap())
            .collect_vec()
    }

    pub fn from_group(rel_node: OptRelNodeRef) -> Self {
        Self(rel_node)
    }
}

impl OptRelNode for ExprList {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.clone()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::List {
            return None;
        }
        Some(ExprList(rel_node))
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::Array(
            (0..self.len())
                .map(|x| self.child(x).explain(meta_map))
                .collect_vec(),
        )
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalExprList(OptRelNodeRef);

impl PhysicalExprList {
    pub fn new(exprs: Vec<Expr>) -> Self {
        PhysicalExprList(
            RelNode {
                typ: OptRelNodeTyp::PhysicalList,
                data: None,
                children: exprs.into_iter().map(|x| x.into_rel_node()).collect_vec(),
            }
            .into(),
        )
    }

    /// Gets number of expressions in the list
    pub fn len(&self) -> usize {
        self.0.children.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.children.is_empty()
    }

    pub fn child(&self, idx: usize) -> Expr {
        Expr::from_rel_node(self.0.child(idx)).unwrap()
    }

    pub fn to_vec(&self) -> Vec<Expr> {
        self.0
            .children
            .iter()
            .map(|x| Expr::from_rel_node(x.clone()).unwrap())
            .collect_vec()
    }

    pub fn from_group(rel_node: OptRelNodeRef) -> Self {
        Self(rel_node)
    }
}

impl OptRelNode for PhysicalExprList {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.clone()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::PhysicalList {
            return None;
        }
        Some(PhysicalExprList(rel_node))
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::Array(
            (0..self.len())
                .map(|x| self.child(x).explain(meta_map))
                .collect_vec(),
        )
    }
}
