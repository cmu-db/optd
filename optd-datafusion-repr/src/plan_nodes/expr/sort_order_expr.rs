use std::fmt::Display;

use optd_core::rel_node::{RelNode, RelNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum SortOrderType {
    Asc,
    Desc,
}

impl Display for SortOrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct SortOrderExpr(Expr);

impl SortOrderExpr {
    pub fn new(order: SortOrderType, child: Expr) -> Self {
        SortOrderExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::SortOrder(order),
                children: vec![child.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr::from_rel_node(self.0.child(0)).unwrap()
    }

    pub fn order(&self) -> SortOrderType {
        if let OptRelNodeTyp::SortOrder(order) = self.0.typ() {
            order
        } else {
            panic!("not a sort order expr")
        }
    }
}

impl OptRelNode for SortOrderExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::SortOrder(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "SortOrder",
            vec![("order", self.order().to_string().into())],
            vec![self.child().explain(meta_map)],
        )
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalSortOrderExpr(Expr);

impl PhysicalSortOrderExpr {
    pub fn new(order: SortOrderType, child: Expr) -> Self {
        PhysicalSortOrderExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::PhysicalSortOrder(order),
                children: vec![child.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr::from_rel_node(self.0.child(0)).unwrap()
    }

    pub fn order(&self) -> SortOrderType {
        if let OptRelNodeTyp::PhysicalSortOrder(order) = self.0.typ() {
            order
        } else {
            panic!("not a sort order expr")
        }
    }
}

impl OptRelNode for PhysicalSortOrderExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::PhysicalSortOrder(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "PhysicalSortOrder",
            vec![("order", self.order().to_string().into())],
            vec![self.child().explain(meta_map)],
        )
    }
}
