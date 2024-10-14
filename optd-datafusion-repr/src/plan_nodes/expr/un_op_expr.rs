use std::fmt::Display;

use optd_core::rel_node::{RelNode, RelNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum UnOpType {
    Neg = 1,
    Not,
}

impl Display for UnOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct UnOpExpr(Expr);

impl UnOpExpr {
    pub fn new(child: Expr, op_type: UnOpType) -> Self {
        UnOpExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::UnOp(op_type),
                children: vec![child.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(0)).unwrap()
    }

    pub fn op_type(&self) -> UnOpType {
        if let OptRelNodeTyp::UnOp(op_type) = self.clone().into_rel_node().typ {
            op_type
        } else {
            panic!("not a un op")
        }
    }
}

impl OptRelNode for UnOpExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::UnOp(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            vec![self.child().explain(meta_map)],
        )
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalUnOpExpr(Expr);

impl PhysicalUnOpExpr {
    pub fn new(child: Expr, op_type: UnOpType) -> Self {
        PhysicalUnOpExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::PhysicalUnOp(op_type),
                children: vec![child.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(0)).unwrap()
    }

    pub fn op_type(&self) -> UnOpType {
        if let OptRelNodeTyp::PhysicalUnOp(op_type) = self.clone().into_rel_node().typ {
            op_type
        } else {
            panic!("not a un op")
        }
    }
}

impl OptRelNode for PhysicalUnOpExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::PhysicalUnOp(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            vec![self.child().explain(meta_map)],
        )
    }
}
