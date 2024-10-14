use std::fmt::Display;

use optd_core::rel_node::{RelNode, RelNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

/// The pattern of storing numerical, comparison, and logical operators in the same type with is_*() functions
///     to distinguish between them matches how datafusion::logical_expr::Operator does things
/// I initially thought about splitting BinOpType into three "subenums". However, having two nested levels of
///     types leads to some really confusing code
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum BinOpType {
    // numerical
    Add,
    Sub,
    Mul,
    Div,
    Mod,

    // comparison
    Eq,
    Neq,
    Gt,
    Lt,
    Geq,
    Leq,
}

impl Display for BinOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl BinOpType {
    pub fn is_numerical(&self) -> bool {
        matches!(
            self,
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod
        )
    }

    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Neq | Self::Gt | Self::Lt | Self::Geq | Self::Leq
        )
    }
}

#[derive(Clone, Debug)]
pub struct BinOpExpr(pub Expr);

impl BinOpExpr {
    pub fn new(left: Expr, right: Expr, op_type: BinOpType) -> Self {
        BinOpExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::BinOp(op_type),
                children: vec![left.into_rel_node(), right.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    pub fn left_child(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(0)).unwrap()
    }

    pub fn right_child(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(1)).unwrap()
    }

    pub fn op_type(&self) -> BinOpType {
        if let OptRelNodeTyp::BinOp(op_type) = self.clone().into_rel_node().typ {
            op_type
        } else {
            panic!("not a bin op")
        }
    }
}

impl OptRelNode for BinOpExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::BinOp(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            vec![
                self.left_child().explain(meta_map),
                self.right_child().explain(meta_map),
            ],
        )
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalBinOpExpr(pub Expr);

impl PhysicalBinOpExpr {
    pub fn new(left: Expr, right: Expr, op_type: BinOpType) -> Self {
        PhysicalBinOpExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::PhysicalBinOp(op_type),
                children: vec![left.into_rel_node(), right.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    pub fn left_child(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(0)).unwrap()
    }

    pub fn right_child(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().child(1)).unwrap()
    }

    pub fn op_type(&self) -> BinOpType {
        if let OptRelNodeTyp::PhysicalBinOp(op_type) = self.clone().into_rel_node().typ {
            op_type
        } else {
            panic!("not a bin op")
        }
    }
}

impl OptRelNode for PhysicalBinOpExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::PhysicalBinOp(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            vec![
                self.left_child().explain(meta_map),
                self.right_child().explain(meta_map),
            ],
        )
    }
}
