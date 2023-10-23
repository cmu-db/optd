//! Typed interface of plan nodes.

mod filter;
mod join;
mod scan;

use crate::rel_node::{RelNode, RelNodeRef, RelNodeTyp, Value};

pub use filter::LogicalFilter;
pub use join::{JoinType, LogicalJoin};
pub use scan::LogicalScan;

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptRelNodeTyp {
    // Plan nodes
    // Developers: update `is_plan_node` function after adding new elements
    Projection,
    Filter,
    Scan,
    Join,
    // Expressions
    Constant,
    ColumnRef,
    UnOp,
    BinOp,
    Func,
}

impl OptRelNodeTyp {
    pub fn is_plan_node(&self) -> bool {
        (OptRelNodeTyp::Projection as usize) <= (*self as usize)
            && (*self as usize) <= (OptRelNodeTyp::Join as usize)
    }

    pub fn is_expression(&self) -> bool {
        (OptRelNodeTyp::Constant as usize) <= (*self as usize)
            && (*self as usize) <= (OptRelNodeTyp::Func as usize)
    }
}

impl std::fmt::Display for OptRelNodeTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl RelNodeTyp for OptRelNodeTyp {}

pub type OptRelNodeRef = RelNodeRef<OptRelNodeTyp>;

pub trait OptRelNode {
    fn into_rel_node(self) -> OptRelNodeRef;
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self>
    where
        Self: Sized;
    fn explain(&self) {}
}

#[derive(Clone, Debug)]
pub struct PlanNode(OptRelNodeRef);

impl OptRelNode for PlanNode {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !rel_node.typ.is_plan_node() {
            return None;
        }
        Some(Self(rel_node))
    }
}

#[derive(Clone, Debug)]
pub struct Expr(OptRelNodeRef);

impl OptRelNode for Expr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !rel_node.typ.is_expression() {
            return None;
        }
        Some(Self(rel_node))
    }
}

#[derive(Clone, Debug)]
pub struct ConstantExpr(pub Expr);

impl ConstantExpr {
    pub fn new(value: Value) -> Self {
        ConstantExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Constant,
                children: vec![],
                data: Some(value),
            }
            .into(),
        ))
    }
}

impl OptRelNode for ConstantExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Constant {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }
}

#[derive(Clone, Debug)]
pub struct ColumnRefExpr(Expr);

impl OptRelNode for ColumnRefExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::ColumnRef {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }
}

pub fn column_ref(column_idx: usize) -> ConstantExpr {
    ConstantExpr(Expr(
        RelNode {
            typ: OptRelNodeTyp::ColumnRef,
            children: vec![],
            data: Some(Value::Int(column_idx as i64)),
        }
        .into(),
    ))
}

pub fn explain(rel_node: OptRelNodeRef) {
    match rel_node.typ {
        OptRelNodeTyp::ColumnRef => ColumnRefExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Constant => ConstantExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Join => LogicalJoin::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Scan => LogicalScan::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Filter => LogicalFilter::from_rel_node(rel_node).unwrap().explain(),
        _ => unimplemented!(),
    }
}
