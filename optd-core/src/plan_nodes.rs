//! Typed interface of plan nodes.

mod filter;
mod join;
mod scan;

use crate::rel_node::{RelNode, RelNodeRef, RelNodeTyp, Value};

pub use filter::LogicalFilter;
pub use join::{JoinType, LogicalJoin};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
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

    /// Gets the constant value.
    pub fn value(&self) -> Value {
        self.0 .0.data.clone().unwrap()
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

impl ColumnRefExpr {
    /// Creates a new `ColumnRef` expression.
    pub fn new(column_idx: usize) -> ColumnRefExpr {
        ColumnRefExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::ColumnRef,
                children: vec![],
                data: Some(Value::Int(column_idx as i64)),
            }
            .into(),
        ))
    }

    /// Gets the column index.
    pub fn index(&self) -> usize {
        usize::from_i64(self.0 .0.data.as_ref().unwrap().as_i64()).unwrap()
    }
}

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

#[derive(FromPrimitive)]
pub enum UnOpType {
    Neg = 1,
    Not,
}

#[derive(Clone, Debug)]
pub struct UnOpExpr(Expr);

impl UnOpExpr {
    pub fn new(child: Expr, op_type: UnOpType) -> Self {
        UnOpExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::UnOp,
                children: vec![child.into_rel_node()],
                data: Some(Value::Int(op_type as i64)),
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().children[0].clone()).unwrap()
    }

    pub fn op_type(&self) -> UnOpType {
        UnOpType::from_i64(self.0 .0.data.as_ref().unwrap().as_i64()).unwrap()
    }
}

impl OptRelNode for UnOpExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::UnOp {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }
}

#[derive(FromPrimitive)]
pub enum BinOpType {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Gt,
    Lt,
    Geq,
    Leq,
    And,
    Or,
    Xor,
}

#[derive(Clone, Debug)]
pub struct BinOpExpr(Expr);

impl BinOpExpr {
    pub fn new(left: Expr, right: Expr, op_type: BinOpType) -> Self {
        BinOpExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::BinOp,
                children: vec![left.into_rel_node(), right.into_rel_node()],
                data: Some(Value::Int(op_type as i64)),
            }
            .into(),
        ))
    }

    pub fn left_child(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().children[0].clone()).unwrap()
    }

    pub fn right_child(&self) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().children[1].clone()).unwrap()
    }

    pub fn op_type(&self) -> BinOpType {
        BinOpType::from_i64(self.0 .0.data.as_ref().unwrap().as_i64()).unwrap()
    }
}

impl OptRelNode for BinOpExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::BinOp {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }
}

#[derive(Clone, Debug)]
pub struct FuncExpr(Expr);

impl FuncExpr {
    pub fn new(func_id: usize, argv: Vec<Expr>) -> Self {
        FuncExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Func,
                children: argv.into_iter().map(Expr::into_rel_node).collect(),
                data: Some(Value::Int(func_id as i64)),
            }
            .into(),
        ))
    }

    /// Gets the i-th argument of the function.
    pub fn arg_at(&self, i: usize) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().children[i].clone()).unwrap()
    }

    /// Gets the function id.
    pub fn id(&self) -> usize {
        usize::from_i64(self.0 .0.data.as_ref().unwrap().as_i64()).unwrap()
    }
}

impl OptRelNode for FuncExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Func {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }
}

pub fn explain(rel_node: OptRelNodeRef) {
    match rel_node.typ {
        OptRelNodeTyp::ColumnRef => ColumnRefExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Constant => ConstantExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::UnOp => BinOpExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::BinOp => BinOpExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Func => BinOpExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Join => LogicalJoin::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Scan => LogicalScan::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Filter => LogicalFilter::from_rel_node(rel_node).unwrap().explain(),
        _ => unimplemented!(),
    }
}
