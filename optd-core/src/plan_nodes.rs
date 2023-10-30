//! Typed interface of plan nodes.

mod filter;
mod join;
mod scan;

use std::sync::Arc;

use crate::rel_node::{RelNode, RelNodeRef, RelNodeTyp, Value};

pub use self::{filter::PhysicalFilter, join::PhysicalNestedLoopJoin, scan::PhysicalScan};
pub use filter::LogicalFilter;
pub use join::{JoinType, LogicalJoin};
use pretty_xmlish::{Pretty, PrettyConfig};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
pub use scan::LogicalScan;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptRelNodeTyp {
    // Plan nodes
    // Developers: update `is_plan_node` function after adding new elements
    Projection,
    Filter,
    Scan,
    Join(JoinType),
    // Physical plan nodes
    PhysicalProjection,
    PhysicalFilter,
    PhysicalScan,
    PhysicalNestedLoopJoin(JoinType),
    // Expressions
    Constant,
    ColumnRef,
    UnOp,
    BinOp,
    Func,
}

impl OptRelNodeTyp {
    pub fn is_plan_node(&self) -> bool {
        if let Self::Projection
        | Self::Filter
        | Self::Scan
        | Self::Join(_)
        | Self::PhysicalProjection
        | Self::PhysicalFilter
        | Self::PhysicalNestedLoopJoin(_)
        | Self::PhysicalScan = self
        {
            true
        } else {
            false
        }
    }

    pub fn is_expression(&self) -> bool {
        if let Self::Constant | Self::ColumnRef | Self::UnOp | Self::BinOp | Self::Func = self {
            true
        } else {
            false
        }
    }
}

impl std::fmt::Display for OptRelNodeTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl RelNodeTyp for OptRelNodeTyp {
    fn is_logical(&self) -> bool {
        if let Self::Projection | Self::Filter | Self::Scan | Self::Join(_) = self {
            true
        } else {
            false
        }
    }
}

pub type OptRelNodeRef = RelNodeRef<OptRelNodeTyp>;

pub trait OptRelNode: 'static + Clone {
    fn into_rel_node(self) -> OptRelNodeRef;
    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self>
    where
        Self: Sized;
    fn dispatch_explain(&self) -> Pretty<'static>;
    fn explain(&self) -> Pretty<'static> {
        explain(self.clone().into_rel_node())
    }
    fn explain_to_string(&self) -> String {
        let mut config = PrettyConfig {
            need_boundaries: false,
            reduced_spaces: false,
            ..Default::default()
        };
        let mut out = String::new();
        config.unicode(&mut out, &self.explain());
        out
    }
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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "<PlanNode>",
            vec![(
                "node_type",
                self.clone().into_rel_node().typ.to_string().into(),
            )],
            self.0
                .children
                .iter()
                .map(|child| explain(child.clone()))
                .collect(),
        )
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
    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "<Expr>",
            vec![(
                "node_type",
                self.clone().into_rel_node().typ.to_string().into(),
            )],
            self.0
                .children
                .iter()
                .map(|child| explain(child.clone()))
                .collect(),
        )
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
    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::display(&self.value())
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

impl FuncExpr {
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

pub fn explain(rel_node: OptRelNodeRef) -> Pretty<'static> {
    match rel_node.typ {
        OptRelNodeTyp::ColumnRef => ColumnRefExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Constant => ConstantExpr::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::UnOp => BinOpExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::BinOp => BinOpExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Func => BinOpExpr::from_rel_node(rel_node).unwrap().explain(),
        OptRelNodeTyp::Join(_) => LogicalJoin::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Scan => LogicalScan::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::Filter => LogicalFilter::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalFilter => PhysicalFilter::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalScan => PhysicalScan::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        OptRelNodeTyp::PhysicalNestedLoopJoin(_) => PhysicalNestedLoopJoin::from_rel_node(rel_node)
            .unwrap()
            .dispatch_explain(),
        _ => unimplemented!(),
    }
}

fn replace_typ(node: OptRelNodeRef, target_type: OptRelNodeTyp) -> OptRelNodeRef {
    Arc::new(RelNode {
        typ: target_type,
        children: node.children.clone(),
        data: node.data.clone(),
    })
}
