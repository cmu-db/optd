use std::fmt::Display;

use itertools::Itertools;
use pretty_xmlish::Pretty;

use optd_core::rel_node::{RelNode, Value};

use super::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::Array(
            (0..self.len())
                .map(|x| self.child(x).explain())
                .collect_vec(),
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

    pub fn bool(value: bool) -> Self {
        Self::new(Value::Bool(value))
    }

    pub fn int(value: i64) -> Self {
        Self::new(Value::Int(value))
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
pub struct ColumnRefExpr(pub Expr);

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
        self.0 .0.data.as_ref().unwrap().as_i64() as usize
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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::display(&format!("#{}", self.index()))
    }
}

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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            vec![self.child().explain()],
        )
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
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

impl Display for BinOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            vec![self.left_child().explain(), self.right_child().explain()],
        )
    }
}

#[derive(Clone, Debug)]
pub struct FuncExpr(Expr);

impl FuncExpr {
    pub fn new(func_id: usize, argv: Vec<Expr>) -> Self {
        FuncExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Func(func_id),
                children: argv.into_iter().map(Expr::into_rel_node).collect(),
                data: None,
            }
            .into(),
        ))
    }

    /// Gets the i-th argument of the function.
    pub fn arg_at(&self, i: usize) -> Expr {
        Expr::from_rel_node(self.clone().into_rel_node().children[i].clone()).unwrap()
    }

    /// Get all children.
    pub fn children(&self) -> Vec<Expr> {
        self.clone()
            .into_rel_node()
            .children
            .iter()
            .map(|x| Expr::from_rel_node(x.clone()).unwrap())
            .collect_vec()
    }

    /// Gets the function id.
    pub fn id(&self) -> usize {
        if let OptRelNodeTyp::Func(func_id) = self.clone().into_rel_node().typ {
            func_id
        } else {
            panic!("not a function")
        }
    }
}

impl OptRelNode for FuncExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::Func(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            self.id().to_string(),
            vec![],
            self.children()
                .iter()
                .map(|child| child.explain())
                .collect(),
        )
    }
}

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

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "SortOrder",
            vec![("order", self.order().to_string().into())],
            vec![self.child().explain()],
        )
    }
}
