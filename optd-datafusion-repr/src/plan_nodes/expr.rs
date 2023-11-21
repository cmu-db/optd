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

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum ConstantType {
    Bool,
    Utf8String,
    Int,
    Date,
    Decimal,
}

#[derive(Clone, Debug)]
pub struct ConstantExpr(pub Expr);

impl ConstantExpr {
    pub fn new(value: Value) -> Self {
        let typ = match &value {
            Value::Bool(_) => ConstantType::Bool,
            Value::String(_) => ConstantType::Utf8String,
            Value::Int(_) => ConstantType::Int,
            Value::Float(_) => ConstantType::Decimal,
            _ => unimplemented!(),
        };
        Self::new_with_type(value, typ)
    }

    pub fn new_with_type(value: Value, typ: ConstantType) -> Self {
        ConstantExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Constant(typ),
                children: vec![],
                data: Some(value),
            }
            .into(),
        ))
    }

    pub fn bool(value: bool) -> Self {
        Self::new_with_type(Value::Bool(value), ConstantType::Bool)
    }

    pub fn string(value: impl AsRef<str>) -> Self {
        Self::new_with_type(
            Value::String(value.as_ref().into()),
            ConstantType::Utf8String,
        )
    }

    pub fn int(value: i64) -> Self {
        Self::new_with_type(Value::Int(value), ConstantType::Int)
    }

    pub fn date(value: i64) -> Self {
        Self::new_with_type(Value::Int(value), ConstantType::Date)
    }

    pub fn decimal(value: f64) -> Self {
        Self::new_with_type(Value::Float(value.into()), ConstantType::Decimal)
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
        if let OptRelNodeTyp::Constant(_) = rel_node.typ {
            return Expr::from_rel_node(rel_node).map(Self);
        }
        None
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

    pub fn with_child(&self, child_idx: usize) -> ColumnRefExpr {
        ColumnRefExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::ColumnRef,
                children: vec![],
                data: Some(Value::Int((self.index() + (child_idx << 32)) as i64)),
            }
            .into(),
        ))
    }

    fn get_data_usize(&self) -> usize {
        self.0 .0.data.as_ref().unwrap().as_i64() as usize
    }

    /// Gets the column index.
    pub fn index(&self) -> usize {
        self.get_data_usize() & ((1 << 32) - 1)
    }

    /// Gets the child id.
    pub fn child_id(&self) -> usize {
        self.get_data_usize() >> 32
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

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FuncType {
    Scalar(datafusion_expr::BuiltinScalarFunction),
    Agg(datafusion_expr::AggregateFunction),
    Case,
}

impl std::fmt::Display for FuncType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FuncType {
    pub fn new_scalar(func_id: datafusion_expr::BuiltinScalarFunction) -> Self {
        FuncType::Scalar(func_id)
    }

    pub fn new_agg(func_id: datafusion_expr::AggregateFunction) -> Self {
        FuncType::Agg(func_id)
    }
}

#[derive(Clone, Debug)]
pub struct FuncExpr(Expr);

impl FuncExpr {
    pub fn new(func_id: FuncType, argv: ExprList) -> Self {
        FuncExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Func(func_id),
                children: vec![argv.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    /// Gets the i-th argument of the function.
    pub fn arg_at(&self, i: usize) -> Expr {
        self.children().child(i)
    }

    /// Get all children.
    pub fn children(&self) -> ExprList {
        ExprList::from_rel_node(self.0.child(0)).unwrap()
    }

    /// Gets the function id.
    pub fn id(&self) -> FuncType {
        if let OptRelNodeTyp::Func(func_id) = &self.clone().into_rel_node().typ {
            func_id.clone()
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
            vec![self.children().explain()],
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

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum LogOpType {
    And,
    Or,
}

impl Display for LogOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogOpExpr(pub Expr);

impl LogOpExpr {
    pub fn new(op_type: LogOpType, expr_list: ExprList) -> Self {
        LogOpExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::LogOp(op_type),
                children: vec![expr_list.into_rel_node()],
                data: None,
            }
            .into(),
        ))
    }

    pub fn children(&self) -> ExprList {
        ExprList::from_rel_node(self.0.child(0)).unwrap()
    }

    pub fn child(&self, idx: usize) -> Expr {
        self.children().child(idx)
    }

    pub fn op_type(&self) -> LogOpType {
        if let OptRelNodeTyp::LogOp(op_type) = self.clone().into_rel_node().typ {
            op_type
        } else {
            panic!("not a log op")
        }
    }
}

impl OptRelNode for LogOpExpr {
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
            vec![self.children().explain()],
        )
    }
}
