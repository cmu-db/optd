use std::{fmt::Display, sync::Arc};

use arrow_schema::DataType;
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
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float64,
    Date,
    Decimal,
    Any,
}

#[derive(Clone, Debug)]
pub struct ConstantExpr(pub Expr);

impl ConstantExpr {
    pub fn new(value: Value) -> Self {
        let typ = match &value {
            Value::Bool(_) => ConstantType::Bool,
            Value::String(_) => ConstantType::Utf8String,
            Value::UInt8(_) => ConstantType::UInt8,
            Value::UInt16(_) => ConstantType::UInt16,
            Value::UInt32(_) => ConstantType::UInt32,
            Value::UInt64(_) => ConstantType::UInt64,
            Value::Int8(_) => ConstantType::Int8,
            Value::Int16(_) => ConstantType::Int16,
            Value::Int32(_) => ConstantType::Int32,
            Value::Int64(_) => ConstantType::Int64,
            Value::Float(_) => ConstantType::Float64,
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

    pub fn uint8(value: u8) -> Self {
        Self::new_with_type(Value::UInt8(value), ConstantType::UInt8)
    }

    pub fn uint16(value: u16) -> Self {
        Self::new_with_type(Value::UInt16(value), ConstantType::UInt16)
    }

    pub fn uint32(value: u32) -> Self {
        Self::new_with_type(Value::UInt32(value), ConstantType::UInt32)
    }

    pub fn uint64(value: u64) -> Self {
        Self::new_with_type(Value::UInt64(value), ConstantType::UInt64)
    }

    pub fn int8(value: i8) -> Self {
        Self::new_with_type(Value::Int8(value), ConstantType::Int8)
    }

    pub fn int16(value: i16) -> Self {
        Self::new_with_type(Value::Int16(value), ConstantType::Int16)
    }

    pub fn int32(value: i32) -> Self {
        Self::new_with_type(Value::Int32(value), ConstantType::Int32)
    }

    pub fn int64(value: i64) -> Self {
        Self::new_with_type(Value::Int64(value), ConstantType::Int64)
    }

    pub fn float64(value: f64) -> Self {
        Self::new_with_type(Value::Float(value.into()), ConstantType::Float64)
    }

    pub fn date(value: i64) -> Self {
        Self::new_with_type(Value::Int64(value), ConstantType::Date)
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
                data: Some(Value::Int64(column_idx as i64)),
            }
            .into(),
        ))
    }

    fn get_data_usize(&self) -> usize {
        self.0 .0.data.as_ref().unwrap().as_i64() as usize
    }

    /// Gets the column index.
    pub fn index(&self) -> usize {
        self.get_data_usize()
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
    pub fn func(&self) -> FuncType {
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
            self.func().to_string(),
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
        if !matches!(rel_node.typ, OptRelNodeTyp::LogOp(_)) {
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

#[derive(Clone, Debug)]
pub struct BetweenExpr(pub Expr);

impl BetweenExpr {
    pub fn new(expr: Expr, lower: Expr, upper: Expr) -> Self {
        BetweenExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Between,
                children: vec![
                    expr.into_rel_node(),
                    lower.into_rel_node(),
                    upper.into_rel_node(),
                ],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn lower(&self) -> Expr {
        Expr(self.0.child(1))
    }

    pub fn upper(&self) -> Expr {
        Expr(self.0.child(2))
    }
}

impl OptRelNode for BetweenExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::Between) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "Between",
            vec![
                ("expr", self.child().explain()),
                ("lower", self.lower().explain()),
                ("upper", self.upper().explain()),
            ],
            vec![],
        )
    }
}

#[derive(Clone, Debug)]
pub struct DataTypeExpr(pub Expr);

impl DataTypeExpr {
    pub fn new(typ: DataType) -> Self {
        DataTypeExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::DataType(typ),
                children: vec![],
                data: None,
            }
            .into(),
        ))
    }

    pub fn data_type(&self) -> DataType {
        if let OptRelNodeTyp::DataType(data_type) = self.0.typ() {
            data_type
        } else {
            panic!("not a data type")
        }
    }
}

impl OptRelNode for DataTypeExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::DataType(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::display(&self.data_type().to_string())
    }
}

#[derive(Clone, Debug)]
pub struct CastExpr(pub Expr);

impl CastExpr {
    pub fn new(expr: Expr, cast_to: DataType) -> Self {
        CastExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Cast,
                children: vec![
                    expr.into_rel_node(),
                    DataTypeExpr::new(cast_to).into_rel_node(),
                ],
                data: None,
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn cast_to(&self) -> DataType {
        DataTypeExpr::from_rel_node(self.0.child(1))
            .unwrap()
            .data_type()
    }
}

impl OptRelNode for CastExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::Cast) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "Cast",
            vec![
                ("cast_to", format!("{}", self.cast_to()).into()),
                ("expr", self.child().explain()),
            ],
            vec![],
        )
    }
}

#[derive(Clone, Debug)]
pub struct LikeExpr(pub Expr);

impl LikeExpr {
    pub fn new(negated: bool, case_insensitive: bool, expr: Expr, pattern: Expr) -> Self {
        // TODO: support multiple values in data.
        let negated = if negated { 1 } else { 0 };
        let case_insensitive = if case_insensitive { 1 } else { 0 };
        LikeExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::Like,
                children: vec![expr.into_rel_node(), pattern.into_rel_node()],
                data: Some(Value::Serialized(Arc::new([negated, case_insensitive]))),
            }
            .into(),
        ))
    }

    pub fn child(&self) -> Expr {
        Expr(self.0.child(0))
    }

    pub fn pattern(&self) -> Expr {
        Expr(self.0.child(1))
    }

    pub fn negated(&self) -> bool {
        match self.0 .0.data.as_ref().unwrap() {
            Value::Serialized(data) => data[0] != 0,
            _ => panic!("not a serialized value"),
        }
    }

    pub fn case_insensitive(&self) -> bool {
        match self.0 .0.data.as_ref().unwrap() {
            Value::Serialized(data) => data[1] != 0,
            _ => panic!("not a serialized value"),
        }
    }
}

impl OptRelNode for LikeExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::Like) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::simple_record(
            "Like",
            vec![
                ("expr", self.child().explain()),
                ("pattern", self.pattern().explain()),
            ],
            vec![],
        )
    }
}
