use std::sync::Arc;

use arrow_schema::DataType;

#[derive(Debug, Clone, Copy)]
pub struct Operator(usize);

#[derive(Debug, Clone)]
pub enum OperatorData {
    Scan { table: String, columns: Vec<IU> },
    Selection { input: Operator, predicate: Expr },
    Projection { input: Operator, columns: Vec<IU> },
}

#[derive(Debug, Clone, Copy)]
pub struct Expr(usize);

#[derive(Debug, Clone, Copy)]
pub struct IU(usize);

#[derive(Debug, Clone)]
pub struct IUData {
    source: Operator,
    name: String,
    ty: DataType,
}

#[derive(Debug, Clone)]
pub enum ExprData {
    Literal(ScalarValue),
    IURef(IU),
    Unary {
        op: UnaryOp,
        expr: Expr,
    },
    Binary {
        op: BinaryOp,
        left: Expr,
        right: Expr,
    },
    Nary {
        op: NaryOp,
        exprs: Vec<Expr>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Not,
    IsNull,
    IsNotNull,
    Negate,
}

#[derive(Debug, Clone, Copy)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, Copy)]
pub enum NaryOp {
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null(DataType),
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Utf8(String),
}

impl ScalarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null(ty) => ty.clone(),
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
        }
    }
}

#[derive(Clone)]
pub struct QueryContext {
    root: Operator,
    operators: Vec<OperatorData>,
    exprs: Vec<ExprData>,
    ius: Vec<IUData>,
    passes: Vec<Arc<dyn Pass>>,
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryContext")
            .field("operators", &self.operators)
            .field("exprs", &self.exprs)
            .field("ius", &self.ius)
            .finish_non_exhaustive()
    }
}

pub trait Pass {}
