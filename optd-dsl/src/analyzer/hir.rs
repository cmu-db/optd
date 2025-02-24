use std::collections::HashMap;

/// Unique identifier for variables, functions, types, etc.
pub type Identifier = String;

/// Values that can be directly represented in the language
#[derive(Debug, Clone)]
pub enum Literal {
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    Unit,
}

/// Types of functions in the system
#[derive(Debug, Clone)]
pub enum FunKind {
    Closure(Vec<Identifier>, Box<Expr>),
    RustUDF(fn(Vec<Value>) -> Value),
}

/// Either grouped or concrete data
#[derive(Debug, Clone)]
pub enum Materializable<T> {
    Group(i64),
    Data(T),
}

/// Logical operator node in query plan
#[derive(Debug, Clone)]
pub struct LogicalOp<T> {
    pub tag: String,
    pub operator_data: Vec<T>,
    pub relational_children: Vec<T>,
    pub scalar_children: Vec<T>,
}

/// Scalar operator node in query plan
#[derive(Debug, Clone)]
pub struct ScalarOp<T> {
    pub tag: String,
    pub operator_data: Vec<T>,
    pub scalar_children: Vec<T>,
}

/// Physical operator node in execution plan
#[derive(Debug, Clone)]
pub struct PhysicalOp<T> {
    pub tag: String,
    pub operator_data: Vec<T>,
    pub relational_children: Vec<T>,
    pub scalar_children: Vec<T>,
}

/// Core data structures shared across the system
#[derive(Debug, Clone)]
pub enum CoreData<T> {
    Literal(Literal),
    Array(Vec<T>),
    Tuple(Vec<T>),
    Map(Vec<(T, T)>),
    Struct(String, Vec<T>),
    Function(FunKind),
    Fail(Box<T>),
    Logical(Materializable<LogicalOp<T>>),
    Scalar(Materializable<ScalarOp<T>>),
    Physical(Materializable<PhysicalOp<T>>),
}

/// Expression nodes in the HIR
#[derive(Debug, Clone)]
pub enum Expr {
    PatternMatch(Box<Expr>, Vec<MatchArm>),
    IfThenElse(Box<Expr>, Box<Expr>, Box<Expr>),
    Let(Identifier, Box<Expr>, Box<Expr>),
    Binary(Box<Expr>, BinOp, Box<Expr>),
    Unary(UnaryOp, Box<Expr>),
    Call(Box<Expr>, Vec<Expr>),
    Ref(Identifier),
    CoreExpr(CoreData<Expr>),
    CoreVal(Value),
}

/// Evaluated expression result
#[derive(Debug, Clone)]
pub struct Value(pub CoreData<Value>);

/// Pattern for matching
#[derive(Debug, Clone)]
pub enum Pattern {
    Bind(Identifier, Box<Pattern>),
    Constructor(Identifier, Vec<Pattern>), // TODO: Support more here, then implement pattern match.
    Literal(Literal),
    Wildcard,
}

/// Match arm combining pattern and expression
#[derive(Debug, Clone)]
pub struct MatchArm {
    pub pattern: Pattern,
    pub expr: Expr,
}

/// Standard binary operators
#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Lt,
    Eq,
    And,
    Or,
    Range,
    Concat,
}

/// Standard unary operators
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}

/// Value with annotations
#[derive(Debug, Clone)]
pub struct AnnotatedValue {
    pub value: Value,
    pub annotations: Vec<Identifier>,
}

/// Program representation after the analysis phase
#[derive(Debug, Clone)]
pub struct HIR {
    pub expressions: HashMap<Identifier, AnnotatedValue>,
}
