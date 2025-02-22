use optd_core::cascades::ir::{PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan};
use std::collections::HashMap;

/// Unique identifier for variables, functions, types, etc.
pub type Identifier = String;

/// Literal values that can be directly represented in the language
#[derive(Debug, Clone)]
pub enum Literal {
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    Unit,
}

/// Represents different types of functions that can exist in the system
#[derive(Debug, Clone)]
pub enum FunType {
    /// User-defined function written in the DSL (closure)
    Closure(Vec<Identifier>, Box<Expr>),
    /// User-defined function implemented in Rust
    RustUDF(fn(Vec<Value>) -> Value),
}

/// Represents the core data structures that can appear in both expressions and values
#[derive(Debug, Clone)]
pub enum CoreData<T> {
    Literal(Literal),
    Array(Vec<T>),
    Tuple(Vec<T>),
    Map(Vec<(T, T)>),
    Struct(String, Vec<T>),
    Function(FunType),
    Logical(PartialLogicalPlan),
    Scalar(PartialScalarPlan),
    Physical(PartialPhysicalPlan),
    Fail(Box<T>),
}

/// Represents expressions in the High-level Intermediate Representation (HIR)
///
/// Expressions encode computation that can be evaluated to produce values.
#[derive(Debug, Clone)]
pub enum Expr {
    // Control flow
    PatternMatch(Box<Expr>, Vec<MatchArm>),
    IfThenElse(Box<Expr>, Box<Expr>, Box<Expr>),

    // Bindings and constructors
    Let(Identifier, Box<Expr>, Box<Expr>),

    // Operations
    Binary(Box<Expr>, BinOp, Box<Expr>),
    Unary(UnaryOp, Box<Expr>),

    // Function invocation and references
    Call(Box<Expr>, Vec<Expr>),
    Ref(Identifier),

    // Core shared data structures
    Core(CoreData<Expr>),
}

/// Result of evaluating an expression
///
/// Values represent fully evaluated data that result from executing expressions.
#[derive(Debug, Clone)]
pub struct Value(pub CoreData<Value>);

/// Represents patterns for pattern matching expressions
#[derive(Debug, Clone)]
pub enum Pattern {
    Bind(Identifier, Box<Pattern>),
    Constructor(Identifier, Vec<Pattern>),
    Literal(Literal),
    Wildcard,
}

/// Represents a single arm in a pattern match expression
#[derive(Debug, Clone)]
pub struct MatchArm {
    pub pattern: Pattern,
    pub expr: Expr,
}

/// Binary operators supported by the language (normalized)
#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,

    // Comparison
    Lt,
    Eq,

    // Logical
    And,
    Or,

    // Other
    Range,
    Concat,
}

/// Unary operators supported by the language
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}

/// Stores annotation information for a value
#[derive(Debug, Clone)]
pub struct AnnotatedValue {
    /// The value itself
    pub value: Value,
    /// List of annotations associated with this value
    pub annotations: Vec<Identifier>,
}

/// The High-level Intermediate Representation (HIR) for a program.
///
/// The HIR is an intermediary representation that exists after type checking and
/// before code generation or optimization. Unlike the AST, the HIR:
///
/// 1. Has undergone semantic analysis and type checking
/// 2. Has simpler, more regular structure with less syntactic sugar
/// 3. Preserves information about annotations for later compilation phases
/// 4. Serves as a bridge between the frontend (parsing, type checking) and
///    the backend (optimization, code generation)
///
/// This representation differs from AST in several key ways:
/// - HIR is more normalized and regular, making it easier for later compiler stages
/// - AST contains source location information (spans) while HIR doesn't
/// - AST preserves all parsing artifacts, while HIR is a cleaned representation
///
/// The HIR is intended to be consumed by later compiler stages like optimization
/// and code generation.
#[derive(Debug, Clone)]
pub struct HIR {
    /// Map from function name to its annotated value
    pub expressions: HashMap<Identifier, AnnotatedValue>,
}
