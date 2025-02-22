use super::r#type::{TypeRegistry, Typed};
use optd_core::cascades::ir::{PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan};
use ordered_float::OrderedFloat;
use std::collections::HashMap;

/// Unique identifier for variables, functions, types, etc.
pub type Identifier = String;

/// Represents expressions in the High-level Intermediate Representation
#[derive(Debug, Clone)]
pub enum Expr {
    // Control flow
    PatternMatch(Typed<Expr>, Vec<MatchArm>),
    IfThenElse(Typed<Expr>, Typed<Expr>, Typed<Expr>),

    // Bindings and constructors
    Let(Identifier, Typed<Expr>, Typed<Expr>),
    Constructor(String, Vec<Typed<Expr>>),

    // Operations
    Binary(Typed<Expr>, BinOp, Typed<Expr>),
    Unary(UnaryOp, Typed<Expr>),

    // Function invocation, array, map, and field invocation
    Call(Typed<Expr>, Vec<Typed<Expr>>),

    // Rust-UDF definition and closure definition
    RustUDF(fn(Vec<Value>) -> Value),
    Closure(Vec<Identifier>, Typed<Expr>),

    // Basic expressions
    Ref(Identifier),
    Literal(Literal),
    Fail(Typed<Expr>),

    // Collections
    Array(Vec<Typed<Expr>>),
    Tuple(Vec<Typed<Expr>>),
    Map(Vec<(Typed<Expr>, Typed<Expr>)>),
}

#[derive(Debug, Clone)]
pub enum Literal {
    Int64(i64),
    Float64(OrderedFloat<f64>),
    String(String),
    Bool(bool),
    Unit,
}

/// Expressions can always be evaluated down to values
#[derive(Debug, Clone)]
pub enum Value {
    Literal(Literal),

    Array(Vec<Value>),
    Tuple(Vec<Value>),
    Map(Vec<(Value, Value)>),

    Struct {
        constructor: String,
        fields: HashMap<String, Box<Value>>,
    },
    Variant {
        constructor: String,
        payload: Vec<Value>,
    },

    Closure(Vec<Identifier>, Box<Expr>),

    Logical(PartialLogicalPlan),
    Scalar(PartialScalarPlan),
    Physical(PartialPhysicalPlan),
}

/// Represents patterns for pattern matching
#[derive(Debug, Clone)]
pub enum Pattern {
    Bind(Typed<Identifier>, Typed<Pattern>),
    Constructor(Typed<Identifier>, Vec<Typed<Pattern>>),
    Literal(Literal),
    Wildcard,
}

/// Represents a single arm in a pattern match expression
#[derive(Debug, Clone)]
pub struct MatchArm {
    pub pattern: Typed<Pattern>,
    pub expr: Typed<Expr>,
}

/// Binary operators supported by the language
#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,

    // String, list, map
    Concat,

    // Comparison
    Eq,
    Neq,
    Gt,
    Lt,
    Ge,
    Le,

    // Logical
    And,
    Or,

    // Other
    Range,
}

/// Unary operators supported by the language
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}

/// Stores annotation information for a function or expression
#[derive(Debug, Clone)]
pub struct AnnotatedExpr {
    /// The expression itself
    pub expr: Typed<Expr>,

    /// List of annotations associated with this expression
    pub annotations: Vec<Identifier>,
}

/// The High-level Intermediate Representation (HIR) for a program.
///
/// The HIR is an intermediary representation that exists after type checking and
/// before code generation or optimization. Unlike the AST, the HIR:
///
/// 1. Contains fully resolved type information for all expressions
/// 2. Has undergone semantic analysis and type checking
/// 3. Has simpler, more regular structure with less syntactic sugar
/// 4. Contains only valid, well-typed expressions (error nodes are removed)
/// 5. Preserves information about annotations for later compilation phases
/// 6. Serves as a bridge between the frontend (parsing, type checking) and
///    the backend (optimization, code generation)
///
/// This representation differs from AST in several key ways:
/// - HIR is more normalized and regular, making it easier for later compiler stages
/// - AST contains source location information (spans) while HIR doesn't
/// - HIR includes type information that wasn't available during parsing
/// - AST preserves all parsing artifacts, while HIR is a cleaned representation
///
/// The HIR is intended to be consumed by later compiler stages like optimization
/// and code generation.
#[derive(Debug, Clone)]
pub struct HIR {
    /// Map from function name to its annotated expression
    pub expressions: HashMap<Identifier, AnnotatedExpr>,

    /// Registry of all types used in the program
    pub types: TypeRegistry,
}
