//! High-level Intermediate Representation (HIR) for the DSL.
//!
//! This module defines the core data structures that represent programs after the parsing
//! and analysis phases. The HIR provides a structured representation of expressions,
//! patterns, operators, and values that can be evaluated by the interpreter.
//!
//! Key components include:
//! - `Expr`: Expression nodes representing computation to be performed
//! - `Pattern`: Patterns for matching against expression values
//! - `Value`: Results of expression evaluation
//! - `Operator`: Query plan operators with their children and parameters
//! - `CoreData`: Fundamental data structures shared across the system
//!
//! The HIR serves as the foundation for the evaluation system, providing a
//! unified representation that can be transformed into optimizer-specific
//! intermediate representations through the bridge modules.

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

/// Either grouped or concrete data with operator kind
///
/// Represents either a fully materialized operator or a reference to an
/// operator group in the optimizer.
#[derive(Debug, Clone)]
pub enum Materializable<T> {
    /// Reference to an operator group with its kind
    UnMaterialized(i64, OperatorKind),
    /// Fully materialized operator
    Materialized(T),
}

/// Operator kind to differentiate between operator types
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum OperatorKind {
    /// Logical operators represent relational algebra operations
    Logical,
    /// Physical operators represent executable implementations
    Physical,
    /// Scalar operators represent expressions that produce values
    Scalar,
}

/// Unified operator node structure for all operator types
///
/// This core structure represents a query plan operator with data parameters
/// and child expressions for both logical and scalar operations.
#[derive(Debug, Clone)]
pub struct Operator<T> {
    /// Specifies the operator's category (Logical, Scalar, or Physical)
    pub kind: OperatorKind,
    /// Identifies the specific operation (e.g., "Join", "Filter")
    pub tag: String,
    /// Operation-specific parameters
    pub operator_data: Vec<T>,
    /// Child operators that produce relations
    pub relational_children: Vec<T>,
    /// Child operators that produce scalar values
    pub scalar_children: Vec<T>,
}

/// A physical operator decorates an operator with additional execution properties
///
/// Physical operators extend the base operator structure with execution-specific
/// details like physical properties (e.g., sort order, distribution) and an
/// optimizer group identifier.
#[derive(Debug, Clone)]
pub struct PhysicalOperator<T> {
    /// The underlying operator structure
    pub operator: Operator<T>,
    /// Physical execution properties
    pub properties: Box<Value>,
    /// Optimizer group identifier
    pub group_id: i64,
}

/// Core data structures shared across the system
#[derive(Debug, Clone)]
pub enum CoreData<T> {
    /// Primitive literal values
    Literal(Literal),
    /// Ordered collection of values
    Array(Vec<T>),
    /// Fixed collection of possibly heterogeneous values
    Tuple(Vec<T>),
    /// Key-value associations
    Map(Vec<(T, T)>),
    /// Named structure with fields
    Struct(Identifier, Vec<T>),
    /// Function or closure
    Function(FunKind),
    /// Error representation
    Fail(Box<T>),
    /// Logical query operators (transformations on relations)
    LogicalOperator(Materializable<Operator<T>>),
    /// Scalar expressions (computations producing scalar values)
    ScalarOperator(Materializable<Operator<T>>),
    /// Physical query operators (executable operations with properties)
    PhysicalOperator(Materializable<PhysicalOperator<T>>),
    /// The none value
    None,
}

/// Expression nodes in the HIR
#[derive(Debug, Clone)]
pub enum Expr {
    /// Pattern matching expression
    PatternMatch(Box<Expr>, Vec<MatchArm>),
    /// Conditional expression
    IfThenElse(Box<Expr>, Box<Expr>, Box<Expr>),
    /// Variable binding
    Let(Identifier, Box<Expr>, Box<Expr>),
    /// Binary operation
    Binary(Box<Expr>, BinOp, Box<Expr>),
    /// Unary operation
    Unary(UnaryOp, Box<Expr>),
    /// Function call
    Call(Box<Expr>, Vec<Expr>),
    /// Variable reference
    Ref(Identifier),
    /// Core expression
    CoreExpr(CoreData<Expr>),
    /// Core value
    CoreVal(Value),
}

/// Evaluated expression result
#[derive(Debug, Clone)]
pub struct Value(pub CoreData<Value>);

/// Pattern for matching
#[derive(Debug, Clone)]
pub enum Pattern {
    /// Bind a value to a name
    Bind(Identifier, Box<Pattern>),
    /// Match a literal value
    Literal(Literal),
    /// Match a struct with a specific name and field patterns
    Struct(Identifier, Vec<Pattern>),
    /// Match an operator with specific structure
    Operator(Operator<Pattern>),
    /// Match any value
    Wildcard,
    /// Match an empty array
    EmptyArray,
    /// Match an array with head and tail
    ArrayDecomp(Box<Pattern>, Box<Pattern>),
}

/// Match arm combining pattern and expression
#[derive(Debug, Clone)]
pub struct MatchArm {
    /// Pattern to match against
    pub pattern: Pattern,
    /// Expression to evaluate if pattern matches
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
    /// The evaluated value
    pub value: Value,
    /// Annotations associated with the value
    pub annotations: Vec<Identifier>,
}

/// Program representation after the analysis phase
#[derive(Debug, Clone)]
pub struct HIR {
    /// Map of named expressions to their annotated values
    pub expressions: HashMap<Identifier, AnnotatedValue>,
}
