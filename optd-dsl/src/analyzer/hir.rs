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

use std::{collections::HashMap, sync::Arc};

use super::context::Context;

/// Unique identifier for variables, functions, types, etc.
pub type Identifier = String;

/// Annotation for functions (e.g. [rust], [rule], etc.)
pub type Annotation = String;

/// Values that can be directly represented in the language
#[derive(Debug, Clone, PartialEq)]
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
    Closure(Vec<Identifier>, Arc<Expr>),
    RustUDF(fn(Vec<Value>) -> Value),
}

/// Group identifier in the optimizer
#[derive(Debug, Clone, PartialEq, Copy)]
pub struct GroupId(pub i64);

/// Either materialized or unmaterialized data
///
/// Represents either a fully materialized operator or a reference to an
/// operator group in the optimizer.
#[derive(Debug, Clone)]
pub enum Materializable<T, U> {
    /// Fully materialized operator
    Materialized(T),
    /// Unmaterialized operator (group id or goal)
    UnMaterialized(U),
}

/// Physical goal to achieve in the optimizer
///
/// Combines a logical group with required physical properties.
#[derive(Debug, Clone)]
pub struct Goal {
    /// The logical group to implement
    pub group_id: GroupId,
    /// Required physical properties
    pub properties: Box<Value>,
}

/// Unified operator node structure for all operator types
///
/// This core structure represents a query plan operator with data parameters
/// and child expressions for both logical and physical operations.
#[derive(Debug, Clone)]
pub struct Operator<T> {
    /// Identifies the specific operation (e.g., "Join", "Filter")
    pub tag: String,
    /// Operation-specific parameters
    pub data: Vec<T>,
    /// Children operators
    pub children: Vec<T>,
}

/// Logical operator in the query plan
///
/// Represents a logical relational algebra operation that can be either
/// materialized as a concrete operator or referenced by a group ID in the optimizer.
#[derive(Debug, Clone)]
pub struct LogicalOp<T>(pub Materializable<Operator<T>, GroupId>);

/// Physical operator in the query plan
///
/// Represents an executable implementation of a logical operation with specific
/// physical properties, either materialized as a concrete operator or as a physical goal.
#[derive(Debug, Clone)]
pub struct PhysicalOp<T>(pub Materializable<Operator<T>, Goal>);

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
    /// Logical query operators
    Logical(LogicalOp<T>),
    /// Physical query operators
    Physical(PhysicalOp<T>),
    /// The null value
    Null,
}

/// Expression nodes in the HIR
#[derive(Debug, Clone)]
pub enum Expr {
    /// Pattern matching expression
    PatternMatch(Arc<Expr>, Vec<MatchArm>),
    /// Conditional expression
    IfThenElse(Arc<Expr>, Arc<Expr>, Arc<Expr>),
    /// Variable binding
    Let(Identifier, Arc<Expr>, Arc<Expr>),
    /// Binary operation
    Binary(Arc<Expr>, BinOp, Arc<Expr>),
    /// Unary operation
    Unary(UnaryOp, Arc<Expr>),
    /// Function call
    Call(Arc<Expr>, Vec<Arc<Expr>>),
    /// Variable reference
    Ref(Identifier),
    /// Core expression
    CoreExpr(CoreData<Arc<Expr>>),
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
    pub expr: Arc<Expr>,
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

/// Program representation after the analysis phase
#[derive(Debug)]
pub struct HIR {
    pub context: Context,
    pub annotations: HashMap<Identifier, Vec<Annotation>>,
}
