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

use super::context::Context;
use std::collections::HashMap;

/// Unique identifier for variables, functions, types, etc.
pub type Identifier = String;

/// A function annotation (e.g. rule, rust, etc.)
pub type Annotation = String;

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
    Group(i64, OperatorKind),
    Data(T),
}

/// Operator kind to differentiate between operator types
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum OperatorKind {
    Logical,
    Physical,
    Scalar,
}

/// Unified operator node structure for all operator types
#[derive(Debug, Clone)]
pub struct Operator<T> {
    pub kind: OperatorKind,
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
    Struct(Identifier, Vec<T>),
    Function(FunKind),
    Fail(Box<T>),
    Operator(Materializable<Operator<T>>),
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
    Literal(Literal),
    Struct(Identifier, Vec<Pattern>),
    Operator(Operator<Pattern>),
    Wildcard,
    EmptyArray,
    ArrayDecomp(Box<Pattern>, Box<Pattern>),
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

/// Program representation after the analysis phase
#[derive(Debug)]
pub struct HIR {
    pub context: Context,
    pub annotations: HashMap<Identifier, Vec<Annotation>>,
}
