use std::collections::HashMap;

use crate::errors::span::Spanned;

pub type Identifier = String;

// Core types
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    // Error recovery
    Error,

    // Primitive types
    Int64,
    String,
    Bool,
    Float64,
    Unit,
    Unknown,

    // Complex types
    Array(Spanned<Type>),
    Map(Spanned<Type>, Spanned<Type>),
    Tuple(Vec<Spanned<Type>>),
    Closure(Spanned<Type>, Spanned<Type>),

    // Operator types
    Scalar,
    Logical,
    Physical,

    // Property types
    LogicalProps,
    PhysicalProps,
}

// Expression-related types
#[derive(Debug, Clone)]
pub enum Literal {
    Int64(i64),
    String(String),
    Bool(bool),
    Float64(f64),
    Unit,
}

#[derive(Debug, Clone)]
pub enum Expr {
    // Error recovery
    Error,

    // Control flow
    PatternMatch(Spanned<Expr>, Vec<Spanned<MatchArm>>),
    IfThenElse(Spanned<Expr>, Spanned<Expr>, Spanned<Expr>),

    // Bindings and constructors
    Val(String, Spanned<Expr>, Spanned<Expr>),
    Constructor(String, Vec<Spanned<Expr>>),

    // Operations
    Binary(Spanned<Expr>, BinOp, Spanned<Expr>),
    Unary(UnaryOp, Spanned<Expr>),

    // Function-related
    Call(Spanned<Expr>, Vec<Spanned<Expr>>),
    MemberAccess(Spanned<Expr>, String),
    MemberCall(Spanned<Expr>, String, Vec<Spanned<Expr>>),
    Closure(Vec<Spanned<String>>, Spanned<Expr>),

    // Basic expressions
    Ref(String),
    Literal(Literal),
    Fail(String),

    // Collections
    Array(Vec<Spanned<Expr>>),
    Tuple(Vec<Spanned<Expr>>),
    Map(HashMap<Spanned<Expr>, Spanned<Expr>>),
}

// Pattern matching
#[derive(Debug, Clone)]
pub enum Pattern {
    Error, // Error recovery
    Bind(Spanned<String>, Spanned<Pattern>),
    Constructor(Spanned<String>, Vec<Spanned<Pattern>>),
    Literal(Literal),
    Wildcard,
    Variable(String),
}

#[derive(Debug, Clone)]
pub struct MatchArm {
    pub pattern: Spanned<Pattern>,
    pub expr: Spanned<Expr>,
}

// Operators
#[derive(Debug, Clone)]
pub enum BinOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    // String
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

#[derive(Debug, Clone)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: Spanned<Identifier>,
    pub ty: Spanned<Type>,
}

// Operator definitions
#[derive(Debug, Clone)]
pub enum Operator {
    Scalar(ScalarOp),
    Logical(LogicalOp),
    Physical(PhysicalOp),
}

#[derive(Debug, Clone)]
pub struct ScalarOp {
    pub name: Spanned<Identifier>,
    pub fields: Vec<Spanned<Field>>,
}

#[derive(Debug, Clone)]
pub struct LogicalOp {
    pub name: Spanned<Identifier>,
    pub fields: Vec<Spanned<Field>>,
    pub derived_props: HashMap<Spanned<String>, Spanned<Expr>>,
}

#[derive(Debug, Clone)]
pub struct PhysicalOp {
    pub name: Spanned<Identifier>,
    pub fields: Vec<Spanned<Field>>,
}

// Function and annotation system
#[derive(Debug, Clone)]
pub struct Function {
    pub name: Spanned<Identifier>,
    pub params: Vec<Spanned<Field>>,
    pub return_type: Spanned<Type>,
    pub body: Spanned<Expr>,
    pub annotation: Option<Spanned<Annotation>>,
}

#[derive(Debug, Clone)]
pub struct Annotation {
    pub name: Spanned<Identifier>,
    pub value: Vec<Spanned<Identifier>>,
}

#[derive(Clone, Debug)]
pub struct Properties {
    pub props: HashMap<Spanned<Identifier>, Spanned<Type>>,
}

// Module-level AST
// TODO(alexis): Integrate real module support.
// Right now, we assume a program = module = file.
#[derive(Debug, Clone)]
pub struct Module {
    pub logical_props: Vec<Spanned<Properties>>,
    pub physical_props: Vec<Spanned<Properties>>,
    pub operators: Vec<Spanned<Operator>>,
    pub functions: Vec<Spanned<Function>>,
}
