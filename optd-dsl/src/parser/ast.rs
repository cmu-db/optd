use std::collections::HashMap;

use crate::errors::span::Spanned;

pub type Identifier = String;

// Core types
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    // Error recovery
    Error,
    Unknown,

    // Primitive types
    Int64,
    String,
    Bool,
    Float64,
    Unit,

    // Complex types
    Array(Spanned<Type>),
    Map(Spanned<Type>, Spanned<Type>),
    Tuple(Vec<Spanned<Type>>),
    Closure(Spanned<Type>, Spanned<Type>),

    // Custom (ADT) types
    Custom(Identifier),
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
    Let(String, Spanned<Expr>, Spanned<Expr>),
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

#[derive(Debug, Clone)]
pub struct Function {
    pub name: Spanned<Identifier>,
    pub params: Vec<Spanned<Field>>,
    pub return_type: Spanned<Type>,
    pub body: Spanned<Expr>,
    pub annotations: Vec<Spanned<Identifier>>,
}

#[derive(Debug, Clone)]
pub enum Adt {
    Struct {
        name: Spanned<Identifier>,
        fields: Vec<Spanned<Field>>,
    },
    Enum {
        name: Spanned<Identifier>,
        variants: Vec<Spanned<Adt>>,
    },
}

// Module-level AST
// TODO(alexis): Integrate real module support.
// Right now, we assume a program = module = file.
#[derive(Debug, Clone)]
pub struct Module {
    pub adts: Vec<Spanned<Adt>>,
    pub functions: Vec<Spanned<Function>>,
}
