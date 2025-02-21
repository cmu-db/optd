use ordered_float::OrderedFloat;

use crate::errors::span::Spanned;

pub type Identifier = String;

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
    Closure(Spanned<Type>, Spanned<Type>),
    Tuple(Vec<Spanned<Type>>),
    Map(Spanned<Type>, Spanned<Type>),

    // User defined types
    Adt(Identifier),
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: Spanned<Identifier>,
    pub ty: Spanned<Type>,
}

#[derive(Debug, Clone)]
pub enum Adt {
    Product {
        name: Spanned<Identifier>,
        fields: Vec<Spanned<Field>>,
    },
    Sum {
        name: Spanned<Identifier>,
        variants: Vec<Spanned<Adt>>,
    },
}

#[derive(Debug, Clone)]
pub enum Expr {
    // Error recovery
    Error,

    // Control flow
    PatternMatch(Spanned<Expr>, Vec<Spanned<MatchArm>>),
    IfThenElse(Spanned<Expr>, Spanned<Expr>, Spanned<Expr>),

    // Bindings and constructors
    Let(Spanned<Field>, Spanned<Expr>, Spanned<Expr>),
    Constructor(String, Vec<Spanned<Expr>>),

    // Operations
    Binary(Spanned<Expr>, BinOp, Spanned<Expr>),
    Unary(UnaryOp, Spanned<Expr>),

    // Function-related
    Postfix(Spanned<Expr>, PostfixOp),
    Closure(Vec<Spanned<Field>>, Spanned<Expr>),

    // Basic expressions
    Ref(String),
    Literal(Literal),
    Fail(Spanned<Expr>),

    // Collections
    Array(Vec<Spanned<Expr>>),
    Tuple(Vec<Spanned<Expr>>),
    Map(Vec<(Spanned<Expr>, Spanned<Expr>)>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int64(i64),
    String(String),
    Bool(bool),
    Float64(OrderedFloat<f64>),
    Unit,
}

#[derive(Debug, Clone)]
pub enum Pattern {
    Error, // Error recovery
    Bind(Spanned<Identifier>, Spanned<Pattern>),
    Constructor(Spanned<Identifier>, Vec<Spanned<Pattern>>),
    Literal(Literal),
    Wildcard,
}

#[derive(Debug, Clone)]
pub struct MatchArm {
    pub pattern: Spanned<Pattern>,
    pub expr: Spanned<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone)]
pub enum PostfixOp {
    Call(Vec<Spanned<Expr>>),
    Member(Identifier),
}

#[derive(Debug, Clone)]
pub struct Function {
    pub name: Spanned<Identifier>,
    pub receiver: Option<Spanned<Field>>,
    pub params: Option<Vec<Spanned<Field>>>,
    pub return_type: Spanned<Type>,
    pub body: Spanned<Expr>,
    pub annotations: Vec<Spanned<Identifier>>,
}

#[derive(Debug, Clone)]
pub enum Item {
    Adt(Spanned<Adt>),
    Function(Spanned<Function>),
}

// Module-level AST
// TODO: Integrate real module support.
// Right now, we assume a program = module = file.
#[derive(Debug, Clone)]
pub struct Module {
    pub items: Vec<Item>,
}
