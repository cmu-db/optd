use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    I64,
    String,
    Bool,
    Float64,
    Array(Box<Type>),
    Map(Box<Type>, Box<Type>),
    Tuple(Vec<Type>),
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub ty: Type,
}

#[derive(Debug, Clone)]
pub struct ScalarOp {
    pub name: String,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone)]
pub struct LogicalOp {
    pub name: String,
    pub fields: Vec<Field>,
    pub derived_props: HashMap<String, Expr>,
}

#[derive(Debug, Clone)]
pub enum Pattern {
    Bind(String, Box<Pattern>),
    Constructor(String, Vec<(String, Pattern)>),
    Wildcard,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Match(Box<Expr>, Vec<(Pattern, Block)>),
    If(Box<Expr>, Box<Block>, Option<Box<Block>>),
    Val(String, Box<Expr>),
    Array(Vec<Expr>),
    Map(Vec<(Expr, Expr)>),
    Range(Box<Expr>, Box<Expr>),
    Binary(Box<Expr>, BinOp, Box<Expr>),
    Call(Box<Expr>, Vec<Expr>), // Changed to support method calls
    Member(Box<Expr>, String),
    Var(String),
    Number(i64),
    StringLit(String),
    Constructor(String, Vec<(String, Expr)>),
    Fail(String),
    Closure(String, Box<Expr>),
}

#[derive(Debug, Clone)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Concat,
    Map,
    Filter,
    Range,
}

#[derive(Debug, Clone)]
pub struct Block {
    pub exprs: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct Function {
    pub name: String,
    pub params: Vec<(String, Type)>,
    pub return_type: Type,
    pub body: Block,
    pub is_rule: bool, // true if this is a @rule
}

#[derive(Debug, Clone)]
pub struct File {
    pub operators: Vec<Operator>,
    pub functions: Vec<Function>,
}

#[derive(Debug, Clone)]
pub enum Operator {
    Scalar(ScalarOp),
    Logical(LogicalOp),
}
