use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Int64,
    String,
    Bool,
    Float64,
    Array(Box<Type>),
    Map(Box<Type>, Box<Type>),
    Tuple(Vec<Type>),
    Function(Box<Type>, Box<Type>), // Function type: (input) -> output
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
    Enum(String, Option<Box<Pattern>>),
    Literal(Literal),
    Wildcard,
    Var(String),
}

#[derive(Debug, Clone)]
pub enum Literal {
    Number(i64),
    StringLit(String),
    Boolean(bool),
}

#[derive(Debug, Clone)]
pub enum Expr {
    Match(Box<Expr>, Vec<(Pattern, Block)>),
    If(Box<Expr>, Box<Block>, Option<Box<Block>>),
    Val(String, Box<Expr>, Box<Expr>),
    Array(Vec<Expr>),
    Map(Vec<(Expr, Expr)>),
    Range(Box<Expr>, Box<Expr>),
    Binary(Box<Expr>, BinOp, Box<Expr>),
    Call(Box<Expr>, Vec<Expr>),
    Member(Box<Expr>, String),
    MemberCall(Box<Expr>, String, Vec<Expr>),
    Var(String),
    Literal(Literal),
    Constructor(String, Vec<(String, Expr)>),
    Fail(String),
    Closure(Vec<String>, Box<Expr>),
}

#[derive(Debug, Clone)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Concat,
    Eq,
    Neq,
    Gt,
    Lt,
    Ge,
    Le,
    And,
    Or,
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
    pub is_rule: bool,
    pub is_operator: bool,
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