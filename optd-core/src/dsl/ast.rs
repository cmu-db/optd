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
    Bind(String, Box<Pattern>), // e.g., `x @ SomeConstructor(y: 42)`
    Constructor(String, Vec<(String, Pattern)>), // e.g., `SomeConstructor(y: 42)`
    Enum(String, Option<Box<Pattern>>), // e.g., `SomeEnum(42)`
    Literal(Literal), // e.g., `42`, `"hello"`, `true`
    Wildcard, // `_`
    Var(String), // e.g., `x`
}

#[derive(Debug, Clone)]
pub enum Literal {
    Number(i64),
    StringLit(String),
    Boolean(bool),
}

#[derive(Debug, Clone)]
pub enum Expr {
    Match(Box<Expr>, Vec<(Pattern, Block)>), // `match expr { pattern => block }`
    If(Box<Expr>, Box<Block>, Option<Box<Block>>), // `if expr block else block`
    Val(String, Box<Expr>, Box<Expr>), // `val x = expr; expr`
    Array(Vec<Expr>), // `[expr1, expr2, ...]`
    Map(Vec<(Expr, Expr)>), // `map[expr1 -> expr2, ...]`
    Range(Box<Expr>, Box<Expr>), // `expr1..expr2`
    Binary(Box<Expr>, BinOp, Box<Expr>), // `expr1 + expr2`, `expr1 && expr2`, etc.
    Call(Box<Expr>, Vec<Expr>), // `expr(expr1, expr2, ...)`
    Member(Box<Expr>, String), // `expr.member`
    MemberCall(Box<Expr>, String, Vec<Expr>), // `expr.member(expr1, expr2, ...)`
    Var(String), // `x`
    Literal(Literal), // `42`, `"hello"`, `true`
    Constructor(String, Vec<(String, Expr)>), // `SomeConstructor(field1: expr1, field2: expr2)`
    Fail(String), // `fail("error message")`
    Closure(Vec<String>, Box<Expr>), // `(x, y) => expr`
}

#[derive(Debug, Clone)]
pub enum BinOp {
    Add, // `+`
    Sub, // `-`
    Mul, // `*`
    Div, // `/`
    Concat, // `++`
    Eq, // `==`
    Neq, // `!=`
    Gt, // `>`
    Lt, // `<`
    Ge, // `>=`
    Le, // `<=`
    And, // `&&`
    Or, // `||`
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
    pub is_rule: bool, // true if this is a `@rule`
    pub is_operator: bool, // true if this is an `@operator`
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