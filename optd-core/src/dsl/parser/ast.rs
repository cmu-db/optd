use std::collections::HashMap;

/// Types supported by the language
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Int64,
    String,
    Bool,
    Float64,
    Array(Box<Type>),               // Array types like [T]
    Map(Box<Type>, Box<Type>),      // Map types like map[K->V]
    Tuple(Vec<Type>),               // Tuple types like (T1, T2)
    Function(Box<Type>, Box<Type>), // Function types like (T1)->T2
    Operator(OperatorKind),         // Operator types (scalar/logical)
}

/// Kinds of operators supported in the language
#[derive(Debug, Clone, PartialEq)]
pub enum OperatorKind {
    Scalar,  // Scalar operators
    Logical, // Logical operators with derivable properties
}

/// A field in an operator or properties block
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub ty: Type,
}

/// Logical properties block that must appear exactly once per file
#[derive(Debug, Clone)]
pub struct Properties {
    pub fields: Vec<Field>,
}

/// Top-level operator definition
#[derive(Debug, Clone)]
pub enum Operator {
    Scalar(ScalarOp),
    Logical(LogicalOp),
}

/// Scalar operator definition
#[derive(Debug, Clone)]
pub struct ScalarOp {
    pub name: String,
    pub fields: Vec<Field>,
}

/// Logical operator definition with derived properties
#[derive(Debug, Clone)]
pub struct LogicalOp {
    pub name: String,
    pub fields: Vec<Field>,
    pub derived_props: HashMap<String, Expr>, // Maps property names to their derivation expressions
}

/// Patterns used in match expressions
#[derive(Debug, Clone)]
pub enum Pattern {
    Bind(String, Box<Pattern>), // Binding patterns like x@p or x:p
    Constructor(
        String,       // Constructor name
        Vec<Pattern>, // Subpatterns, can be named (x:p) or positional
    ),
    Literal(Literal), // Literal patterns like 42 or "hello"
    Wildcard,         // Wildcard pattern _
    Var(String),      // Variable binding pattern
}

/// Literal values
#[derive(Debug, Clone)]
pub enum Literal {
    Int64(i64),
    String(String),
    Bool(bool),
    Float64(f64),
    Array(Vec<Expr>), // Array literals [e1, e2, ...]
    Tuple(Vec<Expr>), // Tuple literals (e1, e2, ...)
}

/// Expressions - the core of the language
#[derive(Debug, Clone)]
pub enum Expr {
    Match(Box<Expr>, Vec<MatchArm>),          // Pattern matching
    If(Box<Expr>, Box<Expr>, Box<Expr>),      // If-then-else
    Val(String, Box<Expr>, Box<Expr>),        // Local binding (val x = e1; e2)
    Constructor(String, Vec<Expr>),           // Constructor application (currently only operators)
    Binary(Box<Expr>, BinOp, Box<Expr>),      // Binary operations
    Unary(UnaryOp, Box<Expr>),                // Unary operations
    Call(Box<Expr>, Vec<Expr>),               // Function application
    Member(Box<Expr>, String),                // Field access (e.f)
    MemberCall(Box<Expr>, String, Vec<Expr>), // Method call (e.f(args))
    ArrayIndex(Box<Expr>, Box<Expr>),         // Array indexing (e[i])
    Var(String),                              // Variable reference
    Literal(Literal),                         // Literal values
    Fail(String),                             // Failure with message
    Closure(Vec<String>, Box<Expr>),          // Anonymous functions  v = (x, y) => x + y;
}

/// A case in a match expression
#[derive(Debug, Clone)]
pub struct MatchArm {
    pub pattern: Pattern,
    pub expr: Expr,
}

/// Binary operators with fixed precedence
#[derive(Debug, Clone)]
pub enum BinOp {
    Add,    // +
    Sub,    // -
    Mul,    // *
    Div,    // /
    Concat, // ++
    Eq,     // ==
    Neq,    // !=
    Gt,     // >
    Lt,     // <
    Ge,     // >=
    Le,     // <=
    And,    // &&
    Or,     // ||
    Range,  // ..
}

/// Unary operators
#[derive(Debug, Clone)]
pub enum UnaryOp {
    Neg, // -
    Not, // !
}

/// Function definition
#[derive(Debug, Clone)]
pub struct Function {
    pub name: String,
    pub params: Vec<(String, Type)>, // Parameter name and type pairs
    pub return_type: Type,
    pub body: Expr,
    pub rule_type: Option<OperatorKind>, // Some if this is a rule, indicating what kind
}

/// A complete source file
#[derive(Debug, Clone)]
pub struct File {
    pub properties: Properties,   // The single logical properties block
    pub operators: Vec<Operator>, // All operator definitions
    pub functions: Vec<Function>, // All function definitions
}
