use super::{BinOp, Identifier, UnaryOp};
use std::collections::HashMap;

/// Represents a location in source code, used for error reporting and debugging
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct Span {
    /// Start position (in bytes) in source
    pub start: usize,
    /// End position (in bytes) in source
    pub end: usize,
}

/// A value paired with its location in source code
pub type Spanned<T> = (T, Span);

impl Span {
    /// Creates a new span with the given start and end positions
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Combines two spans into one that covers both ranges
    pub fn union(&self, other: &Span) -> Span {
        Span {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }
}

/// Types supported by the language
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Int64,
    String,
    Bool,
    Float64,
    Void,

    /// Array types like [T]
    Array(Box<Type>),
    /// Map types like map[K, V] where K and V are types
    Map(Box<Type>, Box<Type>),
    /// Tuple types like (T1, T2, T3)
    Tuple(Vec<Type>),
    /// Function types like (T1) => T2
    Closure(Box<Type>, Box<Type>),

    /// Scalar operators (work on individual values)
    Scalar,
    /// Logical operators (relational algebra operations)
    Logical,
    /// Physical operators (implementation details)
    Physical,
}

/// Properties block that maps identifiers to their types
#[derive(Clone, Debug)]
pub struct Properties {
    /// Maps property names to their types
    pub props: HashMap<Identifier, Type>,
}

/// Categorizes operators into their fundamental types
#[derive(Debug, Clone)]
pub enum Operator {
    /// Operations on individual values (e.g., arithmetic, comparison)
    Scalar(ScalarOp),
    /// Relational algebra operations (e.g., join, project)
    Logical(LogicalOp),
    /// Physical implementation details (e.g., hash join, merge join)
    Physical(PhysicalOp),
}

/// Field definition within an operator
#[derive(Debug, Clone)]
pub struct Field {
    /// Name of the field
    pub name: Identifier,
    /// Type of the field
    pub ty: Type,
}

/// Scalar operator definition (e.g., arithmetic, comparison)
#[derive(Debug, Clone)]
pub struct ScalarOp {
    /// Name of the operator
    pub name: Identifier,
    /// Fields this operator contains
    pub fields: Vec<Field>,
}

/// Logical operator with derived properties
#[derive(Debug, Clone)]
pub struct LogicalOp {
    /// Name of the operator
    pub name: Identifier,
    /// Fields this operator contains
    pub fields: Vec<Field>,
    /// Maps property names to their derivation expressions
    pub derived_props: HashMap<String, Spanned<Expr>>,
}

/// Physical operator (implementation of logical operators)
#[derive(Debug, Clone)]
pub struct PhysicalOp {
    /// Name of the operator
    pub name: Identifier,
    /// Fields this operator contains
    pub fields: Vec<Field>,
}

/// Patterns used in match expressions
#[derive(Debug, Clone)]
pub enum Pattern {
    /// Binding patterns like x@p, or x:a
    Bind(String, Box<Pattern>),
    /// Constructor patterns like Join(left, right)
    Constructor(String, Vec<Pattern>),
    /// Literal patterns like 42 or "hello"
    Literal(Literal),
    /// Wildcard pattern _
    Wildcard,
    /// Variable pattern like x
    Variable(String),
}

/// Literal values in the language
#[derive(Debug, Clone)]
pub enum Literal {
    Int64(i64),
    String(String),
    Bool(bool),
    Float64(f64),
}

/// Core expression type representing all possible expressions
#[derive(Debug, Clone)]
pub enum Expr {
    // Control flow
    /// Pattern matching expression (match x { ... })
    PatternMatch(Box<Spanned<Expr>>, Vec<Spanned<MatchArm>>),
    /// Conditional expression (if x then y else z)
    IfThenElse(Box<Spanned<Expr>>, Box<Spanned<Expr>>, Box<Spanned<Expr>>),

    // Bindings and construction
    /// Let binding (val x = expr1; expr2)
    Val(String, Box<Spanned<Expr>>, Box<Spanned<Expr>>),
    /// Constructor application
    Constructor(String, Vec<Spanned<Expr>>),

    // Operations
    /// Binary operations (a + b)
    Binary(Box<Spanned<Expr>>, BinOp, Box<Spanned<Expr>>),
    /// Unary operations (!x)
    Unary(UnaryOp, Box<Spanned<Expr>>),

    // Function-related
    /// Function call (f(x))
    Call(Box<Spanned<Expr>>, Vec<Spanned<Expr>>),
    /// Member access (obj.field)
    MemberAccess(Box<Spanned<Expr>>, String),
    /// Method call (obj.method(args))
    MemberCall(Box<Spanned<Expr>>, String, Vec<Spanned<Expr>>),

    // Basic expressions
    /// Variable reference
    Ref(String),
    /// Literal value
    Literal(Literal),
    /// Failure with message
    Fail(String),

    // Complex types
    /// Anonymous function ((x, y) => expr)
    Closure(Vec<String>, Box<Spanned<Expr>>),
    /// Array literal ([1, 2, 3])
    Array(Vec<Spanned<Expr>>),
    /// Tuple literal (1, "hello", true)
    Tuple(Vec<Spanned<Expr>>),
    /// Map literal ({"key" -> value})
    Map(HashMap<Spanned<Expr>, Spanned<Expr>>),
}

/// Function closure definition
#[derive(Debug, Clone)]
pub struct Closure {
    /// Parameter names and optional type annotations
    pub args: Vec<(Identifier, Option<Type>)>,
    /// Body of the closure
    pub body: Box<Spanned<Expr>>,
}

/// Pattern matching arm (case pattern => expr)
#[derive(Debug, Clone)]
pub struct MatchArm {
    /// Pattern to match against
    pub pattern: Pattern,
    /// Expression to evaluate if pattern matches
    pub expr: Expr,
}

/// Function or operator annotation (e.g., @rule(scalar))
#[derive(Debug, Clone)]
pub struct Annotation {
    /// Name of the annotation
    pub name: Identifier,
    /// Arguments to the annotation
    pub value: Vec<Identifier>,
}

/// Top-level function definition
#[derive(Debug, Clone)]
pub struct Function {
    /// Name of the function
    pub name: String,
    /// Parameter list with types
    pub params: Vec<Field>,
    /// Optional return type (can be inferred)
    pub return_type: Option<Type>,
    /// Function body
    pub body: Spanned<Expr>,
    /// Optional function annotation
    pub annotation: Option<Annotation>,
}

/// Complete source file AST
#[derive(Debug, Clone)]
pub struct File {
    /// Logical properties definitions
    pub logical_props: Properties,
    /// Physical properties definitions
    pub physical_props: Properties,
    /// List of operator definitions
    pub operators: Vec<Operator>,
    /// List of function definitions
    pub functions: Vec<Function>,
}