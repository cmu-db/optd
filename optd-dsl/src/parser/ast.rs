use crate::errors::span::Spanned;
use ordered_float::OrderedFloat;

/// Unique identifier for variables, functions, types, etc.
pub type Identifier = String;

/// Represents type structures in the language.
/// Types can be primitive, complex, or user-defined (ADT).
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    // Error recovery types
    /// Represents a type error during parsing
    Error,
    /// Represents an unknown or not-yet-inferred type
    Unknown,

    // Primitive types
    /// 64-bit signed integer
    Int64,
    /// UTF-8 encoded string
    String,
    /// Boolean value (true/false)
    Bool,
    /// 64-bit floating point number
    Float64,
    /// Unit type, similar to void but is a valid value
    Unit,

    // Complex types
    /// Array of elements of the same type
    Array(Spanned<Type>),
    /// Function type with parameter type and return type
    Closure(Spanned<Type>, Spanned<Type>),
    /// Tuple type containing multiple heterogeneous values
    Tuple(Vec<Spanned<Type>>),
    /// Map/dictionary with key and value types
    Map(Spanned<Type>, Spanned<Type>),

    // User defined types
    /// Algebraic Data Type reference by name
    Adt(Identifier),
}

/// Represents a field in a record or a parameter in a function
#[derive(Debug, Clone)]
pub struct Field {
    /// Name of the field with source location
    pub name: Spanned<Identifier>,
    /// Type of the field with source location
    pub ty: Spanned<Type>,
}

/// Represents Algebraic Data Types (ADTs): product types (structs) and sum types (enums)
#[derive(Debug, Clone)]
pub enum Adt {
    /// Product type (struct/record) with named fields
    Product {
        /// Name of the product type
        name: Spanned<Identifier>,
        /// Fields that compose the product type
        fields: Vec<Spanned<Field>>,
    },
    /// Sum type (enum/variant) with different possible variants
    Sum {
        /// Name of the sum type
        name: Spanned<Identifier>,
        /// Variants that make up the sum type
        variants: Vec<Spanned<Adt>>,
    },
}

/// Represents expressions in the Abstract Syntax Tree
#[derive(Debug, Clone)]
pub enum Expr {
    /// Error recovery node for syntax errors
    Error,

    // Control flow
    /// Pattern matching expression
    PatternMatch(Spanned<Expr>, Vec<Spanned<MatchArm>>),
    /// Conditional expression (if-then-else)
    IfThenElse(Spanned<Expr>, Spanned<Expr>, Spanned<Expr>),

    // Bindings and constructors
    /// Variable binding (let expressions)
    Let(Spanned<Field>, Spanned<Expr>, Spanned<Expr>),
    /// Constructor for user-defined types
    Constructor(Identifier, Vec<Spanned<Expr>>),

    // Operations
    /// Binary operation (e.g., addition, comparison)
    Binary(Spanned<Expr>, BinOp, Spanned<Expr>),
    /// Unary operation (e.g., negation)
    Unary(UnaryOp, Spanned<Expr>),

    // Function-related
    /// Postfix operations (function call, member access)
    Postfix(Spanned<Expr>, PostfixOp),
    /// Anonymous function definition
    Closure(Vec<Spanned<Field>>, Spanned<Expr>),

    // Basic expressions
    /// Reference to a named variable or function
    Ref(Identifier),
    /// Literal value
    Literal(Literal),
    /// Error propagation or exception raising
    Fail(Spanned<Expr>),

    // Collections
    /// Array literal
    Array(Vec<Spanned<Expr>>),
    /// Tuple literal
    Tuple(Vec<Spanned<Expr>>),
    /// Map/dictionary literal
    Map(Vec<(Spanned<Expr>, Spanned<Expr>)>),
}

/// Represents literal values in the language
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// 64-bit signed integer literal
    Int64(i64),
    /// String literal
    String(String),
    /// Boolean literal
    Bool(bool),
    /// 64-bit floating point literal
    Float64(OrderedFloat<f64>),
    /// Unit literal (similar to null or void)
    Unit,
}

/// Represents patterns for pattern matching
#[derive(Debug, Clone)]
pub enum Pattern {
    /// Error recovery for malformed patterns
    Error,
    /// Binding pattern: binds a value to a name
    Bind(Spanned<Identifier>, Spanned<Pattern>),
    /// Constructor pattern: matches a specific variant of an ADT
    Constructor(Spanned<Identifier>, Vec<Spanned<Pattern>>),
    /// Literal pattern: matches against a specific literal value
    Literal(Literal),
    /// Wildcard pattern: matches any value
    Wildcard,
}

/// Represents a single arm in a pattern match expression
#[derive(Debug, Clone)]
pub struct MatchArm {
    /// Pattern to match against
    pub pattern: Spanned<Pattern>,
    /// Expression to evaluate if pattern matches
    pub expr: Spanned<Expr>,
}

/// Binary operators supported by the language
#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    // Arithmetic
    /// Addition operator (+)
    Add,
    /// Subtraction operator (-)
    Sub,
    /// Multiplication operator (*)
    Mul,
    /// Division operator (/)
    Div,

    // String, list, map
    /// Concatenation operator
    Concat,

    // Comparison
    /// Equality operator (==)
    Eq,
    /// Inequality operator (!=)
    Neq,
    /// Greater than operator (>)
    Gt,
    /// Less than operator (<)
    Lt,
    /// Greater than or equal operator (>=)
    Ge,
    /// Less than or equal operator (<=)
    Le,

    // Logical
    /// Logical AND operator (&&)
    And,
    /// Logical OR operator (||)
    Or,

    // Other
    /// Range operator (..)
    Range,
}

/// Unary operators supported by the language
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    /// Negation operator (-)
    Neg,
    /// Logical NOT operator (!)
    Not,
}

/// Represents postfix operations (function call, member access, composition)
#[derive(Debug, Clone)]
pub enum PostfixOp {
    /// Function or method call with arguments
    Call(Vec<Spanned<Expr>>),
    /// Function composition operator
    Compose(Identifier),
    /// Member/field access
    Member(Identifier),
}

/// Represents a function definition
#[derive(Debug, Clone)]
pub struct Function {
    /// Name of the function with source location
    pub name: Spanned<Identifier>,
    /// Optional receiver for method-style functions (self parameter)
    pub receiver: Option<Spanned<Field>>,
    /// Optional parameters list
    pub params: Option<Vec<Spanned<Field>>>,
    /// Return type with source location
    pub return_type: Spanned<Type>,
    /// Function body with source location (None for external functions)
    pub body: Option<Spanned<Expr>>,
    /// List of annotations attached to the function
    pub annotations: Vec<Spanned<Identifier>>,
}

/// Represents a top-level item in a module
#[derive(Debug, Clone)]
pub enum Item {
    /// Algebraic Data Type definition
    Adt(Spanned<Adt>),
    /// Function definition
    Function(Spanned<Function>),
}

/// Abstract Syntax Tree (AST) for a module.
///
/// The AST is a direct representation of the program's syntactic structure as written
/// by the programmer. It preserves all details from the source code including:
///
/// 1. Source code locations (spans) for precise error reporting
/// 2. The exact syntactic constructs used by the programmer
/// 3. Syntax sugar and convenience features from the language
/// 4. Error nodes for syntax recovery during parsing
///
/// The AST is primarily concerned with representing the syntactic structure rather than
/// semantic meaning. It serves as the initial representation after parsing and before
/// type checking, semantic analysis, and transformation to HIR (High-level Intermediate
/// Representation).
///
/// Key characteristics of the AST:
/// - Preserves the exact structure of the source code
/// - Contains source location information (spans) for error reporting
/// - May contain syntactically valid but semantically invalid constructs
/// - Contains error nodes to enable error recovery during parsing
/// - Does not contain resolved type information (that comes later in HIR)
///
/// The AST is consumed by the semantic analysis phase which verifies its correctness
/// and transforms it into the HIR.
#[derive(Debug, Clone)]
pub struct Module {
    /// List of top-level items in the module
    pub items: Vec<Item>,
}
