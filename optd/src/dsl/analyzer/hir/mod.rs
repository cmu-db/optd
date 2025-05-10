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

use super::type_checks::registry::Type;
use crate::catalog::Catalog;
use crate::dsl::utils::retriever::Retriever;
use crate::dsl::utils::span::Span;
use context::Context;
use map::Map;
use std::fmt::Debug;
use std::{collections::HashMap, sync::Arc};

pub(crate) mod context;
mod display;
pub(crate) mod map;

/// Unique identifier for variables, functions, types, etc.
pub type Identifier = String;

/// Annotation for functions (e.g. [rust], [rule], etc.)
pub type Annotation = String;

/// Values that can be directly represented in the language
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    Unit,
}

/// Metadata that can be attached to expression nodes
///
/// This trait allows for different types of metadata to be attached to
/// expression nodes while maintaining a common interface for access.
pub trait ExprMetadata: Debug + Clone {}

/// Empty metadata implementation for cases where no additional data is needed
#[derive(Debug, Clone, Default)]
pub struct NoMetadata;
impl ExprMetadata for NoMetadata {}

/// Combined span and type information for an expression.
#[derive(Debug, Clone)]
pub struct TypedSpan {
    /// Source code location of the expression.
    pub span: Span,
    /// Inferred type (includes span).
    pub ty: Type,
}
impl ExprMetadata for TypedSpan {}

impl TypedSpan {
    /// Creates a new TypedSpan with the given type and span.
    pub fn new(ty: Type, span: Span) -> Self {
        Self { ty, span }
    }
}

#[derive(Debug, Clone)]
pub struct Udf {
    /// The function pointer to the user-defined function.
    ///
    /// Note that [`Value`]s passed to and returned from this UDF do not have associated metadata.
    pub func: fn(&[Value], &dyn Catalog, &dyn Retriever) -> Value,
}

impl Udf {
    pub fn call(
        &self,
        values: &[Value],
        catalog: &dyn Catalog,
        retriever: &dyn Retriever,
    ) -> Value {
        (self.func)(values, catalog, retriever)
    }
}

/// The different kinds of functions in the system.
#[derive(Debug, Clone)]
pub enum FunKind<M: ExprMetadata = NoMetadata> {
    Closure(Vec<Identifier>, Arc<Expr<M>>),
    /// A user-defined function. Note that the [`Value`] type passed to [`Udf`]s do not carry any
    /// metadata like `Value<TypedSpan>`.
    Udf(Udf),
}

/// Group identifier in the optimizer
#[derive(Debug, Clone, PartialEq, Copy, Eq, Hash)]
pub struct GroupId(pub i64);

/// Either materialized or unmaterialized data
///
/// Represents either a fully materialized operator or a reference to an
/// operator group in the optimizer.
#[derive(Debug, Clone)]
pub enum Materializable<T, U> {
    /// Fully materialized operator
    Materialized(T),
    /// Unmaterialized operator (group id or goal)
    UnMaterialized(U),
}

/// Physical goal to achieve in the optimizer
///
/// Combines a logical group with required physical properties.
#[derive(Debug, Clone)]
pub struct Goal {
    /// The logical group to implement
    pub group_id: GroupId,
    /// Required physical properties
    pub properties: Box<Value<NoMetadata>>,
}

/// Unified operator node structure for all operator types
///
/// This core structure represents a query plan operator with data parameters
/// and child expressions for both logical and physical operations.
#[derive(Debug, Clone)]
pub struct Operator<T> {
    /// Identifies the specific operation (e.g., "Join", "Filter")
    pub tag: String,
    /// Operation-specific parameters
    pub data: Vec<T>,
    /// Children operators
    pub children: Vec<T>,
}

/// Logical operator in the query plan
///
/// Represents a logical relational algebra operation that can be either
/// materialized as a concrete operator or referenced by a group ID in the optimizer.
#[derive(Debug, Clone)]
pub struct LogicalOp<T> {
    pub operator: Operator<T>,
    pub group_id: Option<GroupId>,
}

impl<T> LogicalOp<T> {
    /// Creates a new logical operator without a group ID
    ///
    /// Used for representing operators that are not yet assigned to a group
    /// in the optimizer.
    pub fn logical(operator: Operator<T>) -> Self {
        Self {
            operator,
            group_id: None,
        }
    }

    /// Creates a new logical operator with an assigned group ID
    ///
    /// Used for representing operators that have been stored in the optimizer
    /// with a specific group identity.
    pub fn stored_logical(operator: Operator<T>, group_id: GroupId) -> Self {
        Self {
            operator,
            group_id: Some(group_id),
        }
    }
}

/// Physical operator in the query plan
///
/// Represents an executable implementation of a logical operation with specific
/// physical properties, either materialized as a concrete operator or as a physical goal.
#[derive(Debug, Clone)]
pub struct PhysicalOp<T, M: ExprMetadata = NoMetadata> {
    pub operator: Operator<T>,
    pub goal: Option<Goal>,
    pub cost: Option<Box<Value<M>>>,
}

impl<T, M: ExprMetadata> PhysicalOp<T, M> {
    /// Creates a new physical operator without goal or cost information
    ///
    /// Used for representing physical operators that are not yet part of the
    /// optimization process.
    pub fn physical(operator: Operator<T>) -> Self {
        Self {
            operator,
            goal: None,
            cost: None,
        }
    }

    /// Creates a new physical operator with goal but without cost information
    ///
    /// Used for representing physical operators that have been stored in the optimizer
    /// with a specific goal but haven't been costed yet.
    pub fn stored_physical(operator: Operator<T>, goal: Goal) -> Self {
        Self {
            operator,
            goal: Some(goal),
            cost: None,
        }
    }

    /// Creates a new physical operator with both goal and cost information
    ///
    /// Used for representing fully optimized physical operators with computed cost.
    pub fn costed_physical(operator: Operator<T>, goal: Goal, cost: Value<M>) -> Self {
        Self {
            operator,
            goal: Some(goal),
            cost: Some(Box::new(cost)),
        }
    }
}

/// Evaluated expression result
#[derive(Debug, Clone)]
pub struct Value<M: ExprMetadata = NoMetadata> {
    /// Core data structure representing the value
    pub data: CoreData<Value<M>, M>,
    /// Optional metadata for the value
    pub metadata: M,
}

impl Value {
    /// Creates a new value from core data without metadata
    pub fn new(data: CoreData<Value>) -> Self {
        Self {
            data,
            metadata: NoMetadata,
        }
    }
}

impl Value<TypedSpan> {
    /// Creates a new value from core data with type and span metadata
    pub fn new_with(data: CoreData<Value<TypedSpan>, TypedSpan>, ty: Type, span: Span) -> Self {
        Self {
            data,
            metadata: TypedSpan { span, ty },
        }
    }
}

/// Core data structures shared across the system
#[derive(Debug, Clone)]
pub enum CoreData<T, M: ExprMetadata = NoMetadata> {
    /// Primitive literal values
    Literal(Literal),
    /// Ordered collection of values
    Array(Vec<T>),
    /// Fixed collection of possibly heterogeneous values
    Tuple(Vec<T>),
    /// Key-value associations
    Map(Map),
    /// Named structure with fields
    Struct(Identifier, Vec<T>),
    /// Function or closure
    Function(FunKind<M>),
    /// Error representation
    Fail(Box<T>),
    /// Logical query operators
    Logical(Materializable<LogicalOp<T>, GroupId>),
    /// Physical query operators
    Physical(Materializable<PhysicalOp<T, M>, Goal>),
    /// The None value
    None,
}

/// Expression nodes in the HIR with optional metadata
///
/// The M type parameter allows attaching different kinds of metadata to expressions,
/// such as type information, source spans, or both.
#[derive(Debug, Clone)]
pub struct Expr<M: ExprMetadata = NoMetadata> {
    /// The actual expression node
    pub kind: ExprKind<M>,
    /// Optional metadata for the expression
    pub metadata: M,
}

impl Expr<NoMetadata> {
    /// Creates a new expression without metadata
    pub fn new(kind: ExprKind<NoMetadata>) -> Self {
        Self {
            kind,
            metadata: NoMetadata,
        }
    }
}

impl Expr<TypedSpan> {
    /// Creates a new expression with type and span metadata
    pub fn new_with(kind: ExprKind<TypedSpan>, ty: Type, span: Span) -> Self {
        Self {
            kind,
            metadata: TypedSpan { ty, span },
        }
    }
}

/// Type alias for map entries to reduce type complexity
pub type MapEntry<M> = (Arc<Expr<M>>, Arc<Expr<M>>);

/// Expression node kinds without metadata
#[derive(Debug, Clone)]
pub enum ExprKind<M: ExprMetadata = NoMetadata> {
    /// Pattern matching expression
    PatternMatch(Arc<Expr<M>>, Vec<MatchArm<M>>),
    /// Conditional expression
    IfThenElse(Arc<Expr<M>>, Arc<Expr<M>>, Arc<Expr<M>>),
    /// Expression block creating a new scope
    NewScope(Arc<Expr<M>>),
    /// Variable binding
    Let(LetBinding<M>, Arc<Expr<M>>),
    /// Binary operation
    Binary(Arc<Expr<M>>, BinOp, Arc<Expr<M>>),
    /// Unary operation
    Unary(UnaryOp, Arc<Expr<M>>),
    /// Function call
    Call(Arc<Expr<M>>, Vec<Arc<Expr<M>>>),
    /// Map expression
    Map(Vec<MapEntry<M>>),
    /// Variable reference
    Ref(Identifier),
    /// Return from a function
    Return(Arc<Expr<M>>),
    /// Field access (becomes a call after analysis)
    FieldAccess(Arc<Expr<M>>, Identifier),
    /// Core expression
    CoreExpr(CoreData<Arc<Expr<M>>, M>),
    /// Core value
    CoreVal(Value<M>),
}

/// Variable binding in a let expression
#[derive(Debug, Clone)]
pub struct LetBinding<M: ExprMetadata = NoMetadata> {
    /// Name of the variable
    pub name: Identifier,
    /// Expression to bind to the variable
    pub expr: Arc<Expr<M>>,
    /// Optional metadata for the binding
    pub metadata: M,
}

impl LetBinding<NoMetadata> {
    /// Creates a new let binding without metadata
    pub fn new(name: Identifier, expr: Arc<Expr<NoMetadata>>) -> Self {
        Self {
            name,
            expr,
            metadata: NoMetadata,
        }
    }
}

impl LetBinding<TypedSpan> {
    /// Creates a new let binding with type and span metadata
    pub fn new_with(name: Identifier, expr: Arc<Expr<TypedSpan>>, ty: Type, span: Span) -> Self {
        Self {
            name,
            expr,
            metadata: TypedSpan { ty, span },
        }
    }
}

/// Pattern for matching with optional metadata
#[derive(Debug, Clone)]
pub struct Pattern<M: ExprMetadata = NoMetadata> {
    /// The actual pattern node
    pub kind: PatternKind<M>,
    /// Optional metadata for the pattern
    pub metadata: M,
}

impl Pattern<NoMetadata> {
    /// Creates a new pattern without metadata
    pub fn new(kind: PatternKind<NoMetadata>) -> Self {
        Self {
            kind,
            metadata: NoMetadata,
        }
    }
}

impl Pattern<TypedSpan> {
    /// Creates a new pattern with type and span metadata
    pub fn new_with(kind: PatternKind<TypedSpan>, ty: Type, span: Span) -> Self {
        Self {
            kind,
            metadata: TypedSpan { ty, span },
        }
    }
}

/// Pattern node kinds without metadata
#[derive(Debug, Clone)]
pub enum PatternKind<M: ExprMetadata = NoMetadata> {
    /// Bind a value to a name
    Bind(Identifier, Box<Pattern<M>>),
    /// Match a literal value
    Literal(Literal),
    /// Match a struct with a specific name and field patterns
    Struct(Identifier, Vec<Pattern<M>>),
    /// Match an operator with specific structure
    Operator(Operator<Pattern<M>>),
    /// Match any value
    Wildcard,
    /// Match an empty array
    EmptyArray,
    /// Match an array with head and tail
    ArrayDecomp(Box<Pattern<M>>, Box<Pattern<M>>),
}

/// Match arm combining pattern and expression
#[derive(Debug, Clone)]
pub struct MatchArm<M: ExprMetadata = NoMetadata> {
    /// Pattern to match against
    pub pattern: Pattern<M>,
    /// Expression to evaluate if pattern matches
    pub expr: Arc<Expr<M>>,
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
pub struct HIR<M: ExprMetadata = NoMetadata> {
    pub context: Context<M>,
    pub annotations: HashMap<Identifier, Vec<Annotation>>,
}
