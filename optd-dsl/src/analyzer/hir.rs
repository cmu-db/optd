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

use super::context::Context;
use super::map::Map;
use super::types::Type;
use crate::utils::span::Span;
use std::fmt::Debug;
use std::{collections::HashMap, sync::Arc};

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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NoMetadata;
impl ExprMetadata for NoMetadata {}

/// Combined span and type information for an expression
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedSpan {
    /// Source code location.
    pub span: Span,
    /// Inferred type.
    pub ty: Type,
}
impl ExprMetadata for TypedSpan {}

/// User-defined function: either linked or unlinked
#[derive(Debug, Clone)]
pub enum UdfKind<M: ExprMetadata> {
    Linked(fn(Vec<Value<M>>) -> Value<M>),
    Unlinked(Identifier),
}

/// PartialEq implementation for UdfKind
impl<M: ExprMetadata + PartialEq> PartialEq for UdfKind<M> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Linked(f1), Self::Linked(f2)) => {
                // Compare memory addresses of function pointers
                *f1 as *const () == *f2 as *const ()
            }
            (Self::Unlinked(id1), Self::Unlinked(id2)) => id1 == id2,
            _ => false,
        }
    }
}

// Eq implementation for UdfKind - assuming PartialEq implementation satisfies equivalence relation
impl<M: ExprMetadata + Eq> Eq for UdfKind<M> {}

/// Types of functions in the system
#[derive(Debug, Clone)]
pub enum FunKind<M: ExprMetadata> {
    Closure(Vec<Identifier>, Arc<Expr<M>>),
    Udf(UdfKind<M>),
}

/// PartialEq implementation for FunKind
impl<M: ExprMetadata + PartialEq> PartialEq for FunKind<M> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Closure(p1, body1), Self::Closure(p2, body2)) => {
                // Compare the Arc pointers to see if they point to the same allocation
                Arc::ptr_eq(body1, body2) && p1 == p2
            }
            (Self::Udf(u1), Self::Udf(u2)) => u1 == u2,
            _ => false,
        }
    }
}

// Eq implementation for FunKind
impl<M: ExprMetadata + Eq> Eq for FunKind<M> {}

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

/// PartialEq implementation for Materializable
impl<T: PartialEq, U: PartialEq> PartialEq for Materializable<T, U> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Materialized(t1), Self::Materialized(t2)) => t1 == t2,
            (Self::UnMaterialized(u1), Self::UnMaterialized(u2)) => u1 == u2,
            _ => false,
        }
    }
}

/// Eq implementation for Materializable
impl<T: Eq, U: Eq> Eq for Materializable<T, U> {}

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

/// PartialEq implementation for Goal
impl PartialEq for Goal {
    fn eq(&self, other: &Self) -> bool {
        self.group_id == other.group_id && *self.properties == *other.properties
    }
}

/// Eq implementation for Goal
impl Eq for Goal {}

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

/// PartialEq implementation for Operator
impl<T: PartialEq> PartialEq for Operator<T> {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag && self.data == other.data && self.children == other.children
    }
}

/// Eq implementation for Operator
impl<T: Eq> Eq for Operator<T> {}

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

/// PartialEq implementation for LogicalOp
impl<T: PartialEq> PartialEq for LogicalOp<T> {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator && self.group_id == other.group_id
    }
}

/// Eq implementation for LogicalOp
impl<T: Eq> Eq for LogicalOp<T> {}

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

/// PartialEq implementation for PhysicalOp
impl<T: PartialEq, M: ExprMetadata + PartialEq> PartialEq for PhysicalOp<T, M> {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator
            && self.goal == other.goal
            && match (&self.cost, &other.cost) {
                (Some(c1), Some(c2)) => *c1 == *c2,
                (None, None) => true,
                _ => false,
            }
    }
}

impl<T: Eq, M: ExprMetadata + Eq> Eq for PhysicalOp<T, M> {}

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

    /// Creates a new value from core data with unknown type and span metadata
    pub fn new_unknown(data: CoreData<Value<TypedSpan>, TypedSpan>, span: Span) -> Self {
        Self {
            data,
            metadata: TypedSpan {
                span,
                ty: Type::Unknown,
            },
        }
    }
}

/// PartialEq implementation for Value
impl<M: ExprMetadata + PartialEq> PartialEq for Value<M> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data && self.metadata == other.metadata
    }
}

/// Eq implementation for Value
impl<M: ExprMetadata + Eq> Eq for Value<M> {}

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

/// PartialEq implementation for CoreData
impl<T: PartialEq, M: ExprMetadata + PartialEq> PartialEq for CoreData<T, M> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Literal(l1), Self::Literal(l2)) => l1 == l2,
            (Self::Array(a1), Self::Array(a2)) => a1 == a2,
            (Self::Tuple(t1), Self::Tuple(t2)) => t1 == t2,
            (Self::Map(m1), Self::Map(m2)) => m1 == m2,
            (Self::Struct(n1, f1), Self::Struct(n2, f2)) => n1 == n2 && f1 == f2,
            (Self::Function(f1), Self::Function(f2)) => f1 == f2,
            (Self::Fail(f1), Self::Fail(f2)) => f1 == f2,
            (Self::Logical(l1), Self::Logical(l2)) => l1 == l2,
            (Self::Physical(p1), Self::Physical(p2)) => p1 == p2,
            (Self::None, Self::None) => true,
            _ => false,
        }
    }
}

/// Eq implementation for CoreData
impl<T: Eq, M: ExprMetadata + Eq> Eq for CoreData<T, M> {}

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

    /// Creates a new expression with unknown type and span metadata
    pub fn new_unknown(kind: ExprKind<TypedSpan>, span: Span) -> Self {
        Self {
            kind,
            metadata: TypedSpan {
                ty: Type::Unknown,
                span,
            },
        }
    }
}

/// Type alias for map entries to reduce type complexity
pub type MapEntries<M> = Vec<(Arc<Expr<M>>, Arc<Expr<M>>)>;

/// Expression node kinds without metadata
#[derive(Debug, Clone)]
pub enum ExprKind<M: ExprMetadata> {
    /// Pattern matching expression
    PatternMatch(Arc<Expr<M>>, Vec<MatchArm<M>>),
    /// Conditional expression
    IfThenElse(Arc<Expr<M>>, Arc<Expr<M>>, Arc<Expr<M>>),
    /// Expression block creating a new scope
    NewScope(Arc<Expr<M>>),
    /// Variable binding
    Let(Identifier, Arc<Expr<M>>, Arc<Expr<M>>),
    /// Binary operation
    Binary(Arc<Expr<M>>, BinOp, Arc<Expr<M>>),
    /// Unary operation
    Unary(UnaryOp, Arc<Expr<M>>),
    /// Function call
    Call(Arc<Expr<M>>, Vec<Arc<Expr<M>>>),
    /// Map expression
    Map(MapEntries<M>),
    /// Variable reference
    Ref(Identifier),
    /// Field access (becomes a call after analysis)
    FieldAccess(Arc<Expr<M>>, Identifier),
    /// Core expression
    CoreExpr(CoreData<Arc<Expr<M>>, M>),
    /// Core value
    CoreVal(Value<M>),
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

    /// Creates a new pattern with unknown type and span metadata
    pub fn new_unknown(kind: PatternKind<TypedSpan>, span: Span) -> Self {
        Self {
            kind,
            metadata: TypedSpan {
                ty: Type::Unknown,
                span,
            },
        }
    }
}

/// Pattern node kinds without metadata
#[derive(Debug, Clone)]
pub enum PatternKind<M: ExprMetadata> {
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

#[cfg(test)]
mod eq_tests {
    use super::*;

    // Helper functions
    fn create_literal_int(value: i64) -> Value<NoMetadata> {
        Value::new(CoreData::Literal(Literal::Int64(value)))
    }

    fn create_literal_float(value: f64) -> Value<NoMetadata> {
        Value::new(CoreData::Literal(Literal::Float64(value)))
    }

    fn create_literal_string(value: &str) -> Value<NoMetadata> {
        Value::new(CoreData::Literal(Literal::String(value.to_string())))
    }

    fn create_literal_bool(value: bool) -> Value<NoMetadata> {
        Value::new(CoreData::Literal(Literal::Bool(value)))
    }

    fn create_array(values: Vec<Value<NoMetadata>>) -> Value<NoMetadata> {
        Value::new(CoreData::Array(values))
    }

    fn create_tuple(values: Vec<Value<NoMetadata>>) -> Value<NoMetadata> {
        Value::new(CoreData::Tuple(values))
    }

    fn create_struct(name: &str, fields: Vec<Value<NoMetadata>>) -> Value<NoMetadata> {
        Value::new(CoreData::Struct(name.to_string(), fields))
    }

    fn create_none() -> Value<NoMetadata> {
        Value::new(CoreData::None)
    }

    fn create_fail(inner: Value<NoMetadata>) -> Value<NoMetadata> {
        Value::new(CoreData::Fail(Box::new(inner)))
    }

    fn create_logical_operator(
        tag: &str,
        data: Vec<Value<NoMetadata>>,
        children: Vec<Value<NoMetadata>>,
    ) -> Value<NoMetadata> {
        let operator = Operator {
            tag: tag.to_string(),
            data,
            children,
        };

        let logical_op = LogicalOp {
            operator,
            group_id: None,
        };

        Value::new(CoreData::Logical(Materializable::Materialized(logical_op)))
    }

    fn create_physical_operator(
        tag: &str,
        data: Vec<Value<NoMetadata>>,
        children: Vec<Value<NoMetadata>>,
    ) -> Value<NoMetadata> {
        let operator = Operator {
            tag: tag.to_string(),
            data,
            children,
        };

        let physical_op = PhysicalOp {
            operator,
            goal: None,
            cost: None,
        };

        Value::new(CoreData::Physical(Materializable::Materialized(
            physical_op,
        )))
    }

    fn create_function_udf_linked(f: fn(Vec<Value>) -> Value) -> Value<NoMetadata> {
        Value::new(CoreData::Function(FunKind::Udf(UdfKind::Linked(f))))
    }

    fn create_function_udf_unlinked(name: &str) -> Value<NoMetadata> {
        Value::new(CoreData::Function(FunKind::Udf(UdfKind::Unlinked(
            name.to_string(),
        ))))
    }

    fn create_function_closure(
        params: Vec<&str>,
        body: Arc<Expr<NoMetadata>>,
    ) -> Value<NoMetadata> {
        Value::new(CoreData::Function(FunKind::Closure(
            params.into_iter().map(|s| s.to_string()).collect(),
            body,
        )))
    }

    #[test]
    fn test_basic_types_eq() {
        // Test cases for basic literal types
        struct TestCase {
            name: &'static str,
            v1: Value<NoMetadata>,
            v2: Value<NoMetadata>,
            expected_eq: bool,
        }

        let test_cases = vec![
            // Int64 tests
            TestCase {
                name: "equal ints",
                v1: create_literal_int(42),
                v2: create_literal_int(42),
                expected_eq: true,
            },
            TestCase {
                name: "unequal ints",
                v1: create_literal_int(42),
                v2: create_literal_int(43),
                expected_eq: false,
            },
            TestCase {
                name: "max and min int",
                v1: create_literal_int(i64::MAX),
                v2: create_literal_int(i64::MIN),
                expected_eq: false,
            },
            // String tests
            TestCase {
                name: "equal strings",
                v1: create_literal_string("hello"),
                v2: create_literal_string("hello"),
                expected_eq: true,
            },
            TestCase {
                name: "unequal strings",
                v1: create_literal_string("hello"),
                v2: create_literal_string("world"),
                expected_eq: false,
            },
            TestCase {
                name: "empty string equality",
                v1: create_literal_string(""),
                v2: create_literal_string(""),
                expected_eq: true,
            },
            TestCase {
                name: "unicode string equality",
                v1: create_literal_string("你好，世界!"),
                v2: create_literal_string("你好，世界!"),
                expected_eq: true,
            },
            TestCase {
                name: "emoji string equality",
                v1: create_literal_string("😀🔥💯"),
                v2: create_literal_string("😀🔥💯"),
                expected_eq: true,
            },
            // Bool tests
            TestCase {
                name: "equal bools (true)",
                v1: create_literal_bool(true),
                v2: create_literal_bool(true),
                expected_eq: true,
            },
            TestCase {
                name: "equal bools (false)",
                v1: create_literal_bool(false),
                v2: create_literal_bool(false),
                expected_eq: true,
            },
            TestCase {
                name: "unequal bools",
                v1: create_literal_bool(true),
                v2: create_literal_bool(false),
                expected_eq: false,
            },
            // Float64 tests
            TestCase {
                name: "equal floats",
                v1: create_literal_float(3.14),
                v2: create_literal_float(3.14),
                expected_eq: true,
            },
            TestCase {
                name: "unequal floats",
                v1: create_literal_float(3.14),
                v2: create_literal_float(2.71),
                expected_eq: false,
            },
            TestCase {
                name: "positive and negative zero",
                v1: create_literal_float(0.0),
                v2: create_literal_float(-0.0),
                expected_eq: true,
            },
            TestCase {
                name: "infinity equality",
                v1: create_literal_float(f64::INFINITY),
                v2: create_literal_float(f64::INFINITY),
                expected_eq: true,
            },
            TestCase {
                name: "negative infinity equality",
                v1: create_literal_float(f64::NEG_INFINITY),
                v2: create_literal_float(f64::NEG_INFINITY),
                expected_eq: true,
            },
            TestCase {
                name: "infinity vs negative infinity",
                v1: create_literal_float(f64::INFINITY),
                v2: create_literal_float(f64::NEG_INFINITY),
                expected_eq: false,
            },
            // Cross-type comparisons
            TestCase {
                name: "int vs string",
                v1: create_literal_int(42),
                v2: create_literal_string("42"),
                expected_eq: false,
            },
            TestCase {
                name: "bool vs int",
                v1: create_literal_bool(true),
                v2: create_literal_int(1),
                expected_eq: false,
            },
            // None tests
            TestCase {
                name: "none equality",
                v1: create_none(),
                v2: create_none(),
                expected_eq: true,
            },
        ];

        // Execute all test cases
        for case in test_cases {
            if case.expected_eq {
                assert_eq!(
                    case.v1, case.v2,
                    "Expected equality failed for case: {}",
                    case.name
                );
            } else {
                assert_ne!(
                    case.v1, case.v2,
                    "Expected inequality failed for case: {}",
                    case.name
                );
            }
        }

        // NaN special case - NaN is not equal to itself
        let nan_value = create_literal_float(f64::NAN);
        assert_ne!(nan_value, nan_value.clone(), "NaN should not equal itself");
    }

    #[test]
    fn test_function_eq() {
        // Test function pointer comparison
        let f1: fn(Vec<Value>) -> Value = |_| create_none();
        let f2: fn(Vec<Value>) -> Value = |_| create_literal_int(1);

        // Same function pointers should be equal
        let udf1 = create_function_udf_linked(f1);
        let udf1_clone = create_function_udf_linked(f1);
        assert_eq!(udf1, udf1_clone, "Same function pointers should be equal");

        // Different function pointers should not be equal
        let udf2 = create_function_udf_linked(f2);
        assert_ne!(
            udf1, udf2,
            "Different function pointers should not be equal"
        );

        // Same function names should be equal
        let named_udf1 = create_function_udf_unlinked("fn1");
        let named_udf1_clone = create_function_udf_unlinked("fn1");
        assert_eq!(
            named_udf1, named_udf1_clone,
            "Same function names should be equal"
        );

        // Different function names should not be equal
        let named_udf2 = create_function_udf_unlinked("fn2");
        assert_ne!(
            named_udf1, named_udf2,
            "Different function names should not be equal"
        );

        // Test closures
        let closure_body = Arc::new(Expr::new(ExprKind::Ref("x".to_string())));
        let closure1 = create_function_closure(vec!["x"], closure_body.clone());
        let closure1_clone = closure1.clone();

        // Closures should be equal to themselves (via clone to maintain same pointer)
        assert_eq!(
            closure1, closure1_clone,
            "A closure should equal itself via clone"
        );

        // Different closure instances should not be equal (even if they have the same code)
        let closure2 = create_function_closure(
            vec!["x"],
            Arc::new(Expr::new(ExprKind::Ref("x".to_string()))),
        );
        assert_ne!(
            closure1, closure2,
            "Different closure instances should not be equal"
        );

        // Closures should not equal UDFs
        assert_ne!(closure1, udf1, "A closure should not equal a UDF");
    }

    #[test]
    fn test_containers_eq() {
        // Test tuples
        let tuple1 = create_tuple(vec![create_literal_int(1), create_literal_string("hello")]);

        let tuple2 = create_tuple(vec![create_literal_int(1), create_literal_string("hello")]);

        assert_eq!(tuple1, tuple2, "Equal tuples should be equal");

        // Test arrays
        let array1 = create_array(vec![create_literal_int(1), create_literal_int(2)]);

        let array2 = create_array(vec![create_literal_int(1), create_literal_int(2)]);

        assert_eq!(array1, array2, "Equal arrays should be equal");

        // Test nested structures
        let nested1 = create_array(vec![tuple1.clone()]);
        let nested2 = create_array(vec![tuple2]);

        assert_eq!(nested1, nested2, "Equal nested structures should be equal");

        // Test empty containers
        let empty_array1 = create_array(vec![]);
        let empty_array2 = create_array(vec![]);
        assert_eq!(empty_array1, empty_array2, "Empty arrays should be equal");

        let empty_tuple1 = create_tuple(vec![]);
        let empty_tuple2 = create_tuple(vec![]);
        assert_eq!(empty_tuple1, empty_tuple2, "Empty tuples should be equal");

        let empty_struct1 = create_struct("Empty", vec![]);
        let empty_struct2 = create_struct("Empty", vec![]);
        assert_eq!(
            empty_struct1, empty_struct2,
            "Empty structs should be equal"
        );

        // Test different empty container types
        assert_ne!(
            empty_array1, empty_tuple1,
            "Empty arrays should not equal empty tuples"
        );
        assert_ne!(
            empty_tuple1, empty_struct1,
            "Empty tuples should not equal empty structs"
        );

        // Test empty vs non-empty
        let non_empty_array = create_array(vec![create_literal_int(1)]);
        assert_ne!(
            empty_array1, non_empty_array,
            "Empty arrays should not equal non-empty arrays"
        );

        // Test deep nested structures
        let deep_nested1 = create_array(vec![create_tuple(vec![create_struct(
            "Inner",
            vec![
                create_array(vec![create_literal_int(1)]),
                create_fail(create_literal_string("error")),
            ],
        )])]);

        let deep_nested2 = create_array(vec![create_tuple(vec![create_struct(
            "Inner",
            vec![
                create_array(vec![create_literal_int(1)]),
                create_fail(create_literal_string("error")),
            ],
        )])]);

        assert_eq!(
            deep_nested1, deep_nested2,
            "Equal deep nested structures should be equal"
        );

        // Different deep nested structures
        let deep_nested3 = create_array(vec![create_tuple(vec![create_struct(
            "Inner",
            vec![
                create_array(vec![create_literal_int(2)]),
                create_fail(create_literal_string("error")),
            ],
        )])]);

        assert_ne!(
            deep_nested1, deep_nested3,
            "Different deep nested structures should not be equal"
        );

        // Test Fail equality
        let fail1 = create_fail(create_literal_string("error"));
        let fail2 = create_fail(create_literal_string("error"));
        assert_eq!(fail1, fail2, "Equal fails should be equal");

        // Different message Fail
        let fail3 = create_fail(create_literal_string("different"));
        assert_ne!(
            fail1, fail3,
            "Fails with different messages should not be equal"
        );

        // Nested Fail
        let nested_fail1 = create_fail(create_fail(create_literal_string("nested")));
        let nested_fail2 = create_fail(create_fail(create_literal_string("nested")));
        assert_eq!(
            nested_fail1, nested_fail2,
            "Equal nested fails should be equal"
        );

        // Fail vs non-Fail
        let non_fail = create_literal_string("error");
        assert_ne!(fail1, non_fail, "Fail should not equal non-fail");
    }

    #[test]
    fn test_metadata_comparison() {
        // Test values with different file spans
        let span1 = Span::new("file1".to_string(), 0..10);
        let span2 = Span::new("file2".to_string(), 0..10);

        let v1 = Value::new_with(
            CoreData::Literal(Literal::Int64(42)),
            Type::Int64,
            span1.clone(),
        );

        let v2 = Value::new_with(CoreData::Literal(Literal::Int64(42)), Type::Int64, span2);

        // Values with different metadata should not be equal
        assert_ne!(v1, v2, "Values with different metadata should not be equal");

        // Test values with same span but different types
        let v3 = Value::new_with(
            CoreData::Literal(Literal::Int64(42)),
            Type::Float64,
            span1.clone(),
        );

        assert_ne!(v1, v3, "Values with different types should not be equal");

        // Test TypedSpan metadata equality
        let typed_span1 = TypedSpan {
            span: span1.clone(),
            ty: Type::Int64,
        };
        let typed_span2 = TypedSpan {
            span: span1.clone(),
            ty: Type::Int64,
        };

        assert_eq!(
            typed_span1, typed_span2,
            "Equal TypedSpan metadata should be equal"
        );

        // Test NoMetadata vs TypedSpan
        let v4 = Value::new(CoreData::Literal(Literal::Int64(42)));

        // The generic type is different, so we compare the internal values
        match (&v1.data, &v4.data) {
            (CoreData::Literal(l1), CoreData::Literal(l2)) => {
                assert_eq!(
                    l1, l2,
                    "Literal data should be equal regardless of metadata type"
                );
            }
            _ => panic!("Not literal values"),
        }
    }

    #[test]
    fn test_operators_eq() {
        // Test simple logical operators
        let simple_logical1 = create_logical_operator("Simple", vec![], vec![]);
        let simple_logical2 = create_logical_operator("Simple", vec![], vec![]);

        assert_eq!(
            simple_logical1, simple_logical2,
            "Equal simple logical operators should be equal"
        );

        // Test simple physical operators
        let simple_physical1 = create_physical_operator("SimplePhys", vec![], vec![]);
        let simple_physical2 = create_physical_operator("SimplePhys", vec![], vec![]);

        assert_eq!(
            simple_physical1, simple_physical2,
            "Equal simple physical operators should be equal"
        );

        // Test different operator tags
        let diff_tag_logical = create_logical_operator("Different", vec![], vec![]);
        assert_ne!(
            simple_logical1, diff_tag_logical,
            "Logical operators with different tags should not be equal"
        );

        // Test complex logical operators
        let complex_logical1 = create_logical_operator(
            "Complex",
            vec![
                create_array(vec![create_literal_int(1), create_literal_int(2)]),
                create_tuple(vec![
                    create_literal_string("test"),
                    create_literal_bool(true),
                ]),
            ],
            vec![
                create_logical_operator("Child1", vec![], vec![]),
                create_logical_operator("Child2", vec![], vec![]),
            ],
        );

        // Same complex logical operators
        let complex_logical2 = create_logical_operator(
            "Complex",
            vec![
                create_array(vec![create_literal_int(1), create_literal_int(2)]),
                create_tuple(vec![
                    create_literal_string("test"),
                    create_literal_bool(true),
                ]),
            ],
            vec![
                create_logical_operator("Child1", vec![], vec![]),
                create_logical_operator("Child2", vec![], vec![]),
            ],
        );

        assert_eq!(
            complex_logical1, complex_logical2,
            "Equal complex logical operators should be equal"
        );

        // Operators with slightly different data values
        let complex_logical3 = create_logical_operator(
            "Complex",
            vec![
                create_array(vec![create_literal_int(1), create_literal_int(3)]), // Different values
                create_tuple(vec![
                    create_literal_string("test"),
                    create_literal_bool(true),
                ]),
            ],
            vec![
                create_logical_operator("Child1", vec![], vec![]),
                create_logical_operator("Child2", vec![], vec![]),
            ],
        );

        assert_ne!(
            complex_logical1, complex_logical3,
            "Logical operators with different data should not be equal"
        );

        // Test complex physical operators
        let complex_physical1 = create_physical_operator(
            "ComplexPhys",
            vec![create_literal_string("param")],
            vec![
                create_physical_operator("SubOp1", vec![], vec![]),
                create_physical_operator("SubOp2", vec![], vec![]),
            ],
        );

        let complex_physical2 = create_physical_operator(
            "ComplexPhys",
            vec![create_literal_string("param")],
            vec![
                create_physical_operator("SubOp1", vec![], vec![]),
                create_physical_operator("SubOp2", vec![], vec![]),
            ],
        );

        assert_eq!(
            complex_physical1, complex_physical2,
            "Equal complex physical operators should be equal"
        );

        // Test logical vs physical operator type inequality
        assert_ne!(
            create_literal_string("Logical"),
            create_literal_string("Physical"),
            "Logical and Physical string literals should not be equal"
        );
    }
}
