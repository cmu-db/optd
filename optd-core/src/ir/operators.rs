/// Represents a child node in an operator tree, which can be either
/// a single item or a variable-length collection of items.
///
/// This enumeration provides a flexible way to represent operator children
/// that may have different arities depending on the operation type.
#[derive(Clone, Debug, PartialEq)]
pub enum Child<T> {
    /// A single child node
    Singleton(T),
    /// A variable number of child nodes
    VarLength(Vec<T>),
}

/// Represents a scalar operator in the query plan.
///
/// Scalar operators compute expressions that produce scalar values,
/// such as arithmetic operations, function calls, or constant values.
///
/// # Type Parameters
/// * `S` - Type of scalar children, typically a plan or group reference
#[derive(Clone, Debug, PartialEq)]
pub struct ScalarOperator<S> {
    /// Identifies the specific operation (e.g., "Add", "Subtract", "Function")
    pub tag: String,
    /// Operation-specific parameters and configuration
    pub data: Vec<OperatorData>,
    /// Child scalar expressions that are inputs to this operation
    pub children: Vec<Child<S>>,
}

/// Represents a logical operator in the query plan.
///
/// Logical operators define relational algebra operations like joins,
/// filters, and projections that transform relations without specifying
/// implementation details.
///
/// # Type Parameters
/// * `R` - Type of relational children, typically a logical plan or group reference
/// * `S` - Type of scalar children, typically a scalar plan or group reference
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalOperator<R, S> {
    /// Identifies the specific operation (e.g., "Join", "Filter", "Project")
    pub tag: String,
    /// Operation-specific parameters and configuration
    pub data: Vec<OperatorData>,
    /// Child relational operators that produce input relations
    pub relational_children: Vec<Child<R>>,
    /// Child scalar expressions used by this operator
    pub scalar_children: Vec<Child<S>>,
}

/// Represents a physical operator in the query plan.
///
/// Physical operators define concrete implementation strategies for
/// logical operations, specifying how the operation should be executed.
///
/// # Type Parameters
/// * `R` - Type of relational children, typically a physical plan or goal reference
/// * `S` - Type of scalar children, typically a scalar plan or group reference
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalOperator<R, S> {
    /// Identifies the specific implementation (e.g., "HashJoin", "IndexScan")
    pub tag: String,
    /// Implementation-specific parameters and configuration
    pub data: Vec<OperatorData>,
    /// Child physical operators that produce input relations
    pub relational_children: Vec<Child<R>>,
    /// Child scalar expressions used by this operator
    pub scalar_children: Vec<Child<S>>,
}

/// Represents primitive data values that can be stored in operator parameters.
///
/// This enumeration provides a type-safe way to represent the various data types
/// that can be used as parameters in logical, scalar, and physical operators.
#[derive(Clone, Debug, PartialEq)]
pub enum OperatorData {
    /// 64-bit signed integer value
    Int64(i64),
    /// 64-bit floating point value
    Float64(f64),
    /// String value
    String(String),
    /// Boolean value
    Bool(bool),
    /// Named structure with fields
    Struct(String, Vec<OperatorData>),
    /// Ordered collection of values
    Array(Vec<OperatorData>),
}
