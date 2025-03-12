use ordered_float::OrderedFloat;

/// Represents a child node in an operator tree, which can be either
/// a single item or a variable-length collection of items.
///
/// This enumeration provides a flexible way to represent operator children
/// that may have different arities depending on the operation type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Child<T> {
    /// A single child node
    Singleton(T),
    /// A variable number of child nodes
    VarLength(Vec<T>),
}

/// Represents primitive data values that can be stored in operator parameters.
///
/// This enumeration provides a type-safe way to represent the various data types
/// that can be used as parameters in operators.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum OperatorData {
    /// 64-bit signed integer value
    Int64(i64),
    /// 64-bit floating point value
    Float64(OrderedFloat<f64>),
    /// String value
    String(String),
    /// Boolean value
    Bool(bool),
    /// Named structure with fields
    Struct(String, Vec<OperatorData>),
    /// Ordered collection of values
    Array(Vec<OperatorData>),
}

/// Represents an operator in the query plan.
///
/// This unified operator structure can represent both logical and physical operations,
/// as well as scalar expressions, simplifying the type hierarchy.
///
/// # Type Parameters
/// * `T` - Type of children, typically a plan or group reference
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Operator<T> {
    /// Identifies the specific operation (e.g., "Join", "Filter", "Add")
    pub tag: String,
    /// Operation-specific parameters and configuration
    pub data: Vec<OperatorData>,
    /// Child operators that are inputs to this operation
    pub children: Vec<Child<T>>,
}
