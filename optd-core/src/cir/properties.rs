use ordered_float::OrderedFloat;

use super::plans::LogicalPlan;

/// Represents logical properties of a logical group.
///
/// Logical properties describe logical characteristics of the data produced by a logical group,
/// such as schema, uniqueness constraints, or statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProperties(pub Option<PropertiesData>);

/// Represents physical properties of a physical plan node.
///
/// Physical properties describe physical execution characteristics of a physical plan node, such as
/// sort order, distribution, or resource requirements.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhysicalProperties(pub Option<PropertiesData>);

/// Represents various types of property data that can be associated with operators.
///
/// This enum provides a flexible way to store different types of metadata and properties for both
/// logical and physical plan nodes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PropertiesData {
    /// 64-bit signed integer value.
    Int64(i64),
    /// 64-bit floating point value.
    Float64(OrderedFloat<f64>),
    /// String value
    String(String),
    /// Boolean value.
    Bool(bool),
    /// Named structure with fields.
    Struct(String, Vec<PropertiesData>),
    /// Ordered collection of values.
    Array(Vec<PropertiesData>),
    /// Embedded plan node for nested plan references.
    Logical(LogicalPlan),
}
