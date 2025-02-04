use super::{scalar::ScalarPattern, value::ValuePattern};

/// Pattern for matching logical operators in a query plan.
///
/// Logical patterns can match against both logical and scalar children,
/// reflecting the structure of logical operators in the plan IR.
#[derive(Clone, Debug)]
pub enum LogicalPattern {
    /// Matches any logical subtree without binding.
    Any,

    /// Negates a pattern match.
    Not(Box<LogicalPattern>),

    /// Binds a matched subtree to a name for later reference.
    ///
    /// The bound value can be referenced in rule applications
    /// and transformations.
    Bind(String, Box<LogicalPattern>),

    /// Matches a specific logical operator type with its content and children.
    Operator {
        /// Operator type to match (e.g., "Join", "Filter")
        op_type: String,
        /// Patterns for matching operator values
        content: Vec<Box<ValuePattern>>,
        /// Patterns for matching logical children
        logical_children: Vec<Box<LogicalPattern>>,
        /// Patterns for matching scalar children
        scalar_children: Vec<Box<ScalarPattern>>,
    },
}
