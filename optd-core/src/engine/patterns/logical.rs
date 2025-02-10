//! Pattern matching for logical operators.

use super::{scalar::ScalarPattern, value::ValuePattern};
use crate::operators::relational::logical::LogicalOperatorKind;

/// A pattern for matching logical operators in a query plan.
///
/// Supports matching against the operator type, its values, and both
/// logical and scalar children. Patterns can be composed to match
/// complex subtrees and bind matched components for reuse.
#[derive(Clone, Debug)]
pub enum LogicalPattern {
    /// Matches any logical operator.
    Any,

    /// Binds a matched pattern to a name.
    Bind {
        /// Name to bind the matched subtree to
        binding: String,
        /// Pattern to match
        pattern: Box<LogicalPattern>,
    },

    /// Matches a specific logical operator.
    Operator {
        /// Type of logical operator to match
        op_type: LogicalOperatorKind,
        /// Value patterns for operator content
        content: Vec<Box<ValuePattern>>,
        /// Patterns for logical children
        logical_children: Vec<Box<LogicalPattern>>,
        /// Patterns for scalar children
        scalar_children: Vec<Box<ScalarPattern>>,
    },
}
