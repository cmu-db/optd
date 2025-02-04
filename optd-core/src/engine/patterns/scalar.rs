use crate::operators::scalar::ScalarOperatorKind;

use super::value::ValuePattern;

/// Pattern for matching scalar expressions in a query plan.
///
/// Scalar patterns can only match scalar children, reflecting the
/// more limited structure of scalar operators in the plan IR.
#[derive(Clone, Debug)]
pub enum ScalarPattern {
    /// Matches any scalar expression without binding
    Any,

    /// Negates a pattern match
    Not(Box<ScalarPattern>),

    /// Binds a matched scalar expression to a name
    Bind(String, Box<ScalarPattern>),

    /// Matches a specific scalar operator type with its content and children
    Operator {
        /// Operator type to match (e.g., "Add", "Constant")
        op_type: ScalarOperatorKind,
        /// Patterns for matching operator values
        content: Vec<Box<ValuePattern>>,
        /// Patterns for matching scalar children
        scalar_children: Vec<Box<ScalarPattern>>,
    },
}
