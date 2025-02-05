//! Pattern matching for scalar operators.

use super::value::ValuePattern;
use crate::operators::scalar::ScalarOperatorKind;

/// A pattern for matching scalar operators in a query plan.
///
/// Supports matching against the operator type, its values, and scalar
/// children. Unlike logical patterns, scalar patterns can only match
/// scalar children.
#[derive(Clone, Debug)]
pub enum ScalarPattern {
    /// Matches any scalar operator.
    Any,

    /// Binds a matched pattern to a name.
    Bind {
        /// Name to bind the matched subtree to
        binding: String,
        /// Pattern to match
        pattern: Box<ScalarPattern>,
    },

    /// Matches a specific scalar operator.
    Operator {
        /// Type of scalar operator to match
        op_type: ScalarOperatorKind,
        /// Value patterns for operator content
        content: Vec<Box<ValuePattern>>,
        /// Patterns for scalar children
        scalar_children: Vec<Box<ScalarPattern>>,
    },
}
