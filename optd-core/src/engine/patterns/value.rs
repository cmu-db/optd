//! Pattern matching for operator values.

use crate::values::OptdExpr;

/// A pattern for matching operator metadata values.
///
/// Used within operator patterns to match metadata fields. Type bindings
/// are always leaf nodes as they match atomic values.
#[derive(Clone, Debug)]
pub enum ValuePattern {
    /// Matches any value.
    Any,

    /// Binds a matched value to a name.
    Bind {
        /// Name to bind the matched value to
        binding: String,
    },

    /// Matches using an OPTD expression.
    Match {
        /// Expression to evaluate
        expr: Box<OptdExpr>,
    },
}
