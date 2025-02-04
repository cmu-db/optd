use crate::values::OptdExpr;

/// Pattern for matching metadata values and types.
///
/// Type patterns are used within operator patterns to match against
/// the metadata fields of operators.
#[derive(Clone, Debug)]
pub enum ValuePattern {
    /// Matches any type value
    Any,

    /// Binds a matched type value to a name.
    ///
    /// Note: Type bindings are always leaf nodes as they match
    /// atomic metadata values.
    Bind(String, Box<OptdExpr>),

    /// Matches based on an OPTD expression evaluation.
    ///
    /// Allows complex conditions on type values using the
    /// full OPTD expression system.
    Match {
        /// Expression to evaluate for matching
        expr: Box<OptdExpr>,
    },
}
