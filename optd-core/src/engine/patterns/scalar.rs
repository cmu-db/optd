use super::typ::TypePattern;

/// Pattern for matching scalar expressions in a query plan.
///
/// Scalar patterns can only match scalar children, reflecting the
/// more limited structure of scalar operators in the plan IR.
#[derive(Clone)]
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
        op_type: String,
        /// Pattern for matching operator metadata
        content: Box<TypePattern>,
        /// Patterns for matching scalar children
        scalar_children: Vec<Box<ScalarPattern>>,
    },
}
