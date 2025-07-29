mod pattern;
mod set;

use std::sync::Arc;

use crate::ir::Operator;
pub use pattern::{OperatorMatchFunc, OperatorPattern};
pub use set::{RuleSet, RuleSetBuilder};

/// An interface describing a valid rule over the operator IR.
pub trait Rule: 'static + Send + Sync {
    /// Gets the name of the rule.
    fn name(&self) -> &'static str;
    /// Gets the operator pattern to match.
    fn pattern(&self) -> &OperatorPattern;
    /// Performs the transformation on `operator`.
    /// A rule may produce zero or more new plans as part of the transformation.
    fn transform(&self, operator: &Operator) -> Result<Vec<Arc<Operator>>, ()>;
}
