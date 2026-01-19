//! Rules are used to match against nodes in the IR, and perform transformations
//! from one logical plan to another. Transformation / implementation rules are
//! implemented in core/rules/

mod pattern;
mod set;

use crate::error::Result;
use crate::ir::{IRContext, Operator};
pub use pattern::{OperatorMatchFunc, OperatorPattern};
pub use set::{RuleSet, RuleSetBuilder};
use std::sync::Arc;

/// An interface describing a valid rule over the operator IR.
pub trait Rule: 'static + Send + Sync {
    /// Gets the name of the rule.
    fn name(&self) -> &'static str;

    /// Gets the operator pattern to match.
    fn pattern(&self) -> &OperatorPattern;

    /// Performs the transformation on `operator`.
    /// A rule may produce zero or more new plans as part of the transformation.
    /// The returned plans are returned by returning their root nodes
    fn transform(&self, operator: &Operator, ctx: &IRContext) -> Result<Vec<Arc<Operator>>>;
}
