//! Scalar expression transformations.
//!
//! Transforms scalar expressions through pattern matching and rule composition.
//! Can compose with scalar rules and analyzers only.

use crate::{
    engine::{
        actions::{analyzers::scalar::ScalarAnalyzer, Action, Match as GenericMatch},
        patterns::scalar::ScalarPattern,
    },
    plans::scalar::PartialScalarPlanExpr,
};
use std::{cell::RefCell, rc::Rc};

/// Types of rules that can be composed in a scalar transformation.
///
/// Scalar transformers can only use:
/// - Other scalar transformers
/// - Scalar analyzers
#[derive(Clone, Debug)]
pub enum Composition {
    ScalarTransformer(Rc<RefCell<ScalarTransformer>>),
    ScalarAnalyzer(Rc<RefCell<ScalarAnalyzer>>),
}

/// A single scalar transformer match.
pub type Match = GenericMatch<ScalarPattern, Composition, PartialScalarPlanExpr>;

/// A transformer for scalar expressions that produces new scalar expressions.
pub type ScalarTransformer = Action<Match>;
