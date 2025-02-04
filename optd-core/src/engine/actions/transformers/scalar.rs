//! Scalar expression transformations.
//!
//! Transforms scalar expressions through pattern matching and rule composition.
//! Can compose with scalar rules and analyzers only.

use crate::{
    engine::{
        actions::{analyzers::scalar::ScalarAnalyzer, BindAs},
        patterns::scalar::ScalarPattern,
    },
    plans::scalar::PartialScalarPlanExpr,
};

/// A transformer for scalar expressions that produces new scalar expressions.
#[derive(Clone)]
pub struct ScalarTransformer {
    /// Name identifying this transformer
    pub name: String,

    /// Sequence of possible matches to try
    pub matches: Vec<Match>,
}

/// A single pattern match attempt within a scalar transformer.
#[derive(Clone)]
pub struct Match {
    /// Pattern to match against the input expression
    pub pattern: ScalarPattern,

    /// Sequence of rule applications with their bindings
    pub composition: Vec<BindAs<Composition>>,

    /// Expression constructing the output expression
    pub output: PartialScalarPlanExpr,
}

/// Types of rules that can be composed in a scalar transformation.
///
/// Scalar transformers can only use:
/// - Other scalar transformers
/// - Scalar analyzers
#[derive(Clone)]
pub enum Composition {
    ScalarTransformer(Box<ScalarTransformer>),
    ScalarAnalyzer(Box<ScalarAnalyzer>),
}
