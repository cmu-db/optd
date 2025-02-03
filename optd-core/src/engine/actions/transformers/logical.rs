//! Logical plan transformations.
//!
//! Transforms logical plans through pattern matching and rule composition.
//! Can compose with both logical and scalar rules, as well as analyzers.

use super::scalar::ScalarTransformer;
use crate::alexis_stuff::{
    engine::{
        actions::{
            analyzers::{logical::LogicalAnalyzer, scalar::ScalarAnalyzer},
            WithBinding,
        },
        patterns::scalar::ScalarPattern,
    },
    plans::logical::PartialLogicalPlanExpr,
};

/// A transformer for logical plans that produces new logical plans.
#[derive(Clone)]
pub struct LogicalTransformer {
    /// Name identifying this transformer
    pub name: String,

    /// Sequence of possible matches to try
    pub matches: Vec<Match>,
}

/// A single pattern match attempt within a logical transformer.
#[derive(Clone)]
pub struct Match {
    /// Pattern to match against the input plan
    pub pattern: ScalarPattern,

    /// Sequence of rule applications with their bindings
    pub composition: Vec<WithBinding<Composition>>,

    /// Expression constructing the output plan
    pub output: PartialLogicalPlanExpr,
}

/// Types of rules that can be composed in a logical transformation.
///
/// Logical transformers can use:
/// - Other logical transformers
/// - Scalar transformers
/// - Logical analyzers
/// - Scalar analyzers
#[derive(Clone)]
pub enum Composition {
    ScalarTransformer(Box<ScalarTransformer>),
    ScalarAnalyzer(Box<ScalarAnalyzer>),
    LogicalTransformer(Box<LogicalTransformer>),
    LogicalAnalyzer(Box<LogicalAnalyzer>),
}
