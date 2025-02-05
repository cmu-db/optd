//! Logical plan transformations.
//!
//! Transforms logical plans through pattern matching and rule composition.
//! Can compose with both logical and scalar rules, as well as analyzers.

use std::{cell::RefCell, rc::Rc};

use crate::{
    engine::{
        actions::{
            analyzers::{logical::LogicalAnalyzer, scalar::ScalarAnalyzer},
            BindAs,
        },
        patterns::scalar::ScalarPattern,
    },
    plans::logical::PartialLogicalPlanExpr,
};

use super::scalar::ScalarTransformer;

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
    pub composition: Vec<BindAs<Composition>>,

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
/// Need Rc + RefCell to allow for recursive composition.
#[derive(Clone)]
pub enum Composition {
    ScalarTransformer(Rc<RefCell<ScalarTransformer>>),
    ScalarAnalyzer(Rc<RefCell<ScalarAnalyzer>>),
    LogicalTransformer(Rc<RefCell<LogicalTransformer>>),
    LogicalAnalyzer(Rc<RefCell<LogicalAnalyzer>>),
}
