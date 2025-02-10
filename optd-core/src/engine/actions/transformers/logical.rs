//! Logical plan transformations.
//!
//! Transforms logical plans through pattern matching and rule composition.
//! Can compose with both logical and scalar rules, as well as analyzers.

use super::scalar::ScalarTransformer;
use crate::{
    engine::{
        actions::{
            analyzers::{logical::LogicalAnalyzer, scalar::ScalarAnalyzer},
            Action, Match as GenericMatch,
        },
        patterns::scalar::ScalarPattern,
    },
    plans::logical::PartialLogicalPlanExpr,
};
use std::{cell::RefCell, rc::Rc};

/// Types of rules that can be composed in a logical transformation.
///
/// Logical transformers can use:
/// - Other logical transformers
/// - Scalar transformers
/// - Logical analyzers
/// - Scalar analyzers
#[derive(Clone, Debug)]
pub enum Composition {
    ScalarTransformer(Rc<RefCell<ScalarTransformer>>),
    ScalarAnalyzer(Rc<RefCell<ScalarAnalyzer>>),
    LogicalTransformer(Rc<RefCell<LogicalTransformer>>),
    LogicalAnalyzer(Rc<RefCell<LogicalAnalyzer>>),
}

/// A single logical transformer match.
pub type Match = GenericMatch<ScalarPattern, Composition, PartialLogicalPlanExpr>;

/// A transformer for logical plans that produces new logical plans.
pub type LogicalTransformer = Action<Match>;
