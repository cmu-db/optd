//! Analyzers for logical plans.
//!
//! Logical analyzers can compose with both logical and scalar analyzers
//! to extract information from logical plans into user-defined types.

use super::scalar::ScalarAnalyzer;
use crate::{
    engine::{
        actions::{
            transformers::{logical::LogicalTransformer, scalar::ScalarTransformer},
            Action, Match as GenericMatch,
        },
        patterns::logical::LogicalPattern,
    },
    values::OptdExpr,
};
use std::{cell::RefCell, rc::Rc};

/// Types of analyzers that can be composed in logical analysis.
#[derive(Clone, Debug)]
pub enum Composition {
    LogicalAnalyzer(Rc<RefCell<LogicalAnalyzer>>),
    ScalarAnalyzer(Rc<RefCell<ScalarAnalyzer>>),
    ScalarTransformer(Rc<RefCell<ScalarTransformer>>),
    LogicalTransformer(Rc<RefCell<LogicalTransformer>>),
}

/// A single logical analyzer match.
pub type Match = GenericMatch<LogicalPattern, Composition, OptdExpr>;

/// An analyzer for logical plans that produces user-defined types.
pub type LogicalAnalyzer = Action<Match>;
