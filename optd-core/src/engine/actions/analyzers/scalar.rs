//! Analyzers for scalar expressions.
//!
//! Scalar analyzers can only compose with other scalar analyzers
//! to extract information from scalar expressions into user-defined types.

use crate::{
    engine::{
        actions::{transformers::scalar::ScalarTransformer, Action, Match as GenericMatch},
        patterns::scalar::ScalarPattern,
    },
    values::OptdExpr,
};
use std::{cell::RefCell, rc::Rc};

/// Types of analyzers that can be composed in scalar analysis.
///
/// Scalar analyzers can only compose with other scalar analyzers
/// and transformers.
#[derive(Clone, Debug)]
pub enum Composition {
    ScalarAnalyzer(Rc<RefCell<ScalarAnalyzer>>),
    ScalarTransformer(Rc<RefCell<ScalarTransformer>>),
}

/// A single scalar analyzer match.
pub type Match = GenericMatch<ScalarPattern, Composition, OptdExpr>;

/// An analyzer for scalar expressions that produces user-defined types.
pub type ScalarAnalyzer = Action<Match>;
