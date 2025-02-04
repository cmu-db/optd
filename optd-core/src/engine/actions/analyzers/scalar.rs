//! Analyzers for scalar expressions.
//!
//! Scalar analyzers can only compose with other scalar analyzers
//! to extract information from scalar expressions into user-defined types.

use crate::{
    engine::{actions::BindAs, patterns::scalar::ScalarPattern},
    values::OptdExpr,
};

/// Type alias for scalar analyzer composition with binding
pub type ScalarComposition = (String, Box<ScalarAnalyzer>);

/// An analyzer for scalar expressions that produces user-defined types.
///
/// Scalar analyzers match against expression patterns and can compose
/// with other scalar analyzers to extract information.
#[derive(Clone, Debug)]
pub struct ScalarAnalyzer {
    /// Name identifying this analyzer
    pub name: String,

    /// Sequence of pattern matches to try
    pub matches: Vec<Match>,
}

/// A single pattern match attempt within a scalar analyzer.
///
/// Each match combines:
/// - A pattern to identify relevant expression structures
/// - A sequence of composed scalar analyzers
/// - An expression to produce the final output type
#[derive(Clone, Debug)]
pub struct Match {
    /// Pattern to match against the input expression
    pub pattern: ScalarPattern,

    /// Sequence of analyzer applications with their bindings
    pub composition: Vec<BindAs<Composition>>,

    /// Expression producing the final output type
    pub output: OptdExpr,
}

/// Type alias for composable scalar analyzers.
///
/// Scalar analyzers can only compose with other scalar analyzers,
/// so no enum needed.
pub type Composition = Box<ScalarAnalyzer>;
