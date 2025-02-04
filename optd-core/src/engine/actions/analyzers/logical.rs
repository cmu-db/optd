//! Analyzers for logical plans.
//!
//! Logical analyzers can compose with both logical and scalar analyzers
//! to extract information from logical plans into user-defined types.

use crate::{
    engine::{actions::BindAs, patterns::logical::LogicalPattern},
    values::OptdExpr,
};

use super::scalar::ScalarAnalyzer;

/// An analyzer for logical plans that produces user-defined types.
///
/// Logical analyzers match against plan patterns and can compose with
/// both logical and scalar analyzers to extract information.
#[derive(Clone, Debug)]
pub struct LogicalAnalyzer {
    /// Name identifying this analyzer
    pub name: String,

    /// Sequence of pattern matches to try
    pub matches: Vec<Match>,
}

/// A single pattern match attempt within a logical analyzer.
///
/// Each match combines:
/// - A pattern to identify relevant plan structures
/// - A sequence of composed analyzers to extract information
/// - An expression to produce the final output type
#[derive(Clone, Debug)]
pub struct Match {
    /// Pattern to match against the input plan
    pub pattern: LogicalPattern,

    /// Sequence of analyzer applications with their bindings
    pub composition: Vec<BindAs<Composition>>,

    /// Expression producing the final output type
    pub output: OptdExpr,
}

/// Types of analyzers that can be composed in logical analysis.
#[derive(Clone, Debug)]
pub enum Composition {
    /// Compose with another logical analyzer
    LogicalAnalyzer(Box<LogicalAnalyzer>),

    /// Compose with a scalar analyzer
    ScalarAnalyzer(Box<ScalarAnalyzer>),
}
