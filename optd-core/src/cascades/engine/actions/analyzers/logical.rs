use std::sync::Arc;

use crate::cascades::{engine::patterns::logical::LogicalPattern, types::OptdExpr};

use super::Analyzer;

pub type LogicalComposition = (String, Arc<Analyzer>);

/// Logical Analyzer:
/// - Matches logical patterns (`LogicalPattern`).
/// - Can compose with both logical and scalar analyzers.
/// - Produces an output of `OptdType` after matching.
#[derive(Clone)]
pub struct LogicalAnalyzer {
    pub name: String,               // Name of the logical analyzer
    pub matches: Vec<LogicalMatch>, // List of possible matches
}

/// A match in a LogicalAnalyzer:
/// - Defines a pattern to match against.
/// - Specifies a composition of analyzers (both Logical and Scalar allowed).
/// - Produces an output of `OptdType`.
#[derive(Clone)]
pub struct LogicalMatch {
    pub pattern: LogicalPattern,              // Pattern to match
    pub composition: Vec<LogicalComposition>, // Composition: Can include both Logical and Scalar analyzers
    pub output: OptdExpr,                     // Output expression
}
