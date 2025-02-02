use std::sync::Arc;

use crate::cascades::{engine::patterns::scalar::ScalarPattern, types::OptdExpr};

pub type ScalarComposition = (String, Arc<ScalarAnalyzer>);

/// Scalar Analyzer:
/// - Matches scalar patterns (`ScalarPattern`).
/// - Can only compose with other scalar analyzers.
/// - Produces an output of `OptdType` after matching.
#[derive(Clone)]
pub struct ScalarAnalyzer {
    pub name: String,              // Name of the scalar analyzer
    pub matches: Vec<ScalarMatch>, // List of possible matches
}

/// A match in a ScalarAnalyzer:
/// - Defines a pattern to match against.
/// - Specifies a composition of analyzers (only Scalar allowed).
/// - Produces an output of `OptdType`.
#[derive(Clone)]
pub struct ScalarMatch {
    pub pattern: ScalarPattern,              // Pattern to match
    pub composition: Vec<ScalarComposition>, // Composition: Only Scalar analyzers allowed
    pub output: OptdExpr,                    // Output expression
}
