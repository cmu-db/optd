//! Actions for the OPTD optimizer's execution engine.
//!
//! This module provides a generic framework for pattern-matching based actions in the optimizer.
//! Actions include both analysis (extracting information) and transformation (rewriting plans).
//!
//! The framework supports:
//! - Pattern matching against plan structure
//! - Rule composition through intermediate bindings
//! - OPTD expression inputs

pub mod analyzers; // Analysis actions that produce user-defined types
pub mod transformers; // Transformation actions that produce new plans

use crate::values::OptdExpr;

/// An action that matches patterns and produces outputs.
#[derive(Clone, Debug)]
pub struct Action<Match> {
    /// Name identifying this action
    pub name: String,
    /// Input expressions for this action
    pub inputs: Vec<BindAs<OptdExpr>>,
    /// Sequence of matches to try in order
    pub matches: Vec<Match>,
}

/// A single pattern match attempt within an action.
///
/// # Type Parameters
/// - `Pattern`: The type of pattern to match against (logical/scalar)
/// - `Composition`: The type of sub-actions that can be composed
/// - `Output`: The type produced by this match (analysis result/new plan)
#[derive(Clone, Debug)]
pub struct Match<Pattern, Composition, Output> {
    /// Pattern to match against the input
    pub pattern: Pattern,
    /// Sequence of composed actions with their results bound to names
    pub composition: Vec<BindAs<Composition>>,
    /// Expression producing the final output
    pub output: Output,
}

/// A binding for an action's result to a name.
///
/// Used in rule composition to chain multiple actions together. The bound
/// results can be referenced by name in subsequent actions or in the final
/// output expression.
///
/// # Type Parameter
/// - `T`: Type of action being bound (analyzer/transformer)
type BindAs<T> = (String, T);
