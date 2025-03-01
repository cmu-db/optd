//! This module provides the evaluation machinery for expressions in the DSL.
//!
//! The evaluation system is designed to handle both deterministic and non-deterministic
//! expression evaluation through a streaming approach. Rather than producing a single
//! value, expressions evaluate to streams of possible values, capturing all potential
//! evaluation paths. This is critical for applications like query optimization where
//! pattern matching and rule application may yield multiple valid transformations.
//!
//! Key components of the evaluation system include:
//!
//! - `expr`: Expression evaluation dispatcher and high-level control flow
//! - `binary`: Evaluation of binary operations (arithmetic, logical, comparison)
//! - `unary`: Evaluation of unary operations (negation, not)
//! - `core`: Evaluation of primitive data types and data structures
//! - `match`: Pattern matching and binding for rule application
//! - `operator`: Evaluation of query plan operators with their children
//!
//! Expressions are evaluated in a non-blocking manner, with results propagated through
//! streams that can be consumed incrementally. This approach efficiently handles the
//! potential combinatorial explosion of evaluation paths in complex rule applications.

use optd_dsl::analyzer::context::Context;

use super::utils::streams::ValueStream;

mod binary;
mod core;
mod expr;
mod r#match;
mod operator;
mod unary;

/// Evaluates an expression in the given context.
///
/// This trait serves as the evaluation interface for expressions from HIR.
/// Expression types from HIR must implement this trait to be evaluated
/// by the engine.
pub(crate) trait Evaluate {
    fn evaluate(&self, context: Context) -> ValueStream;
}
