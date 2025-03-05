//! Error module provides error types used throughout the expression evaluation system.
//!
//! This module defines the core error types that can be produced during expression
//! evaluation. Error variant represent different failure cases that might occur
//! during evaluation.

/// Represents errors that can occur during expression evaluation.
#[derive(Clone, Debug)]
pub(crate) enum EngineError {
    /// A failure triggered by a Fail expression in the HIR.
    Fail(String),
    /// A failure triggered by the failure of returning any result.
    NoResult,
}
