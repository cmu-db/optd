//! Transformation rules for the OPTD optimizer.
//!
//! Provides two categories of transformations:
//! - Logical transformations: operate on logical plans
//! - Scalar transformations: operate on scalar expressions
//!
//! Each transformer uses pattern matching and rule composition to
//! produce new plans during optimization.

pub mod interpreter;
pub mod logical;
pub mod scalar;
