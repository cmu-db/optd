//! Analyzer definitions for extracting information from plans and expressions.
//!
//! There are two types of analyzers:
//! - Logical analyzers: analyze logical plans
//! - Scalar analyzers: analyze scalar expressions
//!
//! Both use pattern matching and composition to produce an `OptdType`.

pub mod logical;
pub mod scalar;
