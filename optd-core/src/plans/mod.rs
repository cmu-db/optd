//! Plan expression system for constructing partial plans.
//!
//! Provides a generic expression type for building both logical and scalar
//! plans during optimization, with control flow and reference capabilities.

pub mod logical;
pub mod physical;
pub mod scalar;
