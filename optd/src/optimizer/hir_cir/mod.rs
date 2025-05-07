//! This module provides bridge functionality between the DSL's internal representation (HIR) and
//! the query optimizer's intermediate representation (Optd-IR).
//!
//! The bridge consists of two main components:
//!
//! - [`from_cir`]: Converts optd's type representations (CIR) into DSL [`Value`]s (HIR).
//! - [`into_cir`]: Converts HIR [`Value`]s into optd's type representations (CIR).
//!
//! These bidirectional conversions enable the DSL to interact with the query optimizer, allowing
//! rule-based transformations to be applied to query plans while maintaining the ability to work
//! with both representations.
//!
//! [`Value`]: crate::core::analyzer::hir::Value

pub mod from_cir;
pub mod into_cir;
