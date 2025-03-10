//! This module provides bridge functionality between the DSL's internal representation (HIR)
//! and the query optimizer's intermediate representation (Optd-IR).
//!
//! The bridge consists of two main components:
//! - `into_optd`: Converts HIR Value objects into Optd's PartialPlan representations
//! - `from_optd`: Converts Optd's PartialPlan representations into HIR Value objects
//!
//! These bidirectional conversions enable the DSL to interact with the query optimizer,
//! allowing rule-based transformations to be applied to query plans while maintaining
//! the ability to work with both representations.

pub mod from_cir;
pub mod into_cir;
