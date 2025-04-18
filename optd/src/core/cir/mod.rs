// This module contains the Core Intermediate Representation (CIR) for optd.
//!
//! This module defines core data structures and types used by the query optimizer's internal
//! processing pipeline. The CIR serves as the canonical representation of plans, expressions,
//! groups, and properties throughout the optimizer, bridging between the user-facing DSL and the
//! optimizer's execution engine.

mod expressions;
mod goal;
mod group;
mod operators;
mod plans;
mod properties;
mod rules;

pub use expressions::*;
pub use goal::*;
pub use group::*;
pub use operators::*;
pub use plans::*;
pub use properties::*;
pub use rules::*;
