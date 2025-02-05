//! Core operator types for OPTD's low-level IR (Intermediate Representation).
//!
//! These generic operator types serve multiple purposes in the system:
//!
//! 1. Plan Representations:
//!    a. Logical Plans: Complete query execution plans
//!    b. Scalar Plans: Expression trees within logical plans
//!    c. Partial Plans: Mixed materialization states during optimization
//!
//! 2. Materialization States:
//!    a. Fully Materialized: Complete trees of concrete operators
//!    b. Partially Materialized: Mix of concrete operators and group references
//!    c. Unmaterialized: Pure group references
//!
//! 3. Pattern Matching:
//!    Templates for matching against any of the above plan forms
//!
//! This unified representation handles the complete lifecycle of plans:
//! - Initial logical/scalar plan creation
//! - Optimization process with group references
//! - Pattern matching for transformations
//!
//! TODO(alexis): This *entire* module  will be codegened from the OPTD-DSL.

pub mod relational;
pub mod scalar;
