//! Pattern matching system for the OPTD optimizer.
//!
//! This module provides three distinct but related pattern types that work together
//! to match against different parts of a query plan:
//! - LogicalPattern: Matches logical operators and their structure
//! - ScalarPattern: Matches scalar expressions and their structure
//! - ValuePattern: Matches OPTD values and their content
//!
//! The pattern system mirrors the structure of the plan IR while providing
//! additional matching capabilities like wildcards and bindings.

pub mod logical;
pub mod scalar;
pub mod value;
