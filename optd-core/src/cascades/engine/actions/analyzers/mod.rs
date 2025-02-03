//! Analyzer system for the OPTD optimizer.
//!
//! This module defines analyzers for processing logical and scalar expressions
//! within the optimizer framework. Analyzers are responsible for pattern matching
//! and composition to determine transformations in the query plan.
//! - `LogicalAnalyzer`: Handles logical operators and can compose both logical and scalar analyzers.
//! - `ScalarAnalyzer`: Handles scalar expressions and can only compose with other scalar analyzers.
//!
//! The output of any analyzer is always an `OptdType`.

pub mod logical;
pub mod scalar;
