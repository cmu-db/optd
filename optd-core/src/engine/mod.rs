//! Core optimization engine for OPTD.
//!
//! The engine provides two key mechanisms for plan optimization:
//!
//! # Pattern Matching
//! The pattern system enables matching against different aspects of plans:
//! - Logical patterns: Match logical operator trees
//! - Scalar patterns: Match scalar expressions
//! - Type patterns: Match metadata values
//!
//! Patterns support:
//! - Wildcards
//! - Recursive operator matching
//! - Binding subplans for reuse
//! - Negative matching
//!
//! # Actions
//! Actions define what to do when patterns match. There are two types:
//!
//! ## Analyzers
//! Extract information from plans:
//! - LogicalAnalyzer: Analyzes logical plans
//! - ScalarAnalyzer: Analyzes scalar expressions
//! - Both produce `OptdValue` as output
//!
//! ## Transformers
//! Transform plans into new plans:
//! - LogicalTransformer: Transforms logical plans
//! - ScalarTransformer: Transforms scalar expressions
//! - Both produce new partial plans as output
//!
//! # Composition
//! Both analyzers and transformers support composition through WITH clauses:
//! - Chain multiple actions together
//! - Bind intermediate results
//! - Use results in subsequent actions
//! - Build complex transformations from simple ones

pub mod actions; // Analyzers and transformers
pub mod interpreter;
pub mod patterns; // Pattern matching system // Interpreter implementation
