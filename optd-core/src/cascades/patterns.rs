//! Pattern matching system for the OPTD optimizer.
//!
//! This module provides three distinct but related pattern types that work together
//! to match against different parts of a query plan:
//! - LogicalPattern: Matches logical operators and their structure
//! - ScalarPattern: Matches scalar expressions and their structure
//! - TypePattern: Matches metadata values and their content
//!
//! The pattern system mirrors the structure of the plan IR while providing
//! additional matching capabilities like wildcards and bindings.

use std::sync::Arc;

use super::types::OptdExpr;

/// Pattern for matching logical operators in a query plan.
///
/// Logical patterns can match against both logical and scalar children,
/// reflecting the structure of logical operators in the plan IR.
#[derive(Clone)]
pub enum LogicalPattern {
    /// Matches any logical subtree without binding.
    Any,

    /// Negates a pattern match.
    Not(Box<LogicalPattern>),

    /// Binds a matched subtree to a name for later reference.
    ///
    /// The bound value can be referenced in rule applications
    /// and transformations.
    Bind(String, Box<LogicalPattern>),

    /// Matches a specific logical operator type with its content and children.
    Operator {
        /// Operator type to match (e.g., "Join", "Filter")
        op_type: String,
        /// Patterns for matching operator metadata
        content: Vec<Arc<TypePattern>>,
        /// Patterns for matching logical children
        logical_children: Vec<Box<LogicalPattern>>,
        /// Patterns for matching scalar children
        scalar_children: Vec<Arc<ScalarPattern>>,
    },
}

/// Pattern for matching scalar expressions in a query plan.
///
/// Scalar patterns can only match scalar children, reflecting the
/// more limited structure of scalar operators in the plan IR.
#[derive(Clone)]
pub enum ScalarPattern {
    /// Matches any scalar expression without binding
    Any,

    /// Negates a pattern match
    Not(Box<ScalarPattern>),

    /// Binds a matched scalar expression to a name
    Bind(String, Arc<ScalarPattern>),

    /// Matches a specific scalar operator type with its content and children
    Operator {
        /// Operator type to match (e.g., "Add", "Constant")
        op_type: String,
        /// Pattern for matching operator metadata
        content: Arc<TypePattern>,
        /// Patterns for matching scalar children
        scalar_children: Vec<Arc<ScalarPattern>>,
    },
}

/// Pattern for matching metadata values and types.
///
/// Type patterns are used within operator patterns to match against
/// the metadata fields of operators.
#[derive(Clone)]
pub enum TypePattern {
    /// Matches any type value
    Any,

    /// Binds a matched type value to a name.
    ///
    /// Note: Type bindings are always leaf nodes as they match
    /// atomic metadata values.
    Bind(String, Arc<OptdExpr>),

    /// Matches based on an OPTD expression evaluation.
    ///
    /// Allows complex conditions on type values using the
    /// full OPTD expression system.
    Match {
        /// Expression to evaluate for matching
        expr: Arc<OptdExpr>,
    },
}
