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

use std::collections::HashSet;

/// A logical operator node in the IR tree.
///
/// Generic parameters:
/// - `Metadata`: Type of metadata values/patterns:
///   - Plans: concrete metadata values (OptdType)
///   - Patterns: metadata pattern matchers
/// - `Logical`: Type of logical children:
///   - Logical Plans: LogicalOperator nodes
///   - Partial Plans: Mix of operators and group references
///   - Cascades Groups: Pure group references
///   - Patterns: LogicalPattern matchers
/// - `Scalar`: Type of scalar children:
///   - Scalar Plans: ScalarOperator nodes
///   - Partial Plans: Mix of operators and group references
///   - Cascades Groups: Pure group references
///   - Patterns: ScalarPattern matchers
#[derive(Clone, Debug)]
pub struct LogicalOperator<Metadata, Logical, Scalar> {
    pub op_type: String,
    pub content: Vec<HashSet<String, Metadata>>,
    pub logical_children: Vec<Logical>,
    pub scalar_children: Vec<Scalar>,
}

/// A scalar operator node in the IR tree.
///
/// Generic parameters:
/// - `Metadata`: Type of metadata values/patterns:
///   - Plans: concrete metadata values (OptdType)
///   - Patterns: metadata pattern matchers
/// - `Scalar`: Type of scalar children:
///   - Scalar Plans: ScalarOperator nodes
///   - Partial Plans: Mix of operators and group references
///   - Cascades Groups: Pure group references
///   - Patterns: ScalarPattern matchers
///
/// Note: Scalar operators cannot have logical children, as they represent
/// pure expressions within the overall plan structure.
#[derive(Clone, Debug)]
pub struct ScalarOperator<Metadata, Scalar> {
    pub op_type: String,
    pub content: HashSet<String, Metadata>,
    pub scalar_children: Vec<Scalar>,
}
