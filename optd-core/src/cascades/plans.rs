//! Type-erased plan representations for the optimization process.
//!
//! This module provides plan structures that abstract away specific operator types,
//! allowing the optimizer to work with a simplified view of the query plan. Key concepts:
//!
//! - Type Erasure: Operators are represented by their structure and metadata only,
//!   not their specific Rust types
//! - Materialization States: Plans can be fully materialized, partially materialized,
//!   or references to optimization groups
//! - Translation Layer: Conversion between typed IR and type-erased forms happens
//!   at optimization boundaries
//!
//! This design enables more efficient pattern matching and rule application by
//! removing the need to match against specific operator types.

// TODO(Alexis): split up in directory to keep nice structure.

use super::operators::{LogicalOperator, ScalarOperator};
use super::types::OptdType;
use std::sync::Arc;

/// Identifier for logical operator groups in the optimizer
type LogicalGroupId = usize;

/// Identifier for scalar expression groups in the optimizer
type ScalarGroupId = usize;

/// A fully materialized scalar expression.
///
/// Contains a complete tree of scalar operators where all children
/// are also fully materialized.
#[derive(Clone)]
pub struct ScalarPlan {
    operator: ScalarOperator<OptdType, Arc<ScalarPlan>>,
}

/// A fully materialized logical plan.
///
/// Contains a complete tree of logical operators where all children
/// (both logical and scalar) are fully materialized.
#[derive(Clone)]
pub struct LogicalPlan {
    operator: LogicalOperator<OptdType, LogicalPlan, ScalarPlan>,
}

/// A logical plan that may be partially materialized.
///
/// Represents plans during optimization where some subtrees may be:
/// - Fully materialized (complete operator trees)
/// - Partially materialized (mix of operators and group references)
/// - Unmaterialized (pure group references)
#[derive(Clone)]
pub enum PartialLogicalPlan {
    /// A fully materialized subtree
    Materialized(LogicalPlan),

    /// A single materialized operator with potentially unmaterialized children
    PartialMaterialized {
        operator: LogicalOperator<OptdType, Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>>,
    },

    /// A reference to an optimization group
    UnMaterialized(LogicalGroupId),
}

/// A scalar expression that may be partially materialized.
///
/// Similar to PartialLogicalPlan but for scalar expressions. Can represent:
/// - Fully materialized expressions
/// - Partially materialized expressions
/// - References to scalar expression groups
#[derive(Clone)]
pub enum PartialScalarPlan {
    /// A fully materialized expression
    Materialized(ScalarPlan),

    /// A single materialized operator with potentially unmaterialized children
    PartialMaterialized {
        operator: ScalarOperator<OptdType, Arc<PartialScalarPlan>>,
    },

    /// A reference to an optimization group
    UnMaterialized(ScalarGroupId),
}
