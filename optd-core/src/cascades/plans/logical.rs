use std::sync::Arc;

use crate::cascades::{operators::LogicalOperator, types::OptdType};

use super::scalar::{PartialScalarPlan, ScalarPlan};

/// Identifier for logical operator groups in the optimizer
type LogicalGroupId = usize;

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
