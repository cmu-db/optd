//! Logical plan representations for the OPTD optimizer.
//!
//! Provides three levels of plan materialization:
//! 1. Full materialization (LogicalPlan)
//! 2. Partial materialization (PartialLogicalPlan)
//! 3. Group references (LogicalGroupId)
//!
//! This allows the optimizer to work with plans at different stages
//! of materialization during the optimization process.

use crate::{
    cascades::{groups::RelationalGroupId, ir::OperatorData},
    operators::relational::logical::LogicalOperator,
};

use super::scalar::{PartialScalarPlan, ScalarPlan};
use std::sync::Arc;

/// A fully materialized logical query plan.
///
/// Contains a complete tree of logical operators where all children
/// (both logical and scalar) are fully materialized. Used for final
/// plan representation after optimization is complete.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPlan {
    pub operator: LogicalOperator<OperatorData, Arc<LogicalPlan>, Arc<ScalarPlan>>,
}

/// A logical plan with varying levels of materialization.
///
/// During optimization, plans can be in three states:
/// - Partially materialized: Single materialized operator with group references
/// - Unmaterialized: Pure group reference
#[derive(Clone, Debug, PartialEq)]
pub enum PartialLogicalPlan {
    /// Single materialized operator with potentially unmaterialized children
    PartialMaterialized {
        operator: LogicalOperator<OperatorData, Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>>,
    },

    /// Reference to an optimization group containing equivalent plans
    UnMaterialized(RelationalGroupId),
}
