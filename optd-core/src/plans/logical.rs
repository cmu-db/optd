//! Logical plan representations for the OPTD optimizer.
//!
//! Provides three levels of plan materialization:
//! 1. Full materialization (LogicalPlan)
//! 2. Partial materialization (PartialLogicalPlan)
//! 3. Group references (LogicalGroupId)
//!
//! This allows the optimizer to work with plans at different stages
//! of materialization during the optimization process.

use crate::{operators::relational::logical::LogicalOperator, types::OptdType};

use super::{
    scalar::{PartialScalarPlan, ScalarPlan},
    PartialPlanExpr,
};
use std::sync::Arc;

/// Identifier for logical operator groups in the optimizer.
type LogicalGroupId = usize;

/// A fully materialized logical query plan.
///
/// Contains a complete tree of logical operators where all children
/// (both logical and scalar) are fully materialized. Used for final
/// plan representation after optimization is complete.
#[derive(Clone)]
pub struct LogicalPlan {
    operator: LogicalOperator<OptdType, Arc<LogicalPlan>, Arc<ScalarPlan>>,
}

/// A logical plan with varying levels of materialization.
///
/// During optimization, plans can be in three states:
/// - Partially materialized: Single materialized operator with group references
/// - Unmaterialized: Pure group reference
#[derive(Clone)]
pub enum PartialLogicalPlan {
    /// Single materialized operator with potentially unmaterialized children
    PartialMaterialized {
        operator: LogicalOperator<OptdType, Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>>,
    },

    /// Reference to an optimization group containing equivalent plans
    UnMaterialized(LogicalGroupId),
}

/// Type alias for expressions that construct logical plans.
/// See PartialPlanExpr for the available expression constructs.
pub type PartialLogicalPlanExpr = PartialPlanExpr<PartialLogicalPlan>;
