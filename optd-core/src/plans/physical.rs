//! Physical plan representations for the OPTD optimizer.
//!
//! Provides three levels of plan materialization:
//! 1. Full materialization (PhysicalPlan)
//! 2. Partial materialization (PartialPhysicalPlan)
//! 3. Group references (RelationalGroupId).
//!
//! This allows the optimizer to work with plans at different stages
//! of materialization during the optimization process.

use crate::{
    cascades::groups::RelationalGroupId, operators::relational::physical::PhysicalOperator,
    values::OptdValue,
};

use super::{
    scalar::{PartialScalarPlan, ScalarPlan},
    PartialPlanExpr,
};
use std::sync::Arc;

/// A fully materialized physical query plan.
///
/// Contains a complete tree of physical operators where all children
/// (both physical and scalar) are fully materialized. Used for final
/// plan representation after optimization is complete.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlan {
    pub operator: PhysicalOperator<OptdValue, Arc<PhysicalPlan>, Arc<ScalarPlan>>,
}

/// A physical plan with varying levels of materialization.
///
/// During optimization, plans can be in three states:
/// - Partially materialized: Single materialized operator with group references
/// - Unmaterialized: Pure group reference
#[derive(Clone, Debug, PartialEq)]
pub enum PartialPhysicalPlan {
    /// Single materialized operator with potentially unmaterialized children
    PartialMaterialized {
        operator: PhysicalOperator<OptdValue, Arc<PartialPhysicalPlan>, Arc<PartialScalarPlan>>,
    },

    /// Reference to an optimization group containing equivalent plans
    UnMaterialized(RelationalGroupId),
}

/// Type alias for expressions that construct physical plans.
/// See PartialPlanExpr for the available expression constructs.
pub type PartialPhysicalPlanExpr = PartialPlanExpr<PartialPhysicalPlan>;
