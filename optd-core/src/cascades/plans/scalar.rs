//! Scalar expression representations for the OPTD optimizer.
//!
//! Provides three levels of scalar expression materialization:
//! 1. Full materialization (ScalarPlan)
//! 2. Partial materialization (PartialScalarPlan)
//! 3. Group references (ScalarGroupId)
//!
//! This allows the optimizer to work with expressions at different stages
//! of materialization during the optimization process.

use super::PartialPlanExpr;
use crate::cascades::{operators::ScalarOperator, types::OptdType};
use std::sync::Arc;

/// Identifier for scalar expression groups in the optimizer.
type ScalarGroupId = usize;

/// A fully materialized scalar expression tree.
///
/// Contains a complete tree of scalar operators where all children
/// are also fully materialized. Used for final expression representation
/// after optimization is complete.
#[derive(Clone)]
pub struct ScalarPlan {
    operator: ScalarOperator<OptdType, Arc<ScalarPlan>>,
}

/// A scalar expression with varying levels of materialization.
///
/// During optimization, expressions can be in three states:
/// - Fully materialized: Complete operator trees
/// - Partially materialized: Single materialized operator with group references
/// - Unmaterialized: Pure group reference
#[derive(Clone)]
pub enum PartialScalarPlan {
    /// Complete materialization - all operators and children concrete
    Materialized(ScalarPlan),

    /// Single materialized operator with potentially unmaterialized children
    PartialMaterialized {
        operator: ScalarOperator<OptdType, Arc<PartialScalarPlan>>,
    },

    /// Reference to an optimization group containing equivalent expressions
    UnMaterialized(ScalarGroupId),
}

/// Type alias for expressions that construct scalar plans.
/// See PartialPlanExpr for the available expression constructs.
pub type PartialScalarPlanExpr = PartialPlanExpr<PartialScalarPlan>;
