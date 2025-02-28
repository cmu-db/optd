//! Scalar expression representations for the OPTD optimizer.
//!
//! Provides three levels of scalar expression materialization:
//! 1. Full materialization (ScalarPlan)
//! 2. Partial materialization (PartialScalarPlan)
//! 3. Group references (ScalarGroupId)
//!
//! This allows the optimizer to work with expressions at different stages
//! of materialization during the optimization process.

use crate::{
    cascades::{groups::ScalarGroupId, ir::OperatorData},
    operators::scalar::ScalarOperator,
};

use std::sync::Arc;

/// A fully materialized scalar expression tree.
///
/// Contains a complete tree of scalar operators where all children
/// are also fully materialized. Used for final expression representation
/// after optimization is complete.
#[derive(Clone, Debug, PartialEq)]
pub struct ScalarPlan {
    pub operator: ScalarOperator<OperatorData, Arc<ScalarPlan>>,
}

/// A scalar expression with varying levels of materialization.
///
/// During optimization, expressions can be in three states:
/// - Partially materialized: Single materialized operator with group references
/// - Unmaterialized: Pure group reference
#[derive(Clone, Debug, PartialEq)]
pub enum PartialScalarPlan {
    /// Single materialized operator with potentially unmaterialized children
    PartialMaterialized {
        operator: ScalarOperator<OperatorData, Arc<PartialScalarPlan>>,
    },

    /// Reference to an optimization group containing equivalent expressions
    UnMaterialized(ScalarGroupId),
}
