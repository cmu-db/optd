use std::sync::Arc;

use crate::cascades::{operators::ScalarOperator, types::OptdType};

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
