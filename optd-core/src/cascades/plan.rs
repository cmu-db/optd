//! Plans with type erased operators, for convenience in the optimization process.
//! This avoids having to match on each operator type in the optimization rules.
//! That is annoying and inefficient. Cascades or the rule matching engine does no longer
//! care at all about the operator types, only the patterns and the ids.
//! At this point type safety is achieved anyways, so we should just have a translation
//! layer from the typed IR to this one after into_optd (and vice-versa). 
//! This step can be codegenned to avoid boilerplate anyways.

use std::sync::Arc;

use super::operator::LogicalOperator;
use super::operator::ScalarOperator;

type LogicalGroupId = usize;
type ScalarGroupId = usize;

#[derive(Clone)]
pub struct ScalarPlan {
    operator: ScalarOperator<Arc<ScalarPlan>>,
}

#[derive(Clone)]
pub struct LogicalPlan {
    operator: LogicalOperator<LogicalPlan, ScalarPlan>,
}

#[derive(Clone)]
pub enum PartialLogicalPlan {
    Materialized(LogicalPlan),
    PartialMaterialized {
        operator: LogicalOperator<Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>>,
    },
    UnMaterialized(LogicalGroupId),
}

#[derive(Clone)]
pub enum PartialScalarPlan {
    Materialized(ScalarPlan),
    PartialMaterialized {
        operator: ScalarOperator<Arc<PartialScalarPlan>>,
    },
    UnMaterialized(ScalarGroupId),
}
