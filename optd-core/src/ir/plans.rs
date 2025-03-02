use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{
    goal::GoalId,
    groups::{RelationalGroupId, ScalarGroupId},
    operators::{LogicalOperator, PhysicalOperator, ScalarOperator},
    properties::PhysicalProperties,
};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct LogicalPlan {
    pub node: LogicalOperator<Arc<LogicalPlan>, Arc<ScalarPlan>>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ScalarPlan {
    pub node: ScalarOperator<Arc<ScalarPlan>>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct PhysicalPlan {
    pub node: PhysicalOperator<Arc<PhysicalPlan>, Arc<ScalarPlan>>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum PartialLogicalPlan {
    PartialMaterialized {
        node: LogicalOperator<Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>>,
    },
    UnMaterialized(RelationalGroupId),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum PartialScalarPlan {
    PartialMaterialized {
        node: ScalarOperator<Arc<PartialScalarPlan>>,
        group_id: ScalarGroupId,
    },
    UnMaterialized(ScalarGroupId),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum PartialPhysicalPlan {
    PartialMaterialized {
        node: PhysicalOperator<Arc<PartialPhysicalPlan>, Arc<PartialScalarPlan>>,
        properties: PhysicalProperties,
        group_id: RelationalGroupId,
    },
    UnMaterialized(GoalId),
}
