use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{
    groups::{LogicalGroupId, ScalarGroupId},
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
    UnMaterialized(LogicalGroupId),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum PartialScalarPlan {
    PartialMaterialized {
        node: ScalarOperator<Arc<PartialScalarPlan>>,
    },
    UnMaterialized(ScalarGroupId),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct PhysicalGoal {
    pub group_id: LogicalGroupId,
    pub properties: PhysicalProperties,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum PartialPhysicalPlan {
    PartialMaterialized {
        node: PhysicalOperator<Arc<PartialPhysicalPlan>, Arc<PartialScalarPlan>>,
    },
    UnMaterialized(PhysicalGoal),
}

impl From<LogicalGroupId> for Arc<PartialLogicalPlan> {
    fn from(group_id: LogicalGroupId) -> Arc<PartialLogicalPlan> {
        Arc::new(PartialLogicalPlan::UnMaterialized(group_id))
    }
}

impl From<ScalarGroupId> for Arc<PartialScalarPlan> {
    fn from(group_id: ScalarGroupId) -> Arc<PartialScalarPlan> {
        Arc::new(PartialScalarPlan::UnMaterialized(group_id))
    }
}
