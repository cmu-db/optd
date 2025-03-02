use serde::{Deserialize, Serialize};

use super::plans::PartialPhysicalPlan;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GoalId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum OptimizationStatus {
    Unoptimized,
    Pending,
    Optimized,
}

impl Into<PartialPhysicalPlan> for GoalId {
    fn into(self) -> PartialPhysicalPlan {
        PartialPhysicalPlan::UnMaterialized(self)
    }
}
