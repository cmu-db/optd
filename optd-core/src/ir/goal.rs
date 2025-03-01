use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GoalId(pub i64);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OptimizationStatus {
    Unoptimized,
    Pending,
    Optimized,
}
