use super::{groups::LogicalGroupId, properties::PhysicalProperties};

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalGoal(pub LogicalGroupId, pub PhysicalProperties);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OptimizationStatus {
    Unoptimized,
    Pending,
    Optimized,
}
