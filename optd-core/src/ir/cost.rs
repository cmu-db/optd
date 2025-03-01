#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(pub f64);

pub const MAX_COST: Cost = Cost(f64::INFINITY);
