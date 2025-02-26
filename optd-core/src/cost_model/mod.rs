//! Cost model

/// Cost of a physical plan.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, sqlx::Type)]
#[repr(transparent)]
#[sqlx(transparent)]
pub struct Cost(pub f64);

impl Cost {
    pub const INFINITY: Cost = Cost(f64::INFINITY);
}
