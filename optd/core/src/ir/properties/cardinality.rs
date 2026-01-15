//! This module defines the cardinality property for IR operators,
//! representing the number of tuples that can be produced by an Operator.

use crate::ir::{
    Operator,
    context::IRContext,
    properties::{Derive, GetProperty, PropertyMarker},
};

/// TODO(yuchen): In the future we should consider keeping separate min/max + 
/// error bounds.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cardinality {
    card: f64,
}

pub trait CardinalityEstimator: Send + Sync + 'static {
    fn estimate(&self, op: &Operator, ctx: &IRContext) -> Cardinality;
}

impl Cardinality {
    pub const ZERO: Self = Self::new(0.);
    pub const UNIT: Self = Self::new(1.);
    /// Creates a [`Cardinality`] object with a single value.
    pub const fn new(card: f64) -> Self {
        Self { card }
    }

    pub const fn with_count_lossy(count: usize) -> Self {
        Self::new(count as f64)
    }

    pub const fn as_f64(&self) -> f64 {
        self.card
    }

    pub fn map(self, f: impl Fn(f64) -> f64) -> Self {
        Self::new(f(self.card))
    }
}

impl PropertyMarker for Cardinality {
    type Output = Self;
}

impl Derive<Cardinality> for crate::ir::Operator {
    fn derive_by_compute(&self, ctx: &IRContext) -> <Cardinality as PropertyMarker>::Output {
        ctx.card.estimate(self, ctx)
    }

    fn derive(
        &self,
        ctx: &crate::ir::context::IRContext,
    ) -> <Cardinality as PropertyMarker>::Output {
        *self
            .common
            .properties
            .cardinality
            .get_or_init(|| <Operator as Derive<Cardinality>>::derive_by_compute(self, ctx))
    }
}

impl crate::ir::Operator {
    pub fn cardinality(&self, ctx: &crate::ir::context::IRContext) -> Cardinality {
        self.get_property::<Cardinality>(ctx)
    }
}

impl std::ops::Add for Cardinality {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.card + rhs.card)
    }
}

impl std::ops::Mul for Cardinality {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self::new(self.card * rhs.card)
    }
}

impl std::ops::Mul<f64> for Cardinality {
    type Output = Self;

    fn mul(self, rhs: f64) -> Self::Output {
        Self::new(self.card * rhs)
    }
}

impl std::ops::Mul<Cardinality> for f64 {
    type Output = Cardinality;

    fn mul(self, rhs: Cardinality) -> Self::Output {
        rhs.mul(self)
    }
}
