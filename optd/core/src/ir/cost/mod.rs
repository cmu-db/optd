//! This module defines the necessary components of the cost model used by the
//! optd operators

use std::sync::Arc;
use crate::ir::{IRContext, Operator, explain::quick_explain, properties::Cardinality};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost {
    c: f64,
}

impl std::fmt::Display for Cost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${:.2}", self.c)
    }
}

impl Cost {
    pub const ZERO: Self = Self::new(0.);
    pub const UNIT: Self = Self::new(1.);
    pub const fn new(c: f64) -> Self {
        Self { c }
    }

    pub fn as_f64(&self) -> f64 {
        self.c
    }
}

impl std::ops::Add for Cost {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.c + rhs.c)
    }
}

impl std::ops::Mul for Cost {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self::new(self.c * rhs.c)
    }
}

impl std::ops::Mul<Cardinality> for Cost {
    type Output = Self;

    fn mul(self, rhs: Cardinality) -> Self::Output {
        Self::new(self.c * rhs.as_f64())
    }
}

impl std::ops::Mul<Cost> for Cardinality {
    type Output = Cost;

    fn mul(self, rhs: Cost) -> Self::Output {
        Cost::new(self.as_f64() * rhs.c)
    }
}

impl std::ops::Mul<f64> for Cost {
    type Output = Self;

    fn mul(self, rhs: f64) -> Self::Output {
        Self::new(self.c * rhs)
    }
}

impl std::ops::Mul<Cost> for f64 {
    type Output = Cost;

    fn mul(self, rhs: Cost) -> Self::Output {
        rhs.mul(self)
    }
}

pub trait CostModel: Send + Sync + 'static {

    /// Computes the cost of the given operator, excluding input costs.
    /// This should be implemented by specific cost models per operator
    fn compute_operator_cost(&self, op: &Operator, ctx: &IRContext) -> Option<Cost>;

    /// Computes the total cost of the given operator, given input costs.
    fn compute_total_with_input_costs(
        &self,
        op: &Operator,
        input_costs: &[Cost],
        ctx: &IRContext,
    ) -> Option<Cost> {
        assert_eq!(
            op.input_operators().len(),
            input_costs.len(),
            "input cost array should have length equal to the number of input operators:\n{}",
            quick_explain(Arc::new(op.clone()), ctx),
        );
        let operator_cost = self.compute_operator_cost(op, ctx)?;
        Some(input_costs.iter().fold(operator_cost, |c1, c2| c1 + *c2))
    }

    /// Recursively computes the costs of the input operators of the given 
    /// operator, down to leaf nodes.
    fn compute_input_costs(&self, op: &Operator, ctx: &IRContext) -> Option<Vec<Cost>> {
        op.input_operators()
            .iter()
            .map(|input_op| self.compute_total_cost(input_op, ctx))
            .collect()
    }

    /// Recursively computes the total cost of the given operator, including
    /// all input costs, down to leaf nodes.
    fn compute_total_cost(&self, op: &Operator, ctx: &IRContext) -> Option<Cost> {
        let input_costs = self.compute_input_costs(op, ctx)?;
        self.compute_total_with_input_costs(op, &input_costs, ctx)
    }

}
