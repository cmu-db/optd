//! The rule for implementing a logical `Filter` as a physical `Filter`.
//!
//! See [`PhysicalFilterRule`] for more information.

use super::*;
use crate::operator::relational::{
    logical::{filter::Filter as LogicalFilter, LogicalOperator},
    physical::{filter::filter::PhysicalFilter, PhysicalOperator},
};

/// A unit / marker struct for implementing `PhysicalFilterRule`.
///
/// This mplementation rule converts a logical `Filter` into a physical `Filter` operator.
pub struct PhysicalFilterRule;

impl ImplementationRule for PhysicalFilterRule {
    fn check_and_apply(&self, expr: LogicalExpression) -> Option<PhysicalExpression> {
        let LogicalOperator::Filter(LogicalFilter { child, predicate }) = expr else {
            return None;
        };

        Some(PhysicalOperator::Filter(PhysicalFilter {
            child,
            predicate,
        }))
    }
}
