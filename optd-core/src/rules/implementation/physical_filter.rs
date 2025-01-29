use super::*;
use crate::operator::relational::{
    logical::LogicalOperator,
    physical::{filter::filter::Filter, PhysicalOperator},
};

/// Implementation rule that converts a logical filter into a filter physical operator.
pub struct PhysicalFilterRule;

impl ImplementationRule for PhysicalFilterRule {
    fn check_and_apply(&self, expr: LogicalExpression) -> Option<PhysicalExpression> {
        if let LogicalOperator::Filter(filter) = expr {
            return Some(PhysicalOperator::Filter(Filter {
                child: filter.child,
                predicate: filter.predicate,
            }));
        }

        None
    }
}
