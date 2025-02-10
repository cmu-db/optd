//! A physical filter operator.

use serde::Deserialize;

use crate::{operators::relational::physical::PhysicalOperator, values::OptdValue};

/// A physical operator that filters input rows based on a predicate.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct PhysicalFilter<Relation, Scalar> {
    /// The input relation.
    pub child: Relation,
    /// The filter predicate.
    pub predicate: Scalar,
}

impl<Relation, Scalar> PhysicalFilter<Relation, Scalar> {
    /// Create a new filter operator.
    pub fn new(child: Relation, predicate: Scalar) -> Self {
        Self { child, predicate }
    }
}

/// Creates a filter physical operator.
pub fn filter<Relation, Scalar>(
    child: Relation,
    predicate: Scalar,
) -> PhysicalOperator<OptdValue, Relation, Scalar> {
    PhysicalOperator::Filter(PhysicalFilter::new(child, predicate))
}
