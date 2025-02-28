//! A physical filter operator.

use serde::Deserialize;

use crate::{cascades::ir::OperatorData, operators::relational::physical::PhysicalOperator};

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

/// Creates a physical filter operator.
pub fn physical_filter<Relation, Scalar>(
    child: Relation,
    predicate: Scalar,
) -> PhysicalOperator<OperatorData, Relation, Scalar> {
    PhysicalOperator::Filter(PhysicalFilter::new(child, predicate))
}
