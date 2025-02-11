//! A logical projection.

use serde::Deserialize;

use crate::values::OptdValue;

use super::PhysicalOperator;

/// Physical project operator that specifies output columns.
///
/// Takes input relation (`Relation`) and defines output columns/expressions
/// (`Scalar`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PhysicalProject<Relation, Scalar> {
    /// The input relation.
    pub child: Relation,
    pub fields: Vec<Scalar>,
}

impl<Relation, Scalar> PhysicalProject<Relation, Scalar> {
    /// Create a new physical project operator.
    pub fn new(child: Relation, fields: Vec<Scalar>) -> Self {
        Self { child, fields }
    }
}

/// Creates a physical project operator.
pub fn project<Relation, Scalar>(
    child: Relation,
    fields: Vec<Scalar>,
) -> PhysicalOperator<OptdValue, Relation, Scalar> {
    PhysicalOperator::Project(PhysicalProject::new(child, fields))
}
