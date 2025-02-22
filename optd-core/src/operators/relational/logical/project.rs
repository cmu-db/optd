//! A logical projection.

use serde::Deserialize;
use crate::cascades::ir::OperatorData;
use super::LogicalOperator;

/// Logical project operator that specifies output columns.
///
/// Takes input relation (`Relation`) and defines output columns/expressions
/// (`Scalar`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Project<Relation, Scalar> {
    /// The input relation.
    pub child: Relation,
    pub fields: Vec<Scalar>,
}

impl<Relation, Scalar> Project<Relation, Scalar> {
    /// Create a new project operator.
    pub fn new(child: Relation, fields: Vec<Scalar>) -> Self {
        Self { child, fields }
    }
}

/// Creates a project logical operator.
pub fn project<Relation, Scalar>(
    child: Relation,
    fields: Vec<Scalar>,
) -> LogicalOperator<OperatorData, Relation, Scalar> {
    LogicalOperator::Project(Project::new(child, fields))
}
